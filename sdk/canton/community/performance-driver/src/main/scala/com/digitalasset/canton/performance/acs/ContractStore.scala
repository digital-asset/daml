// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.acs

import com.daml.ledger.javaapi.data.codegen.{Contract, ContractCompanion, ContractId}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.ErrorUtil

import scala.collection.concurrent.TrieMap
import scala.concurrent.blocking

/** A in-memory contract store with trivial indexing and query capabilities
  *
  * The contract store takes an index function which maps an instance to an index object L (a case
  * class), which allows to query the store using find(arg: L): Seq[Contract[TCid, T]].
  *
  * The store find / all commands will only return contracts for which no command has yet been
  * submitted that will consume the contract.
  *
  * @param name
  *   a name which we'll be using for logging
  * @param companion
  *   the template companion of the template we are watching
  * @param index
  *   the indexing function which takes an instance of a contract and returns some index object
  *   (query arguments)
  * @param filter
  *   filter to limit what is stored in the contract store
  * @param loggerFactory
  *   the logger factory
  * @param update
  *   update the contract (i.e. drop data to not waste storage)
  * @tparam TC
  *   the contract type (java code-gen)
  * @tparam TCid
  *   the contract id type (java code-gen)
  * @tparam T
  *   the contract template (java code-gen)
  * @tparam L
  *   the index expression
  */
class ContractStore[TC <: Contract[TCid, T], TCid <: ContractId[T], T, L](
    name: String,
    companion: ContractCompanion[TC, TCid, T],
    index: TC => L,
    filter: TC => Boolean,
    val loggerFactory: NamedLoggerFactory,
) extends BaseContractObserver[TC, TCid, T](companion)
    with NamedLogging
    with NoTracing {

  import com.digitalasset.canton.util.ShowUtil.*

  // our contract store with a flag indicating if there is a pending command which will consume the
  // contract if it succeeds
  private val store = TrieMap[ContractId[T], (Boolean, TC)]()

  // lookup data structure which tracks a list of contract ids that map to a given index L and a counter of how many of
  // these contracts are pending.
  private val lookup = TrieMap[L, (Int, Seq[ContractId[T]])]()

  /** return one contract that is not pending and that matches the query argument */
  def one(item: L): Option[TC] =
    lookup.get(item).flatMap { case (_, contracts) =>
      contracts.flatMap(y => store.get(y).toList).filterNot(_._1).map(_._2).headOption
    }

  /** find all contracts that are not pending and match the query argument */
  def find(item: L): Seq[TC] =
    lookup
      .get(item)
      .toList
      .flatMap(x => x._2.flatMap(y => store.get(y).toList).filterNot(_._1).map(_._2))

  /** return number of contracts matching the query argument excluding pending contracts */
  def num(item: L): Int = lookup.get(item).map(x => x._2.length - x._1).getOrElse(0)

  def entirelyEmpty: Boolean = store.isEmpty

  def hasPending: Boolean = lookup.exists(_._2._1 > 0)

  /** return all contracts which are not pending */
  def allAvailable: Iterable[Contract[TCid, T]] = store.values.filterNot(_._1).map(_._2)

  /** return total number of contracts in store, pending and non-pending */
  def totalNum: Int = store.size

  def setPending(cid: ContractId[T], isPending: Boolean): Boolean = blocking(synchronized {
    val current = store.get(cid)
    current.foreach { case (currentState, contract) =>
      logger.debug(s"Setting $cid to pending = $isPending")
      store.update(cid, (isPending, contract))
      // adjust pending counter in the lookup summary so we can
      // properly answer num(()) requests
      if (isPending != currentState) {
        val idx = index(contract)
        val cur = lookup.get(idx)
        val incOrDec = if (isPending) 1 else -1
        cur.foreach { case (count, seq) =>
          ErrorUtil.requireState(
            count + incOrDec > -1,
            s"Invalid new pending counter: $count + $incOrDec.",
          )
          lookup.update(idx, (count + incOrDec, seq))
        }
      }
    }
    current.isDefined
  })

  protected def contractCreated(create: TC, index: L): Unit = {}
  protected def contractArchived(archive: TC, index: L): Unit = {}

  override def reset(): Unit = {
    store.clear()
    lookup.clear()
  }

  protected def update(before: TC): TC = before

  override protected def processCreate_(create: TC): Unit = blocking(synchronized {
    if (filter(create)) {
      store += (create.id -> ((false, update(create))))
      val idx = index(create)
      lookup.get(idx) match {
        case None => lookup += (idx -> ((0, Seq(create.id))))
        case Some((pending, lst)) => lookup.update(idx, (pending, lst :+ create.id))
      }
      logger.debug(
        s"Observed create ${name.unquoted}, index=${idx.toString.unquoted}, cid=${create.id},$NL" +
          s"args=${create.data})"
      )
      contractCreated(create, idx)
    }
  })

  override protected def processArchive_(archive: ContractId[T]): Unit = blocking(synchronized {
    logger.debug(s"Observed archive ${name.unquoted} $archive")
    val contract = store.remove(archive)
    contract.foreach { case (pending, contract) =>
      val idx = index(contract)
      lookup.get(idx).foreach { case (numPending, seq) =>
        // decrement pending if necessary
        val newPending = if (pending) numPending - 1 else numPending
        lookup.update(idx, (newPending, seq.filter(_ != contract.id)))
      }
      contractArchived(contract, idx)
    }
  })

}
