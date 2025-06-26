// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.{EitherT, OptionT}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** An in-memory contract store. This class is thread-safe. */
class InMemoryContractStore(
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(
    protected implicit val ec: ExecutionContext
) extends ContractStore
    with NamedLogging {

  override protected[store] def logger: TracedLogger = super.logger

  /** Invariants:
    *   - Every [[LfFatContractInst]] is stored under [[LfFatContractInst.contractId]].
    */
  private[this] val contracts = TrieMap.empty[LfContractId, ContractInstance]

  /** Debug find utility to search pcs
    */
  override def find(
      filterId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[List[ContractInstance]] = {
    def search(
        needle: String,
        accessor: ContractInstance => String,
    ): ContractInstance => Boolean =
      needle match {
        case rs if rs.startsWith("!") => accessor(_) == needle.drop(1)
        case rs if rs.startsWith("^") => accessor(_).startsWith(needle.drop(1))
        case _ => accessor(_).contains(needle)
      }
    val flt1 =
      filterPackage.map(search(_, _.templateId.packageId))
    val flt2 = filterTemplate.map(
      search(_, _.templateId.qualifiedName.qualifiedName)
    )
    val flt3 = filterId.map(search(_, _.contractId.coid))

    def conjunctiveFilter(sc: ContractInstance): Boolean =
      flt1.forall(_(sc)) && flt2.forall(_(sc)) && flt3.forall(_(sc))
    FutureUnlessShutdown.pure(
      contracts.values.filter(conjunctiveFilter).take(limit).toList
    )
  }

  def findWithPayload(
      contractIds: NonEmpty[Seq[LfContractId]]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, ContractInstance]] =
    FutureUnlessShutdown.pure(
      contractIds
        .map(cid => cid -> contracts.get(cid))
        .collect { case (cid, Some(contract)) => cid -> contract }
        .toMap
    )

  override def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, ContractInstance] = {
    logger.debug(s"Looking up contract: $id")
    OptionT(FutureUnlessShutdown.pure {
      val result = contracts.get(id)
      result.fold(logger.debug(s"Contract $id not found"))(contract =>
        logger.debug(
          s"Found contract $id of type ${contract.templateId.qualifiedName.qualifiedName}"
        )
      )
      result
    })
  }

  override def storeContracts(
      contracts: Seq[ContractInstance]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    contracts.foreach(store)
    FutureUnlessShutdown.unit
  }

  private def store(storedContract: ContractInstance): Unit =
    contracts
      .putIfAbsent(storedContract.contractId, storedContract)
      .discard[Option[ContractInstance]]

  override def deleteIgnoringUnknown(
      ids: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    ids.foreach(id => contracts.remove(id).discard[Option[ContractInstance]])
    FutureUnlessShutdown.unit
  }

  override def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    contracts.clear()
    FutureUnlessShutdown.unit
  }

  override def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] = {
    val res = contracts.filter { case (cid, _) => ids.contains(cid) }.map { case (cid, c) =>
      (cid, c.stakeholders)
    }
    EitherT.cond(res.sizeCompare(ids) == 0, res.toMap, UnknownContracts(ids -- res.keySet))
  }

  override def lookupSignatories(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] = {
    val res = contracts.filter { case (cid, _) => ids.contains(cid) }.map { case (cid, c) =>
      (cid, c.inst.signatories)
    }
    EitherT.cond(res.sizeCompare(ids) == 0, res.toMap, UnknownContracts(ids -- res.keySet))
  }

  override def contractCount()(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    FutureUnlessShutdown.pure(contracts.size)
}
