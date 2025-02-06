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
    * <ol>
    *   <li>Every [[SerializableContract]] is stored under [[SerializableContract.contractId]].</li>
    * </ol>
    */
  private[this] val contracts = TrieMap.empty[LfContractId, SerializableContract]

  /** Debug find utility to search pcs
    */
  override def find(
      filterId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[List[SerializableContract]] = {
    def search(
        needle: String,
        accessor: SerializableContract => String,
    ): SerializableContract => Boolean =
      needle match {
        case rs if rs.startsWith("!") => accessor(_) == needle.drop(1)
        case rs if rs.startsWith("^") => accessor(_).startsWith(needle.drop(1))
        case _ => accessor(_).contains(needle)
      }
    val flt1 =
      filterPackage.map(search(_, _.contractInstance.unversioned.template.packageId))
    val flt2 = filterTemplate.map(
      search(_, _.contractInstance.unversioned.template.qualifiedName.qualifiedName)
    )
    val flt3 = filterId.map(search(_, _.contractId.coid))

    def conjunctiveFilter(sc: SerializableContract): Boolean =
      flt1.forall(_(sc)) && flt2.forall(_(sc)) && flt3.forall(_(sc))
    FutureUnlessShutdown.pure(
      contracts.values.filter(conjunctiveFilter).take(limit).toList
    )
  }

  def findWithPayload(
      contractIds: NonEmpty[Seq[LfContractId]],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, SerializableContract]] =
    FutureUnlessShutdown.pure(
      contractIds
        .map(cid => cid -> contracts.get(cid))
        .collect { case (cid, Some(contract)) => cid -> contract }
        .take(limit)
        .toMap
    )

  override def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, SerializableContract] = {
    logger.debug(s"Looking up contract: $id")
    OptionT(FutureUnlessShutdown.pure {
      val result = contracts.get(id)
      result.fold(logger.debug(s"Contract $id not found"))(contract =>
        logger.debug(
          s"Found contract $id of type ${contract.contractInstance.unversioned.template.qualifiedName.qualifiedName}"
        )
      )
      result
    })
  }

  override def storeContracts(
      contracts: Seq[SerializableContract]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    contracts.foreach(store)
    FutureUnlessShutdown.unit
  }

  private def store(storedContract: SerializableContract): Unit =
    contracts
      .putIfAbsent(storedContract.contractId, storedContract)
      .discard[Option[SerializableContract]]

  override def deleteIgnoringUnknown(
      ids: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    ids.foreach(id => contracts.remove(id).discard[Option[SerializableContract]])
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
      (cid, c.metadata.stakeholders)
    }
    EitherT.cond(res.sizeCompare(ids) == 0, res.toMap, UnknownContracts(ids -- res.keySet))
  }

  override def contractCount()(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    FutureUnlessShutdown.pure(contracts.size)
}
