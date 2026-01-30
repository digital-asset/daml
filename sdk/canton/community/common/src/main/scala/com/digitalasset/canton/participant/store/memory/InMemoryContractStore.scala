// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.participant.store.ContractStore.InternalContractId
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicLong
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
  private[this] val contracts = TrieMap.empty[LfContractId, PersistedContractInstance]
  private[this] val internalIds = TrieMap.empty[InternalContractId, LfContractId]
  private[this] val index = new AtomicLong(0)

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
        accessor: PersistedContractInstance => String,
    ): PersistedContractInstance => Boolean =
      needle match {
        case rs if rs.startsWith("!") => accessor(_) == needle.drop(1)
        case rs if rs.startsWith("^") => accessor(_).startsWith(needle.drop(1))
        case _ => accessor(_).contains(needle)
      }
    val flt1 =
      filterPackage.map(search(_, _.inst.templateId.packageId))
    val flt2 = filterTemplate.map(
      search(_, _.inst.templateId.qualifiedName.qualifiedName)
    )
    val flt3 = filterId.map(search(_, _.inst.contractId.coid))

    def conjunctiveFilter(sc: PersistedContractInstance): Boolean =
      flt1.forall(_(sc)) && flt2.forall(_(sc)) && flt3.forall(_(sc))
    FutureUnlessShutdown.pure(
      contracts.values.view.filter(conjunctiveFilter).map(_.asContractInstance).take(limit).toList
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
        .collect { case (cid, Some(contract)) => cid -> contract.asContractInstance }
        .toMap
    )

  override def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, ContractInstance] = {
    logger.debug(s"Looking up contract: $id")
    OptionT(FutureUnlessShutdown.pure {
      val result = contracts.get(id).map(_.asContractInstance)
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
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, InternalContractId]] =
    FutureUnlessShutdown.pure(
      contracts.map(contract => contract.contractId -> store(contract)).toMap
    )

  private def store(
      storedContract: ContractInstance
  )(implicit traceContext: TraceContext): InternalContractId = {
    val newIdx = index.getAndIncrement()
    // use a new index only if the contract is not already present
    val idx =
      contracts
        .putIfAbsent(
          storedContract.contractId,
          PersistedContractInstance(newIdx, storedContract.inst),
        ) match {
        case Some(PersistedContractInstance(existingIdx, inst)) =>
          if (inst != storedContract.inst)
            ErrorUtil.internalError(
              new IllegalStateException(
                s"Stored contracts are immutable, but found different contract ${storedContract.contractId}"
              )
            )
          else
            existingIdx
        case None => newIdx
      }
    internalIds
      .putIfAbsent(idx, storedContract.contractId)
      .discard[Option[LfContractId]]
    idx
  }

  override def deleteIgnoringUnknown(
      ids: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    ids.foreach(id =>
      contracts
        .remove(id)
        .flatMap { case PersistedContractInstance(iid, _) => internalIds.remove(iid) }
        .discard[Option[LfContractId]]
    )
    FutureUnlessShutdown.unit
  }

  override def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    contracts.clear()
    internalIds.clear()
    FutureUnlessShutdown.unit
  }

  override def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] = {
    val res = contracts.filter { case (cid, _) => ids.contains(cid) }.map {
      case (cid, PersistedContractInstance(_, c)) =>
        (cid, c.stakeholders)
    }
    EitherT.cond(res.sizeCompare(ids) == 0, res.toMap, UnknownContracts(ids -- res.keySet))
  }

  override def lookupSignatories(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] = {
    val res = contracts.filter { case (cid, _) => ids.contains(cid) }.map {
      case (cid, PersistedContractInstance(_, c)) =>
        (cid, c.signatories)
    }
    EitherT.cond(res.sizeCompare(ids) == 0, res.toMap, UnknownContracts(ids -- res.keySet))
  }

  override def contractCount()(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    FutureUnlessShutdown.pure(contracts.size)

  override def lookupPersistedIfCached(id: LfContractId)(implicit
      traceContext: TraceContext
  ): Option[Option[PersistedContractInstance]] =
    Some(contracts.get(id))

  override def lookupPersisted(id: LfContractId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PersistedContractInstance]] =
    FutureUnlessShutdown.pure(contracts.get(id))

  override def lookupBatchedNonReadThrough(internalContractIds: Iterable[InternalContractId])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Map[InternalContractId, PersistedContractInstance]] =
    FutureUnlessShutdown.pure(
      internalContractIds
        .flatMap(internalIds.get(_).flatMap(contracts.get))
        .map { persistedContractInstance =>
          persistedContractInstance.internalContractId -> persistedContractInstance
        }
        .toMap
    )

  override def lookupBatchedInternalIdsNonReadThrough(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, InternalContractId]] =
    FutureUnlessShutdown.pure(
      contractIds
        .flatMap(cid =>
          contracts.get(cid).map { persistedContractInstance =>
            (cid, persistedContractInstance.internalContractId)
          }
        )
        .toMap
    )

  override def lookupBatchedContractIdsNonReadThrough(
      internalContractIds: Iterable[InternalContractId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[InternalContractId, LfContractId]] =
    FutureUnlessShutdown.pure(
      internalContractIds
        .flatMap(iid => internalIds.get(iid).map(iid -> _))
        .toMap
    )
}
