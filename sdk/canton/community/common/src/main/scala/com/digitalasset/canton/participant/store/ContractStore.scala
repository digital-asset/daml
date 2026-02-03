// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.instances.list.*
import cats.syntax.foldable.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.ContractStore.InternalContractId
import com.digitalasset.canton.participant.store.db.DbContractStore
import com.digitalasset.canton.participant.store.memory.InMemoryContractStore
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.Purgeable
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.transaction.{CreationTime, FatContractInstance}

import scala.concurrent.ExecutionContext

trait ContractStore extends ContractLookup with Purgeable with FlagCloseable {

  def lookupPersistedIfCached(id: LfContractId)(implicit
      traceContext: TraceContext
  ): Option[Option[PersistedContractInstance]]

  def lookupPersisted(id: LfContractId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PersistedContractInstance]]

  def lookupBatchedNonReadThrough(internalContractIds: Iterable[InternalContractId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[InternalContractId, PersistedContractInstance]]

  def lookupBatchedInternalIdsNonReadThrough(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, InternalContractId]]

  def lookupBatchedContractIdsNonReadThrough(internalContractIds: Iterable[InternalContractId])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Map[InternalContractId, LfContractId]]

  override type ContractsCreatedAtTime = CreationTime.CreatedAt

  /** Stores contracts created by a request.
    *
    * @param contracts
    *   The created contracts to be stored
    *
    * @return
    *   A map from contract IDs to internal contract IDs of the stored contracts
    */
  def storeContracts(contracts: Seq[ContractInstance])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, InternalContractId]]

  def storeContract(contract: ContractInstance)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = storeContracts(Seq(contract)).map(_ => ())

  /** Debug find utility to search pcs
    */
  def find(
      exactId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[List[ContractInstance]]

  /** Debug find utility to search pcs. Omits contracts that are not found.
    */
  def findWithPayload(
      contractIds: NonEmpty[Seq[LfContractId]]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, ContractInstance]]

  /** Deletes multiple contracts from the contract store.
    *
    * Ignores errors due to a contract not being present in the store, fails on other errors.
    */
  def deleteIgnoringUnknown(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  def contractCount()(implicit traceContext: TraceContext): FutureUnlessShutdown[Int]

  // TODO(i24535): implement this on db level
  def hasActiveContracts(
      partyId: PartyId,
      contractIds: Iterator[LfContractId],
      batchSize: Int = 10,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = {
    val lfParty = partyId.toLf

    contractIds
      .grouped(batchSize)
      .toList
      .findM(cids =>
        lookupStakeholders(cids.toSet).value.map {
          case Right(x) =>
            x.exists { case (_, listParties) => listParties.contains(lfParty) }
          case Left(_) => false
        }
      )
      .map(_.nonEmpty)
  }

  // TODO(i24535): implement this on db level
  def isSignatoryOnActiveContracts(
      partyId: PartyId,
      contractIds: Iterator[LfContractId],
      batchSize: Int = 10,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = {
    val lfParty = partyId.toLf
    contractIds
      .grouped(batchSize)
      .toList
      .findM(cids =>
        lookupSignatories(cids.toSet).value.map {
          case Right(x) =>
            x.exists { case (_, listParties) => listParties.contains(lfParty) }
          case Left(_) => false
        }
      )
      .map(_.nonEmpty)
  }
}

object ContractStore {
  type InternalContractId = Long

  def create(
      storage: Storage,
      processingTimeouts: ProcessingTimeout,
      cachingConfigs: CachingConfigs,
      batchingConfig: BatchingConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): ContractStore =
    storage match {
      case _: MemoryStorage =>
        new InMemoryContractStore(processingTimeouts, loggerFactory)

      case dbStorage: DbStorage =>
        new DbContractStore(
          dbStorage,
          cacheConfig = cachingConfigs.contractStore,
          dbQueryBatcherConfig = batchingConfig.contractStoreAggregator,
          insertBatchAggregatorConfig = batchingConfig.aggregator,
          processingTimeouts,
          loggerFactory,
        )
    }
}

final case class PersistedContractInstance(
    internalContractId: InternalContractId,
    inst: FatContractInstance { type CreatedAtTime <: CreationTime.CreatedAt },
) {
  def asContractInstance: ContractInstance = ContractInstance.create(inst) match {
    case Right(contract) => contract
    case Left(e) => throw new DbDeserializationException(s"Invalid contract instance: $e")
  }
}

sealed trait ContractStoreError extends Product with Serializable with PrettyPrinting

sealed trait ContractLookupError extends ContractStoreError

final case class UnknownContract(contractId: LfContractId) extends ContractLookupError {
  override protected def pretty: Pretty[UnknownContract] = prettyOfClass(unnamedParam(_.contractId))
}
final case class UnknownContracts(contractIds: Set[LfContractId]) extends ContractLookupError {
  override protected def pretty: Pretty[UnknownContracts] = prettyOfClass(
    unnamedParam(_.contractIds)
  )
}
final case class FailedConvert(contractId: LfContractId) extends ContractLookupError {
  override protected def pretty: Pretty[FailedConvert] = prettyOfClass(unnamedParam(_.contractId))
}
