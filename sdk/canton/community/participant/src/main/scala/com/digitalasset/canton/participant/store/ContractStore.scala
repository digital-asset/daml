// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.instances.list.*
import cats.syntax.foldable.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.store.db.DbContractStore
import com.digitalasset.canton.participant.store.memory.InMemoryContractStore
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.Purgeable
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.transaction.CreationTime

import scala.concurrent.ExecutionContext

trait ContractStore extends ContractLookup with Purgeable with FlagCloseable {

  override type ContractsCreatedAtTime = CreationTime.CreatedAt

  /** Stores contracts created by a request. Assumes the contract data has been authenticated
    * against the contract id using [[com.digitalasset.canton.util.ContractAuthenticator]].
    *
    * @param contracts
    *   The created contracts to be stored
    */
  def storeContracts(contracts: Seq[ContractInstance])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  def storeContract(contract: ContractInstance)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = storeContracts(Seq(contract))

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
  def create(
      storage: Storage,
      parameters: ParticipantNodeParameters,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): ContractStore =
    storage match {
      case _: MemoryStorage =>
        new InMemoryContractStore(parameters.processingTimeouts, loggerFactory)

      case dbStorage: DbStorage =>
        new DbContractStore(
          dbStorage,
          cacheConfig = parameters.cachingConfigs.contractStore,
          dbQueryBatcherConfig = parameters.batchingConfig.aggregator,
          insertBatchAggregatorConfig = parameters.batchingConfig.aggregator,
          parameters.processingTimeouts,
          loggerFactory,
        )
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
