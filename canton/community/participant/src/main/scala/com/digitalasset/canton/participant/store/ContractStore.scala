// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.instances.list.*
import cats.syntax.foldable.*
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract, TransactionId}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait ContractStore extends ContractLookup {

  /** Stores contracts created by a request.
    * If the same contract instance has been stored before, the request counter is set to the lower one.
    *
    * @param requestCounter The request counter of the transaction '''creating''' the contracts.
    * @param transactionId: The transaction ID that created the contract
    * @param creations      The contracts to be created
    * @return [[DuplicateContract]] for the contracts that have been or are being inserted
    *         with different values for [[com.digitalasset.canton.protocol.SerializableContract.contractInstance]]
    *         or [[com.digitalasset.canton.protocol.SerializableContract.ledgerCreateTime]].
    */
  def storeCreatedContracts(
      requestCounter: RequestCounter,
      transactionId: TransactionId,
      creations: Seq[SerializableContract],
  )(implicit traceContext: TraceContext): Future[Unit]

  def storeCreatedContract(
      requestCounter: RequestCounter,
      transactionId: TransactionId,
      contract: SerializableContract,
  )(implicit traceContext: TraceContext): Future[Unit] =
    storeCreatedContracts(requestCounter, transactionId, Seq(contract))

  /** Store divulged contracts.
    *
    * @return [[DuplicateContract]] for the contracts that have been or are being inserted
    *         with a different contract instance or a different ledger time.
    */
  def storeDivulgedContracts(
      requestCounter: RequestCounter,
      divulgences: Seq[SerializableContract],
  )(implicit traceContext: TraceContext): Future[Unit]

  def storeDivulgedContract(requestCounter: RequestCounter, contract: SerializableContract)(implicit
      traceContext: TraceContext
  ): Future[Unit] = storeDivulgedContracts(requestCounter, Seq(contract))

  /** Removes the contract from the contract store. */
  def deleteContract(id: LfContractId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownContract, Unit]

  /** Debug find utility to search pcs
    */
  def find(
      filterId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[List[SerializableContract]]

  /** Deletes multiple contracts from the contract store.
    *
    * Ignores errors due to a contract not being present in the store, fails on other errors.
    */
  def deleteIgnoringUnknown(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Deletes all divulged contracts up to a given request counter. */
  def deleteDivulged(upTo: RequestCounter)(implicit traceContext: TraceContext): Future[Unit]

  def contractCount()(implicit traceContext: TraceContext): Future[Int]

  def hasActiveContracts(
      partyId: PartyId,
      contractIds: Iterator[LfContractId],
      batchSize: Int = 10,
  )(implicit
      traceContext: TraceContext
  ): Future[Boolean] = {
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
}

/** Data to be stored for a contract.
  *
  * @param contract               The contract to be stored
  * @param requestCounter         The request counter of the latest request that stored the contract.
  * @param creatingTransactionIdO The id of the transaction that created the contract.
  *                               [[scala.None]] for divulged and witnessed contracts.
  */
final case class StoredContract(
    contract: SerializableContract,
    requestCounter: RequestCounter,
    creatingTransactionIdO: Option[TransactionId],
) extends PrettyPrinting {
  def contractId: LfContractId = contract.contractId

  def mergeWith(other: StoredContract): StoredContract = {
    if (this eq other) this
    else {
      require(
        this.contractId == other.contractId,
        s"Cannot merge $this with $other due to different contract ids",
      )
      if (creatingTransactionIdO.isEmpty && other.creatingTransactionIdO.isDefined) {
        other
      } else if (
        creatingTransactionIdO.isDefined == other.creatingTransactionIdO.isDefined && requestCounter < other.requestCounter
      ) {
        other
      } else {
        this
      }
    }
  }

  override def pretty: Pretty[StoredContract] = prettyOfClass(
    param("id", _.contractId),
    param("let", _.contract.ledgerCreateTime.ts),
    param("request counter", _.requestCounter),
    paramIfDefined("creating transaction id", _.creatingTransactionIdO),
  )
}

object StoredContract {
  def fromCreatedContract(
      contract: SerializableContract,
      requestCounter: RequestCounter,
      transactionId: TransactionId,
  ): StoredContract =
    StoredContract(contract, requestCounter, Some(transactionId))

  def fromDivulgedContract(
      contract: SerializableContract,
      requestCounter: RequestCounter,
  ): StoredContract =
    StoredContract(contract, requestCounter, None)
}

sealed trait ContractStoreError extends Product with Serializable with PrettyPrinting

final case class DuplicateContract(
    id: LfContractId,
    oldContract: StoredContract,
    newContract: StoredContract,
) extends ContractStoreError {
  override def pretty: Pretty[DuplicateContract] = prettyOfClass(
    param("id", _.id),
    param("old contract", _.oldContract),
    param("new contract", _.newContract),
  )
}

sealed trait ContractLookupError extends ContractStoreError

final case class UnknownContract(contractId: LfContractId) extends ContractLookupError {
  override def pretty: Pretty[UnknownContract] = prettyOfClass(unnamedParam(_.contractId))
}
final case class UnknownContracts(contractIds: Set[LfContractId]) extends ContractLookupError {
  override def pretty: Pretty[UnknownContracts] = prettyOfClass(unnamedParam(_.contractIds))
}
