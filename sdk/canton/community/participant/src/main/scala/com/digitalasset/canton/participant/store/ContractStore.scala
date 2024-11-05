// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.instances.list.*
import cats.syntax.foldable.*
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}
import com.digitalasset.canton.store.Purgeable
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait ContractStore extends ContractLookup with Purgeable {

  /** Stores contracts created by a request.
    * Assumes the contract data has been authenticated against the contract id using
    * [[com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator]].
    * If the same contract instance has been stored before, the fields not covered by the contract id authentication will be updated.
    *
    * @param creations      The contracts to be created together with the transaction id and the request counter
    */
  def storeCreatedContracts(
      creations: Seq[(SerializableContract, RequestCounter)]
  )(implicit traceContext: TraceContext): Future[Unit]

  def storeCreatedContract(
      requestCounter: RequestCounter,
      contract: SerializableContract,
  )(implicit traceContext: TraceContext): Future[Unit] =
    storeCreatedContracts(Seq((contract, requestCounter)))

  /** Store divulged contracts.
    * Assumes the contract data has been authenticated against the contract id using
    * [[com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator]].
    *
    * If the same contract instance has been stored before, the fields not covered by the contract id authentication will be updated.
    * The method will however not override a contract that has previously been stored as created contract.
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
  * @param contract       The contract to be stored
  * @param requestCounter The request counter of the latest request that stored the contract.
  * @param isDivulged     Whether the contract was divulged
  */
final case class StoredContract(
    contract: SerializableContract,
    requestCounter: RequestCounter,
    isDivulged: Boolean,
) extends PrettyPrinting {
  def contractId: LfContractId = contract.contractId

  def mergeWith(other: StoredContract): StoredContract =
    if (this eq other) this
    else {
      require(
        this.contractId == other.contractId,
        s"Cannot merge $this with $other due to different contract ids",
      )
      if (
        isDivulged && !other.isDivulged ||
        isDivulged == other.isDivulged && requestCounter < other.requestCounter
      ) {
        copy(
          requestCounter = other.requestCounter,
          isDivulged = other.isDivulged,
        )
      } else this
    }

  override protected def pretty: Pretty[StoredContract] = prettyOfClass(
    param("contract", _.contract),
    param("request counter", _.requestCounter),
    param("is divulged", _.isDivulged),
  )
}

object StoredContract {

  def fromCreatedContract(
      contract: SerializableContract,
      requestCounter: RequestCounter,
  ): StoredContract =
    StoredContract(contract, requestCounter, isDivulged = false)

  def fromDivulgedContract(
      contract: SerializableContract,
      requestCounter: RequestCounter,
  ): StoredContract =
    StoredContract(contract, requestCounter, isDivulged = true)
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
