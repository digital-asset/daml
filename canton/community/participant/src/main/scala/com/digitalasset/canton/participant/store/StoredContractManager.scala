// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Id
import cats.data.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.protocol.{
  LfContractId,
  SerializableContract,
  TransactionId,
  WithTransactionId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{Checked, CheckedT, MapsUtil}
import com.digitalasset.canton.{LfPartyId, RequestCounter}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Manages a contract store by keeping pending contracts in memory.
  * [[ContractLookup.lookup]] finds these pending contracts in addition to those in the backing [[ContractStore]].
  * A contract ceases to be pending as soon as first request writes it to the backing store
  * using [[StoredContractManager.commitIfPending]].
  *
  * Unlike [[ExtendedContractLookup]], the pending contracts can be updated and committed to the backing store,
  * concurrently with other contract lookups and additions.
  */
class StoredContractManager(store: ContractStore, override val loggerFactory: NamedLoggerFactory)(
    override protected implicit val ec: ExecutionContext
) extends NamedLogging
    with ContractLookup {
  import StoredContractManager.PendingContract

  /** Stores the currently pending contracts.
    *
    * Never stores a contract that is already in the backing [[ContractStore]].
    */
  private val pendingContracts: TrieMap[LfContractId, PendingContract] =
    TrieMap.empty[LfContractId, PendingContract]

  override protected[store] def logger: TracedLogger = super.logger

  /** Finds the contracts in the backing store and among the pending contracts. */
  override def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[Future, StoredContract] =
    pendingContracts
      .get(id)
      .fold(store.lookup(id))(pending => OptionT.pure[Future](pending.storedContract))

  /** Adds the given contracts as pending,
    * unless they are already in the store with a request counter.
    * If they are already pending, the request will be added only
    * if the [[com.digitalasset.canton.protocol.SerializableContract]]s are the same.
    *
    * Checking the contracts in the store is not atomic with adding them as pending.
    *
    * @return The contracts whose [[com.digitalasset.canton.protocol.SerializableContract]] or creating [[com.digitalasset.canton.protocol.TransactionId]]
    *         differs from what is stored in the backing [[ContractStore]] with a request counter or among the pending contracts.
    *         In particular, this implies that those contracts are already pending or in the backing [[ContractStore]]
    *         with a request counter.
    */
  // TODO(i12908) Make the checks against the store atomic with the additions to pending.
  def addPendingContracts(
      requestCounter: RequestCounter,
      contracts: Seq[WithTransactionId[SerializableContract]],
  )(implicit traceContext: TraceContext): Future[Set[StoredContract]] = {

    // IorT is not the right thing here. Its flatmap does not continue on lefts :-( We need CheckedT here!

    def checkContractAgainstStore(
        contractAndId: WithTransactionId[SerializableContract]
    ): CheckedT[Future, Nothing, StoredContract, Option[WithTransactionId[SerializableContract]]] =
      CheckedT {
        val WithTransactionId(contract, creatingTransactionId) = contractAndId
        store.lookup(contract.contractId).value.map {
          case Some(stored) =>
            stored.creatingTransactionIdO match {
              case None =>
                Checked.result(
                  Some(contractAndId)
                ) // the stored contract has only been divulged, so we do not trust that the stored data is correct
              case Some(`creatingTransactionId`) if stored.contract == contract =>
                Checked.result(None)
              case _ =>
                Checked.continueWithResult(stored, None)
            }
          case None => Checked.result(Some(contractAndId))
        }
      }

    def addContract(
        contractAndId: WithTransactionId[SerializableContract]
    ): Option[StoredContract] = {
      import com.digitalasset.canton.util.IdUtil.catsSemigroupForIdRightBias

      val WithTransactionId(contract, creatingTransactionId) = contractAndId
      val contractId = contract.contractId

      val (differentContractO, ()) = MapsUtil
        .updateWithConcurrentlyM_[(Id[Option[StoredContract]], *), LfContractId, PendingContract](
          pendingContracts,
          contractId,
          None -> PendingContract(contract, creatingTransactionId, NonEmpty(Set, requestCounter)),
          {
            case pending @ PendingContract(
                  previousContract,
                  previousCreatingTransactionId,
                  requests,
                ) =>
              if (
                previousContract == contract && previousCreatingTransactionId == creatingTransactionId
              ) {
                val next = pending.copy(requests = requests.incl(requestCounter))
                None -> next
              } else Some(pending.storedContract) -> pending
          },
        )
      differentContractO
    }

    for {
      wrongContractsC <- contracts.parTraverse(checkContractAgainstStore).map(_.flattenOption).value
    } yield {
      val contractsToAdd = wrongContractsC.getResult.getOrElse(Seq.empty)
      val contractsWithDifferentDataInStore = wrongContractsC.nonaborts.toList.toSet
      val contractsWithDifferentDataInPending = contractsToAdd.mapFilter(addContract).toSet
      contractsWithDifferentDataInStore ++ contractsWithDifferentDataInPending
    }
  }

  /** Writes the contracts mapped to `true` to the contract store provided that they are still pending and removes them from pending.
    * Marks contracts mapped to `false` as not pending any more, unless another request still has marked them as pending.
    * Does not check that the request has actually marked the contract as pending with [[addPendingContracts]].
    */
  def commitIfPending(requestCounter: RequestCounter, contracts: Map[LfContractId, Boolean])(
      implicit traceContext: TraceContext
  ): Future[Unit] = {

    def removeRequestFromPendingContract(pending: PendingContract): Id[Option[PendingContract]] =
      NonEmpty
        .from(pending.requests - requestCounter)
        .map(newRequests => pending.copy(requests = newRequests))

    def rollback(contractId: LfContractId): Future[Unit] =
      Future(
        MapsUtil.modifyWithConcurrentlyM_(
          pendingContracts,
          contractId,
          None: Id[Option[PendingContract]],
          removeRequestFromPendingContract,
        )
      )

    def storeContract(
        pending: PendingContract
    )(implicit traceContext: TraceContext): Future[Option[PendingContract]] = {
      logger.debug(
        s"Request $requestCounter: Writing contract ${pending.contract.contractId} to the store"
      )

      store
        .storeCreatedContract(requestCounter, pending.creatingTransactionId, pending.contract)
        .map(_ => None)
    }

    def writeIfPending(contractId: LfContractId): Future[Unit] =
      MapsUtil
        .modifyWithConcurrentlyM_(
          pendingContracts,
          contractId,
          Future.successful(Option.empty[PendingContract]),
          storeContract,
        )

    contracts.toList
      .parTraverse {
        case (contractId, true) => writeIfPending(contractId)
        case (contractId, false) => rollback(contractId)
      }
      .map(_ => ())
  }

  /** Removes given contracts from pending except if another contract has still marked them as pending. */
  def deleteIfPending(requestCounter: RequestCounter, contracts: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    commitIfPending(requestCounter, contracts.map(_ -> false).toMap)

  /** Stores the given contracts as divulged in the contract store, checking them for consistency against pending contracts. */
  def storeDivulgedContracts(
      requestCounter: RequestCounter,
      divulgences: Seq[SerializableContract],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, NonEmptyChain[DuplicateContract], Unit] = {
    def storeDivulgedContract(
        divulgence: SerializableContract
    ): EitherT[Future, DuplicateContract, Unit] = {
      val contractId = divulgence.contractId
      val sameContractIfPresent = pendingContracts.get(contractId) match {
        case None => Right(())
        case Some(pending) =>
          Either.cond(
            pending.contract == divulgence,
            (),
            DuplicateContract(
              contractId,
              pending.storedContract,
              StoredContract.fromDivulgedContract(divulgence, requestCounter),
            ),
          )
      }

      for {
        _ <- EitherT.fromEither[Future](sameContractIfPresent)
        _ <- EitherT.right(store.storeDivulgedContract(requestCounter, divulgence))
      } yield ()
    }

    EitherT {
      divulgences
        .parTraverse(divulgence => storeDivulgedContract(divulgence).toValidatedNec)
        .map(_.sequence_.toEither)
    }
  }

  // This functionality isn't used by any clients of the manager
  override def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] =
    throw new NotImplementedError("StoredContractManager doesn't support bulk stakeholder lookups")

}

object StoredContractManager {

  /** The pending contract and the requests that have marked it as pending. */
  final case class PendingContract(
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      requests: NonEmpty[Set[RequestCounter]],
  ) {
    def storedContract: StoredContract =
      StoredContract(contract, requests.head1, Some(creatingTransactionId))
  }

}
