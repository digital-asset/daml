// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.TransactionRoutingError
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.{
  InvalidPrescribedSynchronizerId,
  SubmissionSynchronizerNotReady,
}
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.{
  NotConnectedToAllContractSynchronizers,
  SubmitterAlwaysStakeholder,
}
import com.digitalasset.canton.error.TransactionRoutingError.{
  MalformedInputErrors,
  RoutingInternalError,
  UnableToQueryTopologySnapshot,
}
import com.digitalasset.canton.ledger.participant.state.{
  RoutingSynchronizerState,
  SubmitterInfo,
  SynchronizerRank,
  TransactionMeta,
}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.protocol.ContractAuthenticator
import com.digitalasset.canton.participant.protocol.TransactionProcessor.{
  TransactionSubmissionError,
  TransactionSubmissionResult,
}
import com.digitalasset.canton.participant.protocol.submission.routing.TransactionRoutingProcessor.inputContractsStakeholders
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.sync.ConnectedSynchronizersLookup
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.{LfKeyResolver, LfPartyId, checked}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.CreationTime

import scala.concurrent.ExecutionContext

/** The synchronizer router routes transaction submissions from upstream to the right synchronizer.
  *
  * Submitted transactions are inspected for which synchronizers are involved based on the location
  * of the involved contracts.
  */
class TransactionRoutingProcessor(
    contractsReassigner: ContractsReassigner,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    serializableContractAuthenticator: ContractAuthenticator,
    synchronizerRankComputation: SynchronizerRankComputation,
    synchronizerSelectorFactory: SynchronizerSelectorFactory,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  import com.digitalasset.canton.util.ShowUtil.*

  /** Reassigns the necessary transaction input contracts to the target synchronizer based on the
    * provided synchronizer rank, and then submits the transaction to the specified synchronizer.
    */
  final def submitTransaction(
      submitterInfo: SubmitterInfo,
      synchronizerRankTarget: SynchronizerRank,
      synchronizerState: RoutingSynchronizerState,
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      explicitlyDisclosedContracts: ImmArray[LfFatContractInst],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, FutureUnlessShutdown[
    TransactionSubmissionResult
  ]] = {
    val synchronizerId = synchronizerRankTarget.synchronizerId

    logger.debug(s"Routing the transaction to synchronizer $synchronizerId")

    for {
      // TODO(#25385) Not needed anymore if we just authenticate all before interpretation
      //          and ensure we just forward the payload
      inputDisclosedContracts <- EitherT
        .fromEither[FutureUnlessShutdown](
          for {
            _ <- explicitlyDisclosedContracts.toList
              .traverse_(serializableContractAuthenticator.authenticateFat)
              .leftMap(MalformedInputErrors.DisclosedContractAuthenticationFailed.Error.apply)
            inputDisclosedContracts <-
              explicitlyDisclosedContracts.toList
                .parTraverse(ContractInstance.apply[CreationTime.CreatedAt])
                .leftMap(MalformedInputErrors.InvalidDisclosedContract.Error.apply)

          } yield inputDisclosedContracts
        )
      _ <- contractsReassigner
        .reassign(synchronizerRankTarget, submitterInfo)

      topologySnapshot <- EitherT
        .fromEither[FutureUnlessShutdown] {
          synchronizerState.topologySnapshots
            .get(synchronizerRankTarget.synchronizerId)
            .toRight(UnableToQueryTopologySnapshot.Failed(synchronizerRankTarget.synchronizerId))
        }

      synchronizer <- EitherT.fromEither[FutureUnlessShutdown](
        connectedSynchronizersLookup
          .get(synchronizerId)
          .toRight[TransactionRoutingError](
            SubmissionSynchronizerNotReady.Error(synchronizerId)
          )
      )
      _ <- EitherT
        .cond[FutureUnlessShutdown](
          synchronizer.ready,
          (),
          SubmissionSynchronizerNotReady.Error(synchronizerId),
        )
      transactionSubmittedF <- wrapSubmissionError(synchronizer.psid)(
        synchronizer
          .submitTransaction(
            submitterInfo,
            transactionMeta,
            keyResolver,
            wfTransaction,
            inputDisclosedContracts.view.map(sc => sc.contractId -> sc).toMap,
            topologySnapshot,
          )(traceContext)
          .mapK(FutureUnlessShutdown.outcomeK)
      )
    } yield transactionSubmittedF
  }

  /** Computes the best synchronizer for a submitted transaction by checking the submitted
    * transaction against the topology of the connected synchronizers and ranking the admissible
    * ones.
    */
  def selectRoutingSynchronizer(
      submitterInfo: SubmitterInfo,
      transaction: LfSubmittedTransaction,
      synchronizerState: RoutingSynchronizerState,
      ledgerTime: CantonTimestamp,
      disclosedContractIds: List[LfContractId],
      synchronizerIdO: Option[SynchronizerId],
      transactionUsedForExternalSigning: Boolean,
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    TransactionRoutingError,
    SynchronizerRank,
  ] =
    for {
      contractsStakeholders <- EitherT.rightT[FutureUnlessShutdown, TransactionRoutingError](
        inputContractsStakeholders(transaction)
      )

      psidO <- EitherT.fromEither[FutureUnlessShutdown](
        synchronizerIdO.traverse(id =>
          synchronizerState
            .getPhysicalId(id)
            .toRight(
              InvalidPrescribedSynchronizerId
                .Generic(id, s"cannot resolve $id to a physical synchronizer id")
            )
        )
      )

      transactionData <- TransactionData.create(
        submitterInfo = submitterInfo,
        transaction = transaction,
        ledgerTime = ledgerTime,
        synchronizerState = synchronizerState,
        inputContractStakeholders = contractsStakeholders,
        disclosedContracts = disclosedContractIds,
        prescribedSynchronizerO = psidO,
      )

      locallyHostedSubmitters = Option
        .unless(transactionUsedForExternalSigning)(
          transactionData.actAs -- transactionData.externallySignedSubmissionO.fold(
            Set.empty[LfPartyId]
          )(_.signatures.keys.map(_.toLf).toSet)
        )
        .getOrElse(Set.empty)

      synchronizerSelector <- synchronizerSelectorFactory.create(
        transactionData = transactionData,
        synchronizerState = synchronizerState,
        submitters = locallyHostedSubmitters,
      )

      isMultiSynchronizerTx <- isMultiSynchronizerTx(
        inputSynchronizers = transactionData.inputContractsSynchronizerData.synchronizers,
        informees = transactionData.informees,
        synchronizerState = synchronizerState,
        synchronizerIdO = psidO,
      )

      synchronizerRankTarget <-
        if (!isMultiSynchronizerTx) {
          logger.debug(
            s"Choosing the synchronizer as single-synchronizer workflow for ${submitterInfo.commandId}"
          )
          synchronizerSelector.forSingleSynchronizer
        } else
          chooseSynchronizerForMultiSynchronizer(synchronizerSelector)

    } yield synchronizerRankTarget

  /** Computes the highest ranked synchronizer from the given admissible synchronizers without
    * performing topology checks.
    *
    * This method is used internally in command processing to pre-select a synchronizer for
    * determining the package preference set used in command interpretation.
    */
  def computeHighestRankedSynchronizerFromAdmissible(
      submitterInfo: SubmitterInfo,
      transaction: LfSubmittedTransaction,
      transactionMeta: TransactionMeta,
      admissibleSynchronizerIds: NonEmpty[Set[PhysicalSynchronizerId]],
      disclosedContractIds: List[LfContractId],
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, PhysicalSynchronizerId] =
    EitherT
      .fromEither[FutureUnlessShutdown](
        TransactionMetadata.fromTransactionMeta(
          metaLedgerEffectiveTime = transactionMeta.ledgerEffectiveTime,
          metaPreparationTime = transactionMeta.preparationTime,
          metaOptNodeSeeds = transactionMeta.optNodeSeeds,
        )
      )
      .leftMap(RoutingInternalError.IllformedTransaction.apply)
      .flatMap { metadata =>
        TransactionData
          .create(
            submitterInfo = submitterInfo,
            transaction = transaction,
            ledgerTime = metadata.ledgerTime,
            synchronizerState = routingSynchronizerState,
            inputContractStakeholders = inputContractsStakeholders(transaction),
            disclosedContracts = disclosedContractIds,
            prescribedSynchronizerO = None, // Not used here
          )
          .flatMap(transactionData =>
            synchronizerRankComputation
              .computeBestSynchronizerRank(
                synchronizerState = routingSynchronizerState,
                contracts = transactionData.inputContractsSynchronizerData.contractsData,
                readers = transactionData.readers,
                synchronizerIds = admissibleSynchronizerIds,
              )
          )
          .map(_.synchronizerId)
      }

  private def allInformeesOnSynchronizer(
      informees: Set[LfPartyId],
      synchronizerState: RoutingSynchronizerState,
  )(synchronizerId: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnableToQueryTopologySnapshot.Failed, Boolean] =
    for {
      snapshot <- EitherT.fromEither[FutureUnlessShutdown](
        synchronizerState.getTopologySnapshotFor(synchronizerId)
      )
      allInformeesOnSynchronizer <- EitherT.right(
        snapshot.allHaveActiveParticipants(informees).bimap(_ => false, _ => true).merge
      )
    } yield allInformeesOnSynchronizer

  private def chooseSynchronizerForMultiSynchronizer(
      synchronizerSelector: SynchronizerSelector
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] =
    for {
      _ <- checkValidityOfMultiSynchronizer(
        synchronizerSelector.transactionData,
        synchronizerSelector.synchronizerState,
      )
      synchronizerRankTarget <- synchronizerSelector.forMultiSynchronizer
    } yield synchronizerRankTarget

  /** We have a multi-synchronizer transaction if the input contracts are on more than one
    * synchronizer, if the (single) input synchronizer does not host all informees or if the target
    * synchronizer is different than the synchronizer of the input contracts (because we will need
    * to reassign the contracts to a synchronizer that *does* host all informees. Transactions
    * without input contracts are always single-synchronizer.
    */
  private def isMultiSynchronizerTx(
      inputSynchronizers: Set[PhysicalSynchronizerId],
      informees: Set[LfPartyId],
      synchronizerState: RoutingSynchronizerState,
      synchronizerIdO: Option[PhysicalSynchronizerId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnableToQueryTopologySnapshot.Failed, Boolean] =
    if (inputSynchronizers.sizeCompare(2) >= 0) EitherT.rightT(true)
    else if (
      synchronizerIdO
        .exists(targetSynchronizer => inputSynchronizers.exists(_ != targetSynchronizer))
    ) EitherT.rightT(true)
    else
      inputSynchronizers.toList
        .parTraverse(allInformeesOnSynchronizer(informees, synchronizerState)(_))
        .map(!_.forall(identity))

  private def checkValidityOfMultiSynchronizer(
      transactionData: TransactionData,
      synchronizerState: RoutingSynchronizerState,
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] = {
    val inputContractsSynchronizerData = transactionData.inputContractsSynchronizerData

    val contractData = inputContractsSynchronizerData.contractsData
    val contractsSynchronizerNotConnected = contractData.filter { contractData =>
      synchronizerState.getTopologySnapshotFor(contractData.synchronizerId).left.exists { _ =>
        true
      }
    }

    // Check that at least one party listed in actAs or readAs is a stakeholder so that we can reassign the contract if needed.
    // This check is overly strict on behalf of contracts that turn out not to need to be reassigned.
    val readerNotBeingStakeholder = contractData.filter { data =>
      data.stakeholders.all.intersect(transactionData.readers).isEmpty
    }

    for {
      // Check: reader
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        readerNotBeingStakeholder.isEmpty,
        SubmitterAlwaysStakeholder.Error(readerNotBeingStakeholder.map(_.id)),
      )

      // Check: connected synchronizers
      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](
          contractsSynchronizerNotConnected.isEmpty, {
            val contractsAndSynchronizers: Map[String, PhysicalSynchronizerId] =
              contractsSynchronizerNotConnected.map { contractData =>
                contractData.id.show -> contractData.synchronizerId
              }.toMap

            NotConnectedToAllContractSynchronizers.Error(
              contractsAndSynchronizers.view.mapValues(_.logical).toMap
            )
          },
        )
        .leftWiden[TransactionRoutingError]

    } yield ()
  }

  private def wrapSubmissionError[T](synchronizerId: PhysicalSynchronizerId)(
      eitherT: EitherT[FutureUnlessShutdown, TransactionSubmissionError, T]
  )(implicit ec: ExecutionContext): EitherT[FutureUnlessShutdown, TransactionRoutingError, T] =
    eitherT.leftMap(subm => TransactionRoutingError.SubmissionError(synchronizerId, subm))

}

object TransactionRoutingProcessor {
  def apply(
      connectedSynchronizersLookup: ConnectedSynchronizersLookup,
      synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
      cryptoPureApi: CryptoPureApi,
      participantId: ParticipantId,
      parameters: ParticipantNodeParameters,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): TransactionRoutingProcessor = {

    val reassigner =
      new ContractsReassigner(
        connectedSynchronizersLookup,
        submittingParticipant = participantId,
        loggerFactory,
      )

    val synchronizerRankComputation = new SynchronizerRankComputation(
      participantId = participantId,
      priorityOfSynchronizer = priorityOfSynchronizer(synchronizerConnectionConfigStore),
      loggerFactory = loggerFactory,
    )

    val synchronizerSelectorFactory = new SynchronizerSelectorFactory(
      admissibleSynchronizersComputation =
        new AdmissibleSynchronizersComputation(participantId, loggerFactory),
      priorityOfSynchronizer = priorityOfSynchronizer(synchronizerConnectionConfigStore),
      synchronizerRankComputation = synchronizerRankComputation,
      loggerFactory = loggerFactory,
    )

    val serializableContractAuthenticator = ContractAuthenticator(cryptoPureApi)

    new TransactionRoutingProcessor(
      reassigner,
      connectedSynchronizersLookup,
      serializableContractAuthenticator,
      synchronizerRankComputation,
      synchronizerSelectorFactory,
      parameters.processingTimeouts,
      loggerFactory,
    )
  }

  private def priorityOfSynchronizer(
      synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore
  )(synchronizerId: PhysicalSynchronizerId): Int = {
    val maybePriority = for {
      priority <- synchronizerConnectionConfigStore
        .get(synchronizerId)
        .toOption
        .map(_.config.priority)
    } yield priority

    // If the participant is disconnected from the synchronizer while this code is evaluated,
    // we may fail to determine the priority.
    // Choose the lowest possible priority, as it will be unlikely that a submission to the synchronizer succeeds.
    maybePriority.getOrElse(Integer.MIN_VALUE)
  }

  private[routing] def inputContractsStakeholders(
      tx: LfVersionedTransaction
  ): Map[LfContractId, Stakeholders] = {

    // TODO(#16065) Revisit this value
    val keyLookupMap = tx.nodes.values.collect { case LfNodeLookupByKey(_, _, key, Some(cid), _) =>
      cid -> checked(
        Stakeholders.tryCreate(stakeholders = key.maintainers, signatories = Set.empty)
      )
    }.toMap

    val mainMap = tx.nodes.values.collect {
      case n: LfNodeFetch =>
        val stakeholders = checked(
          Stakeholders.tryCreate(signatories = n.signatories, stakeholders = n.stakeholders)
        )
        n.coid -> stakeholders
      case n: LfNodeExercises =>
        val stakeholders = checked(
          Stakeholders.tryCreate(signatories = n.signatories, stakeholders = n.stakeholders)
        )

        n.targetCoid -> stakeholders
    }.toMap

    (keyLookupMap ++ mainMap) -- tx.localContracts.keySet
  }

}
