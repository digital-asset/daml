// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.{CantonTimestamp, ProcessedDisclosedContract}
import com.digitalasset.canton.ledger.participant.state.{
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
import com.digitalasset.canton.participant.sync.TransactionRoutingError.ConfigurationErrors.{
  MultiSynchronizerSupportNotEnabled,
  SubmissionSynchronizerNotReady,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors.{
  NotConnectedToAllContractSynchronizers,
  SubmitterAlwaysStakeholder,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.{
  MalformedInputErrors,
  RoutingInternalError,
  UnableToQueryTopologySnapshot,
}
import com.digitalasset.canton.participant.sync.{
  ConnectedSynchronizersLookup,
  TransactionRoutingError,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerAliasManager
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.{LfKeyResolver, LfPartyId, checked}
import com.digitalasset.daml.lf.data.ImmArray

import scala.concurrent.{ExecutionContext, Future}

/** The synchronizer router routes transaction submissions from upstream to the right synchronizer.
  *
  * Submitted transactions are inspected for which synchronizers are involved based on the location
  * of the involved contracts.
  */
class TransactionRoutingProcessor(
    contractsReassigner: ContractsReassigner,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    serializableContractAuthenticator: ContractAuthenticator,
    enableAutomaticReassignments: Boolean,
    synchronizerSelectorFactory: SynchronizerSelectorFactory,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  import com.digitalasset.canton.util.ShowUtil.*

  def submitTransaction(
      submitterInfo: SubmitterInfo,
      optSynchronizerId: Option[SynchronizerId],
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: LfSubmittedTransaction,
      explicitlyDisclosedContracts: ImmArray[ProcessedDisclosedContract],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, FutureUnlessShutdown[
    TransactionSubmissionResult
  ]] =
    for {
      // do some sanity checks for invalid inputs (to not conflate these with broken nodes)
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        WellFormedTransaction.sanityCheckInputs(transaction).leftMap {
          case WellFormedTransaction.InvalidInput.InvalidParty(err) =>
            MalformedInputErrors.InvalidPartyIdentifier.Error(err)
        }
      )

      inputDisclosedContracts <- EitherT
        .fromEither[FutureUnlessShutdown](
          for {
            inputDisclosedContracts <-
              explicitlyDisclosedContracts.toList
                .parTraverse(SerializableContract.fromDisclosedContract)
                .leftMap(MalformedInputErrors.InvalidDisclosedContract.Error.apply)
            _ <- inputDisclosedContracts
              .traverse_(serializableContractAuthenticator.authenticateSerializable)
              .leftMap(MalformedInputErrors.DisclosedContractAuthenticationFailed.Error.apply)
          } yield inputDisclosedContracts
        )

      metadata <- EitherT
        .fromEither[FutureUnlessShutdown](
          TransactionMetadata.fromTransactionMeta(
            metaLedgerEffectiveTime = transactionMeta.ledgerEffectiveTime,
            metaSubmissionTime = transactionMeta.submissionTime,
            metaOptNodeSeeds = transactionMeta.optNodeSeeds,
          )
        )
        .leftMap(RoutingInternalError.IllformedTransaction.apply)

      wfTransaction <- EitherT.fromEither[FutureUnlessShutdown](
        WellFormedTransaction
          .normalizeAndCheck(transaction, metadata, WithoutSuffixes)
          .leftMap(RoutingInternalError.IllformedTransaction.apply)
      )

      synchronizerState = RoutingSynchronizerState(connectedSynchronizersLookup)

      synchronizerRankTarget <- selectRoutingSynchronizer(
        submitterInfo,
        transaction,
        synchronizerState,
        metadata.ledgerTime,
        inputDisclosedContracts.map(_.contractId),
        optSynchronizerId,
      )
      _ <- contractsReassigner
        .reassign(
          synchronizerRankTarget,
          submitterInfo,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      _ = logger.debug(s"Routing the transaction to the ${synchronizerRankTarget.synchronizerId}")

      topologySnapshot <- EitherT
        .fromEither[Future] {
          synchronizerState.topologySnapshots
            .get(synchronizerRankTarget.synchronizerId)
            .toRight(UnableToQueryTopologySnapshot.Failed(synchronizerRankTarget.synchronizerId))
        }
        .mapK(FutureUnlessShutdown.outcomeK)

      transactionSubmittedF <- submit(synchronizerRankTarget.synchronizerId)(
        submitterInfo,
        transactionMeta,
        keyResolver,
        wfTransaction,
        traceContext,
        inputDisclosedContracts.view.map(sc => sc.contractId -> sc).toMap,
        topologySnapshot,
      ).mapK(FutureUnlessShutdown.outcomeK)
    } yield transactionSubmittedF

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
      optSynchronizerId: Option[SynchronizerId],
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    TransactionRoutingError,
    SynchronizerRank,
  ] =
    for {
      contractsStakeholders <- EitherT.rightT[FutureUnlessShutdown, TransactionRoutingError](
        inputContractsStakeholders(transaction)
      )

      transactionData <- TransactionData.create(
        submitterInfo = submitterInfo,
        transaction = transaction,
        ledgerTime = ledgerTime,
        synchronizerState = synchronizerState,
        inputContractStakeholders = contractsStakeholders,
        disclosedContracts = disclosedContractIds,
        prescribedSynchronizerO = optSynchronizerId,
      )

      locallyHostedSubmitters =
        transactionData.actAs -- transactionData.externallySignedSubmissionO.fold(
          Set.empty[LfPartyId]
        )(_.signatures.keys.map(_.toLf).toSet)

      synchronizerSelector <- synchronizerSelectorFactory.create(
        transactionData = transactionData,
        synchronizerState = synchronizerState,
        submitters = locallyHostedSubmitters,
      )

      isMultiSynchronizerTx <- isMultiSynchronizerTx(
        inputSynchronizers = transactionData.inputContractsSynchronizerData.synchronizers,
        informees = transactionData.informees,
        synchronizerState = synchronizerState,
        optSynchronizerId = optSynchronizerId,
      )

      synchronizerRankTarget <-
        if (!isMultiSynchronizerTx) {
          logger.debug(
            s"Choosing the synchronizer as single-synchronizer workflow for ${submitterInfo.commandId}"
          )
          synchronizerSelector.forSingleSynchronizer
        } else if (enableAutomaticReassignments) {
          logger.debug(
            s"Choosing the synchronizer as multi-synchronizer workflow for ${submitterInfo.commandId}"
          )
          chooseSynchronizerForMultiSynchronizer(synchronizerSelector)
        } else
          EitherT.leftT[FutureUnlessShutdown, SynchronizerRank](
            MultiSynchronizerSupportNotEnabled.Error(
              transactionData.inputContractsSynchronizerData.synchronizers
            ): TransactionRoutingError
          )
    } yield synchronizerRankTarget

  private def allInformeesOnSynchronizer(
      informees: Set[LfPartyId],
      synchronizerState: RoutingSynchronizerState,
  )(synchronizerId: SynchronizerId)(implicit
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
      inputSynchronizers: Set[SynchronizerId],
      informees: Set[LfPartyId],
      synchronizerState: RoutingSynchronizerState,
      optSynchronizerId: Option[SynchronizerId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnableToQueryTopologySnapshot.Failed, Boolean] =
    if (inputSynchronizers.sizeCompare(2) >= 0) EitherT.rightT(true)
    else if (
      optSynchronizerId
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
            val contractsAndSynchronizers: Map[String, SynchronizerId] =
              contractsSynchronizerNotConnected.map { contractData =>
                contractData.id.show -> contractData.synchronizerId
              }.toMap

            NotConnectedToAllContractSynchronizers.Error(contractsAndSynchronizers)
          },
        )
        .leftWiden[TransactionRoutingError]

    } yield ()
  }

  /** We intentionally do not store the `keyResolver` in
    * [[com.digitalasset.canton.protocol.WellFormedTransaction]] because we do not (yet) need to
    * deal with merging the mappings in
    * [[com.digitalasset.canton.protocol.WellFormedTransaction.merge]].
    */
  private def submit(synchronizerId: SynchronizerId)(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      tx: WellFormedTransaction[WithoutSuffixes],
      traceContext: TraceContext,
      disclosedContracts: Map[LfContractId, SerializableContract],
      topologySnapshot: TopologySnapshot,
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, TransactionRoutingError, FutureUnlessShutdown[TransactionSubmissionResult]] =
    for {
      synchronizer <- EitherT.fromEither[Future](
        connectedSynchronizersLookup
          .get(synchronizerId)
          .toRight[TransactionRoutingError](SubmissionSynchronizerNotReady.Error(synchronizerId))
      )
      _ <- EitherT
        .cond[Future](synchronizer.ready, (), SubmissionSynchronizerNotReady.Error(synchronizerId))
      result <- wrapSubmissionError(synchronizer.synchronizerId)(
        synchronizer.submitTransaction(
          submitterInfo,
          transactionMeta,
          keyResolver,
          tx,
          disclosedContracts,
          topologySnapshot,
        )(traceContext)
      )
    } yield result

  private def wrapSubmissionError[T](synchronizerId: SynchronizerId)(
      eitherT: EitherT[Future, TransactionSubmissionError, T]
  )(implicit ec: ExecutionContext): EitherT[Future, TransactionRoutingError, T] =
    eitherT.leftMap(subm => TransactionRoutingError.SubmissionError(synchronizerId, subm))

}

object TransactionRoutingProcessor {
  def apply(
      connectedSynchronizersLookup: ConnectedSynchronizersLookup,
      synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
      synchronizerAliasManager: SynchronizerAliasManager,
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
      priorityOfSynchronizer =
        priorityOfSynchronizer(synchronizerConnectionConfigStore, synchronizerAliasManager),
      loggerFactory = loggerFactory,
    )

    val synchronizerSelectorFactory = new SynchronizerSelectorFactory(
      admissibleSynchronizersComputation =
        new AdmissibleSynchronizersComputation(participantId, loggerFactory),
      priorityOfSynchronizer =
        priorityOfSynchronizer(synchronizerConnectionConfigStore, synchronizerAliasManager),
      synchronizerRankComputation = synchronizerRankComputation,
      loggerFactory = loggerFactory,
    )

    val serializableContractAuthenticator = ContractAuthenticator(cryptoPureApi)

    new TransactionRoutingProcessor(
      reassigner,
      connectedSynchronizersLookup,
      serializableContractAuthenticator,
      enableAutomaticReassignments = parameters.enablePreviewFeatures,
      synchronizerSelectorFactory,
      parameters.processingTimeouts,
      loggerFactory,
    )
  }

  private def priorityOfSynchronizer(
      synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
      synchronizerAliasManager: SynchronizerAliasManager,
  )(synchronizerId: SynchronizerId): Int = {
    val maybePriority = for {
      synchronizerAlias <- synchronizerAliasManager.aliasForSynchronizerId(synchronizerId)
      config <- synchronizerConnectionConfigStore.get(synchronizerAlias).toOption.map(_.config)
    } yield config.priority

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
