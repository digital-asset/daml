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
import com.digitalasset.canton.data.ProcessedDisclosedContract
import com.digitalasset.canton.ledger.participant.state.{SubmitterInfo, TransactionMeta}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.participant.protocol.TransactionProcessor.{
  TransactionSubmissionError,
  TransactionSubmissionResult,
}
import com.digitalasset.canton.participant.protocol.submission.routing.SynchronizerRouter.inputContractsStakeholders
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
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.{LfKeyResolver, LfPartyId, checked}
import com.digitalasset.daml.lf.data.ImmArray

import scala.concurrent.{ExecutionContext, Future}

/** The synchronizer router routes transaction submissions from upstream to the right domain.
  *
  * Submitted transactions are inspected for which synchronizers are involved based on the location of the involved contracts.
  *
  * @param submit Submits the given transaction to the given domain.
  *               The outer future completes after the submission has been registered as in-flight.
  *               The inner future completes after the submission has been sequenced or if it will never be sequenced.
  */
class SynchronizerRouter(
    submit: SynchronizerId => (
        SubmitterInfo,
        TransactionMeta,
        LfKeyResolver,
        WellFormedTransaction[WithoutSuffixes],
        TraceContext,
        Map[LfContractId, SerializableContract],
    ) => EitherT[Future, TransactionRoutingError, FutureUnlessShutdown[
      TransactionSubmissionResult
    ]],
    contractsReassigner: ContractsReassigner,
    snapshotProvider: SynchronizerStateProvider,
    serializableContractAuthenticator: SerializableContractAuthenticator,
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
              .traverse_(serializableContractAuthenticator.authenticate)
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

      contractsStakeholders = inputContractsStakeholders(wfTransaction.unwrap)

      transactionData <- TransactionData.create(
        submitterInfo,
        transaction,
        metadata.ledgerTime,
        snapshotProvider,
        contractsStakeholders,
        inputDisclosedContracts.map(_.contractId),
        optSynchronizerId,
      )

      synchronizerSelector <- synchronizerSelectorFactory
        .create(transactionData)
      inputSynchronizers = transactionData.inputContractsSynchronizerData.synchronizers

      isMultiSynchronizerTx <- isMultiSynchronizerTx(
        inputSynchronizers,
        transactionData.informees,
        optSynchronizerId,
      )

      synchronizerRankTarget <- {
        if (!isMultiSynchronizerTx) {
          logger.debug(
            s"Choosing the synchronizer as single-domain workflow for ${submitterInfo.commandId}"
          )
          synchronizerSelector.forSingleSynchronizer
        } else if (enableAutomaticReassignments) {
          logger.debug(
            s"Choosing the synchronizer as multi-domain workflow for ${submitterInfo.commandId}"
          )
          chooseSynchronizerForMultiSynchronizer(synchronizerSelector)
        } else {
          EitherT.leftT[FutureUnlessShutdown, SynchronizerRank](
            MultiSynchronizerSupportNotEnabled.Error(
              transactionData.inputContractsSynchronizerData.synchronizers
            ): TransactionRoutingError
          )
        }
      }
      _ <- contractsReassigner
        .reassign(
          synchronizerRankTarget,
          submitterInfo,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      _ = logger.debug(s"Routing the transaction to the ${synchronizerRankTarget.synchronizerId}")
      transactionSubmittedF <- submit(synchronizerRankTarget.synchronizerId)(
        submitterInfo,
        transactionMeta,
        keyResolver,
        wfTransaction,
        traceContext,
        inputDisclosedContracts.view.map(sc => sc.contractId -> sc).toMap,
      ).mapK(FutureUnlessShutdown.outcomeK)
    } yield transactionSubmittedF

  private def allInformeesOnSynchronizer(
      informees: Set[LfPartyId]
  )(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnableToQueryTopologySnapshot.Failed, Boolean] =
    for {
      snapshot <- EitherT.fromEither[FutureUnlessShutdown](
        snapshotProvider.getTopologySnapshotFor(synchronizerId)
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
      _ <- checkValidityOfMultiSynchronizer(synchronizerSelector.transactionData)
      synchronizerRankTarget <- synchronizerSelector.forMultiSynchronizer
    } yield synchronizerRankTarget

  /** We have a multi-domain transaction if the input contracts are on more than one domain,
    * if the (single) input synchronizer does not host all informees
    * or if the target synchronizer is different than the synchronizer of the input contracts
    * (because we will need to reassign the contracts to a synchronizer that *does* host all informees.
    * Transactions without input contracts are always single-domain.
    */
  private def isMultiSynchronizerTx(
      inputSynchronizers: Set[SynchronizerId],
      informees: Set[LfPartyId],
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
        .parTraverse(allInformeesOnSynchronizer(informees)(_))
        .map(!_.forall(identity))

  private def checkValidityOfMultiSynchronizer(
      transactionData: TransactionData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] = {
    val inputContractsSynchronizerData = transactionData.inputContractsSynchronizerData

    val contractData = inputContractsSynchronizerData.contractsData
    val contractsSynchronizerNotConnected = contractData.filter { contractData =>
      snapshotProvider.getTopologySnapshotFor(contractData.synchronizerId).left.exists { _ =>
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
}

object SynchronizerRouter {
  def apply(
      connectedSynchronizersLookup: ConnectedSynchronizersLookup,
      synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
      synchronizerAliasManager: SynchronizerAliasManager,
      cryptoPureApi: CryptoPureApi,
      participantId: ParticipantId,
      parameters: ParticipantNodeParameters,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): SynchronizerRouter = {

    val reassigner =
      new ContractsReassigner(
        connectedSynchronizersLookup,
        submittingParticipant = participantId,
        loggerFactory,
      )

    val synchronizerStateProvider = new SynchronizerStateProviderImpl(connectedSynchronizersLookup)
    val synchronizerRankComputation = new SynchronizerRankComputation(
      participantId = participantId,
      priorityOfSynchronizer =
        priorityOfSynchronizer(synchronizerConnectionConfigStore, synchronizerAliasManager),
      snapshotProvider = synchronizerStateProvider,
      loggerFactory = loggerFactory,
    )

    val synchronizerSelectorFactory = new SynchronizerSelectorFactory(
      admissibleSynchronizers =
        new AdmissibleSynchronizers(participantId, connectedSynchronizersLookup, loggerFactory),
      priorityOfSynchronizer =
        priorityOfSynchronizer(synchronizerConnectionConfigStore, synchronizerAliasManager),
      synchronizerRankComputation = synchronizerRankComputation,
      synchronizerStateProvider = synchronizerStateProvider,
      loggerFactory = loggerFactory,
    )

    val serializableContractAuthenticator = SerializableContractAuthenticator(cryptoPureApi)

    new SynchronizerRouter(
      submit(connectedSynchronizersLookup),
      reassigner,
      synchronizerStateProvider,
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

  /** We intentionally do not store the `keyResolver` in [[com.digitalasset.canton.protocol.WellFormedTransaction]]
    * because we do not (yet) need to deal with merging the mappings
    * in [[com.digitalasset.canton.protocol.WellFormedTransaction.merge]].
    */
  private def submit(
      connectedSynchronizers: ConnectedSynchronizersLookup
  )(synchronizerId: SynchronizerId)(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      tx: WellFormedTransaction[WithoutSuffixes],
      traceContext: TraceContext,
      disclosedContracts: Map[LfContractId, SerializableContract],
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, TransactionRoutingError, FutureUnlessShutdown[TransactionSubmissionResult]] =
    for {
      synchronizer <- EitherT.fromEither[Future](
        connectedSynchronizers
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
        )(traceContext)
      )
    } yield result

  private def wrapSubmissionError[T](synchronizerId: SynchronizerId)(
      eitherT: EitherT[Future, TransactionSubmissionError, T]
  )(implicit ec: ExecutionContext): EitherT[Future, TransactionRoutingError, T] =
    eitherT.leftMap(subm => TransactionRoutingError.SubmissionError(synchronizerId, subm))

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
