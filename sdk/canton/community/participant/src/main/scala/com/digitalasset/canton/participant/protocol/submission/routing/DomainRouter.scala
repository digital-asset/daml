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
import com.digitalasset.canton.participant.protocol.submission.routing.DomainRouter.inputContractsStakeholders
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore
import com.digitalasset.canton.participant.sync.TransactionRoutingError.ConfigurationErrors.{
  MultiDomainSupportNotEnabled,
  SubmissionDomainNotReady,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors.{
  NotConnectedToAllContractDomains,
  SubmitterAlwaysStakeholder,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.{
  MalformedInputErrors,
  RoutingInternalError,
  UnableToQueryTopologySnapshot,
}
import com.digitalasset.canton.participant.sync.{ConnectedDomainsLookup, TransactionRoutingError}
import com.digitalasset.canton.participant.synchronizer.SynchronizerAliasManager
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.{LfKeyResolver, LfPartyId, checked}
import com.digitalasset.daml.lf.data.ImmArray

import scala.concurrent.{ExecutionContext, Future}

/** The domain router routes transaction submissions from upstream to the right domain.
  *
  * Submitted transactions are inspected for which domains are involved based on the location of the involved contracts.
  *
  * @param submit Submits the given transaction to the given domain.
  *               The outer future completes after the submission has been registered as in-flight.
  *               The inner future completes after the submission has been sequenced or if it will never be sequenced.
  */
class DomainRouter(
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
    snapshotProvider: DomainStateProvider,
    serializableContractAuthenticator: SerializableContractAuthenticator,
    enableAutomaticReassignments: Boolean,
    domainSelectorFactory: DomainSelectorFactory,
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

      domainSelector <- domainSelectorFactory
        .create(transactionData)
      inputDomains = transactionData.inputContractsDomainData.domains

      isMultiDomainTx <- isMultiDomainTx(inputDomains, transactionData.informees, optSynchronizerId)

      domainRankTarget <- {
        if (!isMultiDomainTx) {
          logger.debug(
            s"Choosing the domain as single-domain workflow for ${submitterInfo.commandId}"
          )
          domainSelector.forSingleDomain
        } else if (enableAutomaticReassignments) {
          logger.debug(
            s"Choosing the domain as multi-domain workflow for ${submitterInfo.commandId}"
          )
          chooseDomainForMultiDomain(domainSelector)
        } else {
          EitherT.leftT[FutureUnlessShutdown, DomainRank](
            MultiDomainSupportNotEnabled.Error(
              transactionData.inputContractsDomainData.domains
            ): TransactionRoutingError
          )
        }
      }
      _ <- contractsReassigner
        .reassign(
          domainRankTarget,
          submitterInfo,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      _ = logger.debug(s"Routing the transaction to the ${domainRankTarget.synchronizerId}")
      transactionSubmittedF <- submit(domainRankTarget.synchronizerId)(
        submitterInfo,
        transactionMeta,
        keyResolver,
        wfTransaction,
        traceContext,
        inputDisclosedContracts.view.map(sc => sc.contractId -> sc).toMap,
      ).mapK(FutureUnlessShutdown.outcomeK)
    } yield transactionSubmittedF

  private def allInformeesOnDomain(
      informees: Set[LfPartyId]
  )(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnableToQueryTopologySnapshot.Failed, Boolean] =
    for {
      snapshot <- EitherT.fromEither[FutureUnlessShutdown](
        snapshotProvider.getTopologySnapshotFor(synchronizerId)
      )
      allInformeesOnDomain <- EitherT.right(
        snapshot.allHaveActiveParticipants(informees).bimap(_ => false, _ => true).merge
      )
    } yield allInformeesOnDomain

  private def chooseDomainForMultiDomain(
      domainSelector: DomainSelector
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, DomainRank] =
    for {
      _ <- checkValidityOfMultiDomain(domainSelector.transactionData)
      domainRankTarget <- domainSelector.forMultiDomain
    } yield domainRankTarget

  /** We have a multi-domain transaction if the input contracts are on more than one domain,
    * if the (single) input domain does not host all informees
    * or if the target domain is different than the domain of the input contracts
    * (because we will need to reassign the contracts to a domain that *does* host all informees.
    * Transactions without input contracts are always single-domain.
    */
  private def isMultiDomainTx(
      inputDomains: Set[SynchronizerId],
      informees: Set[LfPartyId],
      optSynchronizerId: Option[SynchronizerId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnableToQueryTopologySnapshot.Failed, Boolean] =
    if (inputDomains.sizeCompare(2) >= 0) EitherT.rightT(true)
    else if (
      optSynchronizerId
        .exists(targetDomain => inputDomains.exists(inputDomain => inputDomain != targetDomain))
    ) EitherT.rightT(true)
    else
      inputDomains.toList
        .parTraverse(allInformeesOnDomain(informees)(_))
        .map(!_.forall(identity))

  private def checkValidityOfMultiDomain(
      transactionData: TransactionData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] = {
    val inputContractsDomainData = transactionData.inputContractsDomainData

    val contractData = inputContractsDomainData.withDomainData
    val contractsDomainNotConnected = contractData.filter { contractData =>
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

      // Check: connected domains
      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](
          contractsDomainNotConnected.isEmpty, {
            val contractsAndDomains: Map[String, SynchronizerId] = contractsDomainNotConnected.map {
              contractData => contractData.id.show -> contractData.synchronizerId
            }.toMap

            NotConnectedToAllContractDomains.Error(contractsAndDomains)
          },
        )
        .leftWiden[TransactionRoutingError]

    } yield ()
  }
}

object DomainRouter {
  def apply(
      connectedDomains: ConnectedDomainsLookup,
      domainConnectionConfigStore: DomainConnectionConfigStore,
      synchronizerAliasManager: SynchronizerAliasManager,
      cryptoPureApi: CryptoPureApi,
      participantId: ParticipantId,
      parameters: ParticipantNodeParameters,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): DomainRouter = {

    val reassigner =
      new ContractsReassigner(
        connectedDomains,
        submittingParticipant = participantId,
        loggerFactory,
      )

    val domainStateProvider = new DomainStateProviderImpl(connectedDomains)
    val domainRankComputation = new DomainRankComputation(
      participantId = participantId,
      priorityOfSynchronizer =
        priorityOfDomain(domainConnectionConfigStore, synchronizerAliasManager),
      snapshotProvider = domainStateProvider,
      loggerFactory = loggerFactory,
    )

    val domainSelectorFactory = new DomainSelectorFactory(
      admissibleDomains = new AdmissibleDomains(participantId, connectedDomains, loggerFactory),
      priorityOfSynchronizer =
        priorityOfDomain(domainConnectionConfigStore, synchronizerAliasManager),
      domainRankComputation = domainRankComputation,
      domainStateProvider = domainStateProvider,
      loggerFactory = loggerFactory,
    )

    val serializableContractAuthenticator = SerializableContractAuthenticator(cryptoPureApi)

    new DomainRouter(
      submit(connectedDomains),
      reassigner,
      domainStateProvider,
      serializableContractAuthenticator,
      enableAutomaticReassignments = parameters.enablePreviewFeatures,
      domainSelectorFactory,
      parameters.processingTimeouts,
      loggerFactory,
    )
  }

  private def priorityOfDomain(
      domainConnectionConfigStore: DomainConnectionConfigStore,
      synchronizerAliasManager: SynchronizerAliasManager,
  )(synchronizerId: SynchronizerId): Int = {
    val maybePriority = for {
      synchronizerAlias <- synchronizerAliasManager.aliasForSynchronizerId(synchronizerId)
      config <- domainConnectionConfigStore.get(synchronizerAlias).toOption.map(_.config)
    } yield config.priority

    // If the participant is disconnected from the domain while this code is evaluated,
    // we may fail to determine the priority.
    // Choose the lowest possible priority, as it will be unlikely that a submission to the domain succeeds.
    maybePriority.getOrElse(Integer.MIN_VALUE)
  }

  /** We intentionally do not store the `keyResolver` in [[com.digitalasset.canton.protocol.WellFormedTransaction]]
    * because we do not (yet) need to deal with merging the mappings
    * in [[com.digitalasset.canton.protocol.WellFormedTransaction.merge]].
    */
  private def submit(connectedDomains: ConnectedDomainsLookup)(synchronizerId: SynchronizerId)(
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
      domain <- EitherT.fromEither[Future](
        connectedDomains
          .get(synchronizerId)
          .toRight[TransactionRoutingError](SubmissionDomainNotReady.Error(synchronizerId))
      )
      _ <- EitherT.cond[Future](domain.ready, (), SubmissionDomainNotReady.Error(synchronizerId))
      result <- wrapSubmissionError(domain.synchronizerId)(
        domain.submitTransaction(
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
