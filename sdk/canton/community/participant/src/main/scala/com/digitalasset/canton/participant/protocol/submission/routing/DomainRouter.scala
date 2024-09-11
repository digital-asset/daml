// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.participant.domain.DomainAliasManager
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
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{LfKeyResolver, LfPartyId}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}

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
    submit: DomainId => (
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
      optDomainId: Option[DomainId],
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: LfSubmittedTransaction,
      explicitlyDisclosedContracts: ImmArray[ProcessedDisclosedContract],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, FutureUnlessShutdown[TransactionSubmissionResult]] =
    for {
      // do some sanity checks for invalid inputs (to not conflate these with broken nodes)
      _ <- EitherT.fromEither[Future](
        WellFormedTransaction.sanityCheckInputs(transaction).leftMap {
          case WellFormedTransaction.InvalidInput.InvalidParty(err) =>
            MalformedInputErrors.InvalidPartyIdentifier.Error(err)
        }
      )

      inputDisclosedContracts <- EitherT
        .fromEither[Future](
          for {
            inputDisclosedContracts <-
              explicitlyDisclosedContracts.toList
                .parTraverse(SerializableContract.fromDisclosedContract)
                .leftMap(MalformedInputErrors.InvalidDisclosedContract.Error)
            _ <- inputDisclosedContracts
              .traverse_(serializableContractAuthenticator.authenticate)
              .leftMap(MalformedInputErrors.DisclosedContractAuthenticationFailed.Error)
          } yield inputDisclosedContracts
        )

      metadata <- EitherT
        .fromEither[Future](
          TransactionMetadata.fromTransactionMeta(
            metaLedgerEffectiveTime = transactionMeta.ledgerEffectiveTime,
            metaSubmissionTime = transactionMeta.submissionTime,
            metaOptNodeSeeds = transactionMeta.optNodeSeeds,
          )
        )
        .leftMap(RoutingInternalError.IllformedTransaction)

      wfTransaction <- EitherT.fromEither[Future](
        WellFormedTransaction
          .normalizeAndCheck(transaction, metadata, WithoutSuffixes)
          .leftMap(RoutingInternalError.IllformedTransaction)
      )

      contractsStakeholders = inputContractsStakeholders(wfTransaction.unwrap)

      transactionData <- TransactionData.create(
        submitterInfo,
        transaction,
        metadata.ledgerTime,
        snapshotProvider,
        contractsStakeholders,
        inputDisclosedContracts.map(_.contractId),
        optDomainId,
      )

      domainSelector <- domainSelectorFactory.create(transactionData)
      inputDomains = transactionData.inputContractsDomainData.domains

      isMultiDomainTx <- isMultiDomainTx(inputDomains, transactionData.informees, optDomainId)

      domainRankTarget <-
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
          EitherT.leftT[Future, DomainRank](
            MultiDomainSupportNotEnabled.Error(
              transactionData.inputContractsDomainData.domains
            ): TransactionRoutingError
          )
        }
      _ <- contractsReassigner.reassign(
        domainRankTarget,
        submitterInfo,
      )
      _ = logger.debug(s"Routing the transaction to the ${domainRankTarget.domainId}")
      transactionSubmittedF <- submit(domainRankTarget.domainId)(
        submitterInfo,
        transactionMeta,
        keyResolver,
        wfTransaction,
        traceContext,
        inputDisclosedContracts.view.map(sc => sc.contractId -> sc).toMap,
      )
    } yield transactionSubmittedF

  private def allInformeesOnDomain(
      informees: Set[LfPartyId]
  )(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnableToQueryTopologySnapshot.Failed, Boolean] =
    for {
      snapshot <- EitherT.fromEither[Future](snapshotProvider.getTopologySnapshotFor(domainId))
      allInformeesOnDomain <- EitherT.right(
        snapshot.allHaveActiveParticipants(informees).bimap(_ => false, _ => true).merge
      )
    } yield allInformeesOnDomain

  private def chooseDomainForMultiDomain(
      domainSelector: DomainSelector
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, DomainRank] =
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
      inputDomains: Set[DomainId],
      informees: Set[LfPartyId],
      optDomainId: Option[DomainId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnableToQueryTopologySnapshot.Failed, Boolean] =
    if (inputDomains.sizeCompare(2) >= 0) EitherT.rightT(true)
    else if (
      optDomainId
        .exists(targetDomain => inputDomains.exists(inputDomain => inputDomain != targetDomain))
    ) EitherT.rightT(true)
    else
      inputDomains.toList
        .parTraverse(allInformeesOnDomain(informees)(_))
        .map(!_.forall(identity))

  private def checkValidityOfMultiDomain(
      transactionData: TransactionData
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, Unit] = {
    val inputContractsDomainData = transactionData.inputContractsDomainData

    val contractData = inputContractsDomainData.withDomainData
    val contractsDomainNotConnected = contractData.filter { contractData =>
      snapshotProvider.getTopologySnapshotFor(contractData.domain).left.exists { _ =>
        true
      }
    }

    // Check that at least one party listed in actAs or readAs is a stakeholder so that we can reassign the contract if needed.
    // This check is overly strict on behalf of contracts that turn out not to need to be reassigned.
    val readerNotBeingStakeholder = contractData.filter { data =>
      data.stakeholders.intersect(transactionData.readers).isEmpty
    }

    for {
      // Check: reader
      _ <- EitherTUtil.condUnitET[Future](
        readerNotBeingStakeholder.isEmpty,
        SubmitterAlwaysStakeholder.Error(readerNotBeingStakeholder.map(_.id)),
      )

      // Check: connected domains
      _ <- EitherTUtil
        .condUnitET[Future](
          contractsDomainNotConnected.isEmpty, {
            val contractsAndDomains: Map[String, DomainId] = contractsDomainNotConnected.map {
              contractData => contractData.id.show -> contractData.domain
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
      domainAliasManager: DomainAliasManager,
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
      priorityOfDomain = priorityOfDomain(domainConnectionConfigStore, domainAliasManager),
      snapshotProvider = domainStateProvider,
      loggerFactory = loggerFactory,
    )

    val domainSelectorFactory = new DomainSelectorFactory(
      admissibleDomains = new AdmissibleDomains(participantId, connectedDomains, loggerFactory),
      priorityOfDomain = priorityOfDomain(domainConnectionConfigStore, domainAliasManager),
      domainRankComputation = domainRankComputation,
      domainStateProvider = domainStateProvider,
      loggerFactory = loggerFactory,
    )

    val serializableContractAuthenticator = SerializableContractAuthenticator(
      cryptoPureApi,
      parameters,
    )

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
      domainAliasManager: DomainAliasManager,
  )(domainId: DomainId): Int = {
    val maybePriority = for {
      domainAlias <- domainAliasManager.aliasForDomainId(domainId)
      config <- domainConnectionConfigStore.get(domainAlias).toOption.map(_.config)
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
  private def submit(connectedDomains: ConnectedDomainsLookup)(domainId: DomainId)(
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
          .get(domainId)
          .toRight[TransactionRoutingError](SubmissionDomainNotReady.Error(domainId))
      )
      _ <- EitherT.cond[Future](domain.ready, (), SubmissionDomainNotReady.Error(domainId))
      result <- wrapSubmissionError(domain.domainId)(
        domain.submitTransaction(
          submitterInfo,
          transactionMeta,
          keyResolver,
          tx,
          disclosedContracts,
        )(traceContext)
      )
    } yield result

  private def wrapSubmissionError[T](domainId: DomainId)(
      eitherT: EitherT[Future, TransactionSubmissionError, T]
  )(implicit ec: ExecutionContext): EitherT[Future, TransactionRoutingError, T] =
    eitherT.leftMap(subm => TransactionRoutingError.SubmissionError(domainId, subm))

  private[routing] def inputContractsStakeholders(
      tx: LfVersionedTransaction
  ): Map[LfContractId, Set[Ref.Party]] = {

    val keyLookupMap = tx.nodes.values.collect { case LfNodeLookupByKey(_, _, key, Some(cid), _) =>
      cid -> key.maintainers
    }.toMap

    val mainMap = tx.nodes.values.collect {
      case n: LfNodeFetch => n.coid -> n.stakeholders
      case n: LfNodeExercises => n.targetCoid -> n.stakeholders
    }.toMap

    (keyLookupMap ++ mainMap) -- tx.localContracts.keySet

  }

}
