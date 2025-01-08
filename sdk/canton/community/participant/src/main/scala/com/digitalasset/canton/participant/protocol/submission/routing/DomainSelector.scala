// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyColl.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.UsableDomains
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.RoutingInternalError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors.NoDomainForSubmission
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

private[routing] class DomainSelectorFactory(
    admissibleDomains: AdmissibleDomains,
    priorityOfSynchronizer: SynchronizerId => Int,
    domainRankComputation: DomainRankComputation,
    domainStateProvider: DomainStateProvider,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext) {
  def create(
      transactionData: TransactionData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, DomainSelector] =
    for {
      admissibleDomains <- admissibleDomains.forParties(
        submitters = transactionData.actAs -- transactionData.externallySignedSubmissionO.fold(
          Set.empty[LfPartyId]
        )(_.signatures.keys.map(_.toLf).toSet),
        informees = transactionData.informees,
      )
    } yield new DomainSelector(
      transactionData,
      admissibleDomains,
      priorityOfSynchronizer,
      domainRankComputation,
      domainStateProvider,
      loggerFactory,
    )
}

/** Selects the best domain for routing.
  *
  * @param admissibleDomains     Domains that host both submitters and informees of the transaction:
  *                          - submitters have to be hosted on the local participant
  *                          - informees have to be hosted on some participant
  *                            It is assumed that the participant is connected to all domains in `connectedDomains`
  * @param priorityOfSynchronizer      Priority of each domain (lowest number indicates highest priority)
  * @param domainRankComputation Utility class to compute `DomainRank`
  * @param domainStateProvider   Provides state information about a domain.
  *                              Note: returns an either rather than an option since failure comes from disconnected
  *                              domains and we assume the participant to be connected to all domains in `connectedDomains`
  */
private[routing] class DomainSelector(
    val transactionData: TransactionData,
    admissibleDomains: NonEmpty[Set[SynchronizerId]],
    priorityOfSynchronizer: SynchronizerId => Int,
    domainRankComputation: DomainRankComputation,
    domainStateProvider: DomainStateProvider,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Choose the appropriate domain for a transaction.
    * The domain is chosen as follows:
    * 1. Domain whose id equals `transactionData.prescribedDomainO` (if non-empty)
    * 2. The domain with the smaller number of reassignments on which all informees have active participants
    */
  def forMultiDomain(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, DomainRank] = {
    val contracts = transactionData.inputContractsDomainData.withDomainData

    transactionData.prescribedDomainO match {
      case Some(prescribedDomain) =>
        for {
          _ <- validatePrescribedDomain(prescribedDomain)
          domainRank <- domainRankComputation
            .compute(
              contracts,
              Target(prescribedDomain),
              transactionData.readers,
            )
            .mapK(FutureUnlessShutdown.outcomeK)
        } yield domainRank

      case None =>
        for {
          admissibleDomains <- filterDomains(admissibleDomains)
          domainRank <- pickSynchronizerIdAndComputeReassignments(contracts, admissibleDomains)
        } yield domainRank
    }
  }

  /** Choose the appropriate domain for a transaction.
    * The domain is chosen as follows:
    * 1. Domain whose alias equals the workflow id
    * 2. Domain of all input contracts (fail if there is more than one)
    * 3. An arbitrary domain to which the submitter can submit and on which all informees have active participants
    */
  def forSingleDomain(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, DomainRank] =
    for {
      inputContractsSynchronizerIdO <- chooseDomainOfInputContracts

      synchronizerId <- transactionData.prescribedDomainO match {
        case Some(prescribedSynchronizerId) =>
          // If a domain is prescribed, we use the prescribed one
          singleDomainValidatePrescribedDomain(
            prescribedSynchronizerId,
            inputContractsSynchronizerIdO,
          )
            .map(_ => prescribedSynchronizerId)

        case None =>
          inputContractsSynchronizerIdO match {
            case Some(inputContractsSynchronizerId) =>
              // If all the contracts are on a single domain, we use this one
              singleDomainValidatePrescribedDomain(
                inputContractsSynchronizerId,
                inputContractsSynchronizerIdO,
              )
                .map(_ => inputContractsSynchronizerId)
            // TODO(#10088) If validation fails, try to re-submit as multi-domain

            case None =>
              // Pick the best valid domain in domainsOfSubmittersAndInformees
              filterDomains(admissibleDomains)
                .map(_.minBy1(id => DomainRank(Map.empty, priorityOfSynchronizer(id), id)))
          }
      }
    } yield DomainRank(Map.empty, priorityOfSynchronizer(synchronizerId), synchronizerId)

  private def filterDomains(
      admissibleDomains: NonEmpty[Set[SynchronizerId]]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[Set[SynchronizerId]]] = {

    val (unableToFetchStateDomains, domainStates) = admissibleDomains.forgetNE.toList.map {
      synchronizerId =>
        domainStateProvider.getTopologySnapshotAndPVFor(synchronizerId).map {
          case (snapshot, protocolVersion) =>
            (synchronizerId, protocolVersion, snapshot)
        }
    }.separate

    for {
      domains <- EitherT.right(
        UsableDomains.check(
          domains = domainStates,
          transaction = transactionData.transaction,
          ledgerTime = transactionData.ledgerTime,
        )
      )

      (unusableDomains, usableDomains) = domains
      allUnusableDomains =
        unusableDomains.map(d => d.synchronizerId -> d.toString).toMap ++
          unableToFetchStateDomains.map(d => d.synchronizerId -> d.toString).toMap

      _ = logger.debug(s"Not considering the following domains for routing: $allUnusableDomains")

      usableDomainsNE <- EitherT
        .pure[FutureUnlessShutdown, TransactionRoutingError](usableDomains)
        .map(NonEmpty.from)
        .subflatMap(
          _.toRight[TransactionRoutingError](NoDomainForSubmission.Error(allUnusableDomains))
        )

      _ = logger.debug(s"Candidates for submission: $usableDomainsNE")
    } yield usableDomainsNE.toSet
  }

  private def singleDomainValidatePrescribedDomain(
      synchronizerId: SynchronizerId,
      inputContractsSynchronizerIdO: Option[SynchronizerId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] = {
    /*
      If there are input contracts, then they should be on domain `synchronizerId`
     */
    def validateContainsInputContractsSynchronizerId
        : EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] =
      inputContractsSynchronizerIdO match {
        case Some(inputContractsSynchronizerId) =>
          EitherTUtil.condUnitET(
            inputContractsSynchronizerId == synchronizerId,
            TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
              .InputContractsNotOnDomain(synchronizerId, inputContractsSynchronizerId),
          )

        case None => EitherT.pure(())
      }

    for {
      // Single-domain specific validations
      _ <- validateContainsInputContractsSynchronizerId

      // Generic validations
      _ <- validatePrescribedDomain(synchronizerId)
    } yield ()
  }

  /** Validation that are shared between single- and multi- domain submission:
    *
    * - Participant is connected to `synchronizerId`
    *
    * - List `domainsOfSubmittersAndInformees` contains `synchronizerId`
    */
  private def validatePrescribedDomain(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] =
    for {
      domainState <- EitherT.fromEither[FutureUnlessShutdown](
        domainStateProvider.getTopologySnapshotAndPVFor(synchronizerId)
      )
      (snapshot, protocolVersion) = domainState

      // Informees and submitters should reside on the selected domain
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        admissibleDomains.contains(synchronizerId),
        TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
          .NotAllInformeeAreOnDomain(
            synchronizerId,
            admissibleDomains,
          ),
      )

      // Further validations
      _ <- UsableDomains
        .check(
          synchronizerId = synchronizerId,
          protocolVersion = protocolVersion,
          snapshot = snapshot,
          transaction = transactionData.transaction,
          ledgerTime = transactionData.ledgerTime,
          interactiveSubmissionVersionO = transactionData.externallySignedSubmissionO.map(_.version),
        )
        .leftMap[TransactionRoutingError] { err =>
          TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
            .Generic(synchronizerId, err.toString)
        }

    } yield ()

  private def pickSynchronizerIdAndComputeReassignments(
      contracts: Seq[ContractData],
      domains: NonEmpty[Set[SynchronizerId]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, DomainRank] = {
    val rankedDomainOpt = FutureUnlessShutdown.outcomeF {
      for {
        rankedDomains <- domains.forgetNE.toList
          .parTraverseFilter(targetDomain =>
            domainRankComputation
              .compute(
                contracts,
                Target(targetDomain),
                transactionData.readers,
              )
              .toOption
              .value
          )
        // Priority of domain
        // Number of reassignments if we use this domain
        // pick according to the least amount of reassignments
      } yield rankedDomains.minOption
        .toRight(
          TransactionRoutingError.AutomaticReassignmentForTransactionFailure.Failed(
            s"None of the following $domains is suitable for automatic reassignment."
          )
        )
    }
    EitherT(rankedDomainOpt)
  }

  private def chooseDomainOfInputContracts
      : EitherT[FutureUnlessShutdown, TransactionRoutingError, Option[SynchronizerId]] = {
    val inputContractsDomainData = transactionData.inputContractsDomainData

    inputContractsDomainData.domains.size match {
      case 0 | 1 => EitherT.rightT(inputContractsDomainData.domains.headOption)
      // Input contracts reside on different domains
      // Fail..
      case _ =>
        EitherT.leftT[FutureUnlessShutdown, Option[SynchronizerId]](
          RoutingInternalError
            .InputContractsOnDifferentDomains(inputContractsDomainData.domains)
        )
    }
  }
}
