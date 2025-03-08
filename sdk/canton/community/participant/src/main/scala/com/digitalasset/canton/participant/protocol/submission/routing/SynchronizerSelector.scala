// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.alternative.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyColl.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.error.TransactionRoutingError
import com.digitalasset.canton.ledger.participant.state.{RoutingSynchronizerState, SynchronizerRank}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.UsableSynchronizers
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

import TransactionRoutingError.RoutingInternalError
import TransactionRoutingError.TopologyErrors.NoSynchronizerForSubmission

private[routing] class SynchronizerSelectorFactory(
    admissibleSynchronizersComputation: AdmissibleSynchronizersComputation,
    priorityOfSynchronizer: SynchronizerId => Int,
    synchronizerRankComputation: SynchronizerRankComputation,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext) {

  def create(
      transactionData: TransactionData,
      synchronizerState: RoutingSynchronizerState,
      submitters: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerSelector] =
    for {
      admissibleSynchronizers <- admissibleSynchronizersComputation.forParties(
        submitters = submitters,
        informees = transactionData.informees,
        synchronizerState = synchronizerState,
      )
    } yield new SynchronizerSelector(
      transactionData,
      admissibleSynchronizers,
      priorityOfSynchronizer,
      synchronizerRankComputation,
      synchronizerState,
      loggerFactory,
    )
}

/** Selects the best synchronizer for routing.
  *
  * @param admissibleSynchronizers
  *   Synchronizers that host both submitters and informees of the transaction:
  *   - submitters have to be hosted on the local participant
  *   - informees have to be hosted on some participant It is assumed that the participant is
  *     connected to all synchronizers in `connectedSynchronizers`
  * @param priorityOfSynchronizer
  *   Priority of each synchronizer (lowest number indicates highest priority)
  * @param synchronizerRankComputation
  *   Utility class to compute `SynchronizerRank`
  * @param synchronizerState
  *   Provides state information about a synchronizer. Note: returns an either rather than an option
  *   since failure comes from disconnected synchronizers and we assume the participant to be
  *   connected to all synchronizers in `connectedSynchronizers`
  */
private[routing] class SynchronizerSelector(
    val transactionData: TransactionData,
    admissibleSynchronizers: NonEmpty[Set[SynchronizerId]],
    priorityOfSynchronizer: SynchronizerId => Int,
    synchronizerRankComputation: SynchronizerRankComputation,
    val synchronizerState: RoutingSynchronizerState,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Choose the appropriate synchronizer for a transaction. The synchronizer is chosen as follows:
    *   1. synchronizer whose id equals `transactionData.prescribedSynchronizerO` (if non-empty)
    *
    *   1. The synchronizer with the smaller number of reassignments on which all informees have
    *      active participants
    */
  def forMultiSynchronizer(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] = {
    val contracts = transactionData.inputContractsSynchronizerData.contractsData

    transactionData.prescribedSynchronizerIdO match {
      case Some(prescribedSynchronizer) =>
        for {
          _ <- validatePrescribedSynchronizer(prescribedSynchronizer)
          synchronizerRank <- synchronizerRankComputation
            .compute(
              contracts,
              Target(prescribedSynchronizer),
              transactionData.readers,
              synchronizerState,
            )
        } yield synchronizerRank

      case None =>
        for {
          admissibleSynchronizers <- filterSynchronizers(admissibleSynchronizers)
          synchronizerRank <- synchronizerRankComputation.computeBestSynchronizerRank(
            synchronizerState = synchronizerState,
            contracts = contracts,
            readers = transactionData.readers,
            synchronizerIds = admissibleSynchronizers,
          )
        } yield synchronizerRank
    }
  }

  /** Choose the appropriate synchronizer for a transaction. The synchronizer is chosen as follows:
    *   1. synchronizer whose alias equals the workflow id
    *   1. synchronizer of all input contracts (fail if there is more than one)
    *   1. An arbitrary synchronizer to which the submitter can submit and on which all informees
    *      have active participants
    */
  def forSingleSynchronizer(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] =
    for {
      inputContractsSynchronizerIdO <- getSynchronizerOfInputContracts

      synchronizerId <- transactionData.prescribedSynchronizerIdO match {
        case Some(prescribedSynchronizerId) =>
          // If a synchronizer is prescribed, we use the prescribed one
          singleSynchronizerValidatePrescribedSynchronizer(
            prescribedSynchronizerId,
            inputContractsSynchronizerIdO,
          )
            .map(_ => prescribedSynchronizerId)

        case None =>
          inputContractsSynchronizerIdO match {
            case Some(inputContractsSynchronizerId) =>
              // If all the contracts are on a single synchronizer, we use this one
              singleSynchronizerValidatePrescribedSynchronizer(
                inputContractsSynchronizerId,
                inputContractsSynchronizerIdO,
              )
                .map(_ => inputContractsSynchronizerId)
            // TODO(#10088) If validation fails, try to re-submit as multi-synchronizer

            case None =>
              // Pick the best valid synchronizer in synchronizersOfSubmittersAndInformees
              filterSynchronizers(admissibleSynchronizers)
                .map(_.minBy1(id => SynchronizerRank(Map.empty, priorityOfSynchronizer(id), id)))
          }
      }
    } yield SynchronizerRank(Map.empty, priorityOfSynchronizer(synchronizerId), synchronizerId)

  private def filterSynchronizers(
      admissibleSynchronizers: NonEmpty[Set[SynchronizerId]]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[Set[SynchronizerId]]] = {

    val (unableToFetchStateSynchronizers, synchronizerStates) =
      admissibleSynchronizers.forgetNE.toList.map { synchronizerId =>
        synchronizerState.getTopologySnapshotAndPVFor(synchronizerId).map {
          case (snapshot, protocolVersion) =>
            (synchronizerId, protocolVersion, snapshot)
        }
      }.separate

    for {
      synchronizers <- EitherT.right[TransactionRoutingError](
        UsableSynchronizers.check(
          synchronizers = synchronizerStates,
          transaction = transactionData.transaction,
          ledgerTime = transactionData.ledgerTime,
        )
      )

      (unusableSynchronizers, usableSynchronizers) = synchronizers
      allUnusableSynchronizers =
        unusableSynchronizers.map(d => d.synchronizerId -> d.toString).toMap ++
          unableToFetchStateSynchronizers.map(d => d.synchronizerId -> d.toString).toMap

      _ = logger.debug(
        s"Not considering the following synchronizers for routing: $allUnusableSynchronizers"
      )

      usableSynchronizersNE <- EitherT
        .pure[FutureUnlessShutdown, TransactionRoutingError](usableSynchronizers)
        .map(NonEmpty.from)
        .subflatMap(
          _.toRight[TransactionRoutingError](
            NoSynchronizerForSubmission.Error(allUnusableSynchronizers)
          )
        )

      _ = logger.debug(s"Candidates for submission: $usableSynchronizersNE")
    } yield usableSynchronizersNE.toSet
  }

  private def singleSynchronizerValidatePrescribedSynchronizer(
      synchronizerId: SynchronizerId,
      inputContractsSynchronizerIdO: Option[SynchronizerId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] = {
    /*
      If there are input contracts, then they should be on synchronizer `synchronizerId`
     */
    def validateContainsInputContractsSynchronizerId
        : EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] =
      inputContractsSynchronizerIdO match {
        case Some(inputContractsSynchronizerId) =>
          EitherTUtil.condUnitET(
            inputContractsSynchronizerId == synchronizerId,
            TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
              .InputContractsNotOnSynchronizer(synchronizerId, inputContractsSynchronizerId),
          )

        case None => EitherT.pure(())
      }

    for {
      // Single-synchronizer specific validations
      _ <- validateContainsInputContractsSynchronizerId

      // Generic validations
      _ <- validatePrescribedSynchronizer(synchronizerId)
    } yield ()
  }

  /** Validation that are shared between single- and multi- synchronizer submission:
    *
    *   - Participant is connected to `synchronizerId`
    *
    *   - List `synchronizersOfSubmittersAndInformees` contains `synchronizerId`
    */
  private def validatePrescribedSynchronizer(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] =
    for {
      synchronizerState <- EitherT.fromEither[FutureUnlessShutdown](
        synchronizerState.getTopologySnapshotAndPVFor(synchronizerId)
      )
      (snapshot, protocolVersion) = synchronizerState

      // Informees and submitters should reside on the selected synchronizer
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        admissibleSynchronizers.contains(synchronizerId),
        TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
          .NotAllInformeeAreOnSynchronizer(
            synchronizerId,
            admissibleSynchronizers,
          ),
      )

      // Further validations
      _ <- UsableSynchronizers
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

  private def getSynchronizerOfInputContracts
      : EitherT[FutureUnlessShutdown, TransactionRoutingError, Option[SynchronizerId]] = {
    val inputContractsSynchronizerData = transactionData.inputContractsSynchronizerData

    inputContractsSynchronizerData.synchronizers.size match {
      case 0 | 1 => EitherT.rightT(inputContractsSynchronizerData.synchronizers.headOption)
      // Input contracts reside on different synchronizers
      // Fail..
      case _ =>
        EitherT.leftT[FutureUnlessShutdown, Option[SynchronizerId]](
          RoutingInternalError
            .InputContractsOnDifferentSynchronizers(inputContractsSynchronizerData.synchronizers)
        )
    }
  }
}
