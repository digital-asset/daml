// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.implicits.catsSyntaxParallelTraverse_
import cats.syntax.either.*
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.admin.data.{
  ContractImportMode,
  RepairContract,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.participant.admin.party.LapiAcsHelper
import com.digitalasset.canton.participant.admin.repair.RepairServiceError.ImportAcsError
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{MonadUtil, ResourceUtil}
import com.google.protobuf.ByteString
import org.apache.pekko.actor.ActorSystem

import java.io.OutputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[participant] object ParticipantCommon {

  private[grpc] def findLedgerEnd(sync: CantonSyncService): Either[String, Offset] =
    sync.participantNodePersistentState.value.ledgerApiStore.ledgerEndCache
      .apply()
      .map(_.lastOffset)
      .toRight("No ledger end found")

  /** Writes a snapshot of the Active Contract Set (ACS) for the given parties at a specific offset
    * to a destination stream. .
    *
    * @param indexService
    *   The service for querying active contracts.
    * @param parties
    *   The set of parties used to query the initial ACS.
    * @param atOffset
    *   The ledger offset for the snapshot.
    * @param destination
    *   The output stream for the serialized contracts.
    * @param excludedStakeholders
    *   Excludes any contract where a stakeholder is in this set.
    * @param synchronizerId
    *   Optionally filters contracts by this ID.
    * @param contractSynchronizerRenames
    *   A map to rename synchronizer IDs before writing.
    * @return
    *   A future that completes with `Right(())` on success, or a `Left` with an error message on
    *   failure.
    */
  private[grpc] def writeAcsSnapshot(
      indexService: InternalIndexService,
      parties: Set[PartyId],
      atOffset: Offset,
      destination: OutputStream,
      excludedStakeholders: Set[PartyId] = Set.empty,
      synchronizerId: Option[SynchronizerId] = None,
      contractSynchronizerRenames: Map[String, String] = Map.empty,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      actorSystem: ActorSystem,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _ <- EitherT
        .apply[Future, String, Unit](
          ResourceUtil.withResourceM(destination)(out =>
            LapiAcsHelper
              .ledgerApiAcsSource(
                indexService,
                parties,
                atOffset,
                excludedStakeholders,
                synchronizerId,
                contractSynchronizerRenames,
              )
              .map(_.writeDelimitedTo(out) match {
                // throwing intentionally to immediately interrupt any further Pekko source stream processing
                case Left(errorMessage) => throw new RuntimeException(errorMessage)
                case Right(_) => out.flush()
              })
              .run()
              .transform {
                case Failure(e) => Success(Left(e.getMessage)) // a Pekko stream error
                case Success(_) => Success(Right(()))
              }
          )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield ()

  private[grpc] def importAcsNewSnapshot(
      acsSnapshot: ByteString,
      batching: BatchingConfig,
      contractImportMode: ContractImportMode,
      excludedStakeholders: Set[PartyId],
      representativePackageIdOverride: RepresentativePackageIdOverride,
      sync: CantonSyncService,
      workflowIdPrefix: String,
  )(implicit
      ec: ExecutionContext,
      elc: ErrorLoggingContext,
      traceContext: TraceContext,
  ): Future[Unit] =
    new AcsImporter(
      sync,
      batching,
      workflowIdPrefix,
      contractImportMode,
      representativePackageIdOverride,
    ).runImport(acsSnapshot, excludedStakeholders)

  private final class AcsImporter(
      sync: CantonSyncService,
      batching: BatchingConfig,
      workflowIdPrefix: String,
      contractImportMode: ContractImportMode,
      representativePackageIdOverride: RepresentativePackageIdOverride,
  )(implicit
      ec: ExecutionContext,
      elc: ErrorLoggingContext,
      traceContext: TraceContext,
  ) {

    private val workflowIdPrefixO: Option[String] =
      Option.when(workflowIdPrefix.nonEmpty)(workflowIdPrefix)

    def runImport(
        acsSnapshot: ByteString,
        excludedStakeholders: Set[PartyId],
    ): Future[Unit] = {

      val contractsE = if (excludedStakeholders.isEmpty) {
        RepairContract.loadAcsSnapshot(acsSnapshot)
      } else {
        RepairContract
          .loadAcsSnapshot(acsSnapshot)
          .map(
            _.filter(_.contract.stakeholders.intersect(excludedStakeholders.map(_.toLf)).isEmpty)
          )
      }

      importAcsContracts(contractsE, contractImportMode)
    }

    private def importAcsContracts(
        contracts: Either[String, List[RepairContract]],
        contractImportMode: ContractImportMode,
    ): Future[Unit] = {
      val resultET = for {
        repairContracts <- contracts
          .toEitherT[FutureUnlessShutdown]
          .ensure( // TODO(#23073) - Remove this restriction once #27325 has been re-implemented
            "Found at least one contract with a non-zero reassignment counter. ACS import does not yet support it."
          )(_.forall(_.reassignmentCounter == ReassignmentCounter.Genesis))

        _ <- repairContracts.groupBy(_.synchronizerId).toSeq.parTraverse_ {
          case (synchronizerId, contracts) =>
            MonadUtil.batchedSequentialTraverse_(
              batching.parallelism,
              batching.maxAcsImportBatchSize,
            )(contracts)(
              writeContractsBatch(
                synchronizerId,
                _,
                contractImportMode,
                representativePackageIdOverride,
              )
                .mapK(FutureUnlessShutdown.outcomeK)
            )
        }
      } yield ()

      resultET.value.flatMap {
        case Left(error) => FutureUnlessShutdown.failed(ImportAcsError.Error(error).asGrpcError)
        case Right(()) => FutureUnlessShutdown.unit
      }.asGrpcFuture
    }

    private def writeContractsBatch(
        synchronizerId: SynchronizerId,
        contracts: Seq[RepairContract],
        contractImportMode: ContractImportMode,
        representativePackageIdOverride: RepresentativePackageIdOverride,
    ): EitherT[Future, String, Unit] =
      for {
        alias <- EitherT.fromEither[Future](
          sync.aliasManager
            .aliasForSynchronizerId(synchronizerId)
            .toRight(s"Not able to find synchronizer alias for ${synchronizerId.toString}")
        )

        _ <- EitherT.fromEither[Future](
          sync.repairService.addContracts(
            alias,
            contracts,
            ignoreAlreadyAdded = true,
            ignoreStakeholderCheck = true,
            contractImportMode,
            sync.getPackageMetadataSnapshot,
            representativePackageIdOverride,
            workflowIdPrefix = workflowIdPrefixO,
          )
        )
      } yield ()

  }

}
