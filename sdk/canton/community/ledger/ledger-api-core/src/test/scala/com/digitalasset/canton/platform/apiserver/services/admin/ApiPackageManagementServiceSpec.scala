// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import cats.data.EitherT
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v2.admin.package_management_service.{
  PackageManagementServiceGrpc,
  UploadDarFileRequest,
  ValidateDarFileRequest,
  ValidateDarFileResponse,
}
import com.daml.nonempty.NonEmpty
import com.daml.tracing.DefaultOpenTelemetry
import com.daml.tracing.TelemetrySpecBase.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.error.{TransactionError, TransactionRoutingError}
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.api.{
  EnrichedVettedPackages,
  ListVettedPackagesOpts,
  UpdateVettedPackagesOpts,
  UploadDarVettingChange,
}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.SyncService.SubmissionCostEstimation
import com.digitalasset.canton.ledger.participant.state.{
  InternalIndexService,
  PruningResult,
  ReassignmentCommand,
  RoutingSynchronizerState,
  SubmissionResult,
  SubmitterInfo,
  SynchronizerRank,
  TransactionMeta,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.platform.apiserver.services.command.interactive.CostEstimationHints
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfFatContractInst,
  LfSubmittedTransaction,
  LfVersionedTransaction,
}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  ExternalPartyOnboardingDetails,
  ParticipantId,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LfKeyResolver, LfPackageId, LfPartyId}
import com.digitalasset.daml.lf.data.Ref.{CommandId, Party, SubmissionId, UserId, WorkflowId}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.SubmittedTransaction
import com.google.protobuf.ByteString
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level.DEBUG

import java.util.concurrent.CompletionStage
import scala.concurrent.Future

// TODO(#17635) Very thin layer. Revisit utility of testing
class ApiPackageManagementServiceSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with PekkoBeforeAndAfterAll
    with Eventually
    with ErrorsAssertions
    with BaseTest
    with BeforeAndAfterEach {

  import ApiPackageManagementServiceSpec.*

  var testTelemetrySetup: TestTelemetrySetup = _

  override def beforeEach(): Unit =
    testTelemetrySetup = new TestTelemetrySetup()

  override def afterEach(): Unit =
    testTelemetrySetup.close()

  "ApiPackageManagementService $suffix" should {
    "propagate trace context" in {
      val apiService = createApiService()
      val span = testTelemetrySetup.anEmptySpan()
      val scope = span.makeCurrent()
      apiService
        .uploadDarFile(
          UploadDarFileRequest(
            ByteString.EMPTY,
            aSubmissionId,
            UploadDarFileRequest.VettingChange.VETTING_CHANGE_VET_ALL_PACKAGES,
            synchronizerId = "",
          )
        )
        .thereafter { _ =>
          scope.close()
          span.end()
        }
        .map { _ =>
          testTelemetrySetup.reportedSpanAttributes should contain(anUserIdSpanAttribute)
          succeed
        }
    }

    "have a tid" in {
      val apiService = createApiService()
      val span = testTelemetrySetup.anEmptySpan()
      val _ = span.makeCurrent()

      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(DEBUG))(
        within = {
          apiService
            .uploadDarFile(
              UploadDarFileRequest(
                ByteString.EMPTY,
                aSubmissionId,
                UploadDarFileRequest.VettingChange.VETTING_CHANGE_VET_ALL_PACKAGES,
                synchronizerId = "",
              )
            )
            .map(_ => succeed)
        },
        { logEntries =>
          logEntries should not be empty

          val mdcs = logEntries.map(_.mdc)
          forEvery(mdcs)(_.getOrElse("trace-id", "") should not be empty)
        },
      )
    }

    "validate a dar" in {
      val apiService = createApiService()
      apiService
        .validateDarFile(ValidateDarFileRequest(ByteString.EMPTY, aSubmissionId, ""))
        .map { case ValidateDarFileResponse() => succeed }
    }
  }

  private def createApiService(): PackageManagementServiceGrpc.PackageManagementService =
    ApiPackageManagementService.createApiService(
      TestSyncService(testTelemetrySetup.tracer),
      telemetry = new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build()),
      loggerFactory = loggerFactory,
    )
}

object ApiPackageManagementServiceSpec {
  private val aSubmissionId = "aSubmission"

  private final case class TestSyncService(tracer: Tracer) extends state.SyncService {
    override def uploadDar(
        dar: Seq[ByteString],
        submissionId: Ref.SubmissionId,
        vettingChange: UploadDarVettingChange,
        synchronizerId: Option[SynchronizerId],
    )(implicit
        traceContext: TraceContext
    ): Future[SubmissionResult] = {
      val telemetryContext = traceContext.toDamlTelemetryContext(tracer)
      telemetryContext.setAttribute(
        anUserIdSpanAttribute._1,
        anUserIdSpanAttribute._2,
      )
      Future.successful(state.SubmissionResult.Acknowledged)
    }

    override def validateDar(
        dar: ByteString,
        darName: String,
        synchronizerId: Option[SynchronizerId],
    )(implicit
        traceContext: TraceContext
    ): Future[SubmissionResult] = {
      val telemetryContext = traceContext.toDamlTelemetryContext(tracer)
      telemetryContext.setAttribute(
        anUserIdSpanAttribute._1,
        anUserIdSpanAttribute._2,
      )
      Future.successful(state.SubmissionResult.Acknowledged)
    }

    override def internalIndexService: Option[InternalIndexService] =
      throw new UnsupportedOperationException()

    override def registerInternalIndexService(internalIndexService: InternalIndexService): Unit =
      throw new UnsupportedOperationException()

    override def unregisterInternalIndexService(): Unit =
      throw new UnsupportedOperationException()

    override def currentHealth(): HealthStatus =
      throw new UnsupportedOperationException()

    override def hashOps: HashOps = throw new UnsupportedOperationException()

    override def submitTransaction(
        transaction: SubmittedTransaction,
        synchronizerRank: SynchronizerRank,
        routingSynchronizerState: RoutingSynchronizerState,
        submitterInfo: SubmitterInfo,
        transactionMeta: TransactionMeta,
        // Currently, the estimated interpretation cost is not used
        _estimatedInterpretationCost: Long,
        keyResolver: LfKeyResolver,
        processedDisclosedContracts: ImmArray[LfFatContractInst],
    )(implicit
        traceContext: TraceContext
    ): CompletionStage[SubmissionResult] =
      throw new UnsupportedOperationException()

    override def submitReassignment(
        submitter: Party,
        userId: UserId,
        commandId: CommandId,
        submissionId: Option[SubmissionId],
        workflowId: Option[WorkflowId],
        reassignmentCommands: Seq[ReassignmentCommand],
    )(implicit traceContext: TraceContext): CompletionStage[SubmissionResult] =
      throw new UnsupportedOperationException()

    override def allocateParty(
        hint: Party,
        submissionId: SubmissionId,
        synchronizerIdO: Option[SynchronizerId],
        externalPartyOnboardingDetails: Option[ExternalPartyOnboardingDetails],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[SubmissionResult] =
      throw new UnsupportedOperationException()

    override def prune(
        pruneUpToInclusive: Offset,
        submissionId: SubmissionId,
    ): CompletionStage[PruningResult] =
      throw new UnsupportedOperationException()

    override def computePartyVettingMap(
        submitters: Set[LfPartyId],
        informees: Set[LfPartyId],
        vettingValidityTimestamp: CantonTimestamp,
        prescribedSynchronizer: Option[SynchronizerId],
        routingSynchronizerState: RoutingSynchronizerState,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[PhysicalSynchronizerId, Map[LfPartyId, Set[LfPackageId]]]] =
      throw new UnsupportedOperationException()

    override def computeHighestRankedSynchronizerFromAdmissible(
        submitterInfo: SubmitterInfo,
        transaction: LfSubmittedTransaction,
        transactionMeta: TransactionMeta,
        admissibleSynchronizers: NonEmpty[Set[PhysicalSynchronizerId]],
        disclosedContractIds: List[LfContractId],
        routingSynchronizerState: RoutingSynchronizerState,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, PhysicalSynchronizerId] =
      throw new UnsupportedOperationException()

    override def selectRoutingSynchronizer(
        submitterInfo: SubmitterInfo,
        transaction: LfSubmittedTransaction,
        transactionMeta: TransactionMeta,
        disclosedContractIds: List[LfContractId],
        optSynchronizerId: Option[SynchronizerId],
        transactionUsedForExternalSigning: Boolean,
        routingSynchronizerState: RoutingSynchronizerState,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TransactionError, SynchronizerRank] =
      throw new UnsupportedOperationException()

    override def getRoutingSynchronizerState(implicit
        traceContext: TraceContext
    ): RoutingSynchronizerState =
      throw new UnsupportedOperationException()

    override def estimateTrafficCost(
        synchronizerId: SynchronizerId,
        transaction: LfVersionedTransaction,
        transactionMetadata: TransactionMeta,
        submitterInfo: SubmitterInfo,
        keyResolver: LfKeyResolver,
        disclosedContracts: Map[LfContractId, LfFatContractInst],
        costHints: CostEstimationHints,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, SubmissionCostEstimation] =
      throw new UnsupportedOperationException()

    override def listVettedPackages(
        opts: ListVettedPackagesOpts
    )(implicit
        traceContext: TraceContext
    ): Future[Seq[EnrichedVettedPackages]] =
      throw new UnsupportedOperationException()

    override def updateVettedPackages(
        opts: UpdateVettedPackagesOpts
    )(implicit
        traceContext: TraceContext
    ): Future[(Option[EnrichedVettedPackages], Option[EnrichedVettedPackages])] =
      throw new UnsupportedOperationException()

    override def protocolVersionForSynchronizerId(
        synchronizerId: SynchronizerId
    ): Option[ProtocolVersion] =
      throw new UnsupportedOperationException()

    override def participantId: ParticipantId = DefaultTestIdentities.participant1
  }
}
