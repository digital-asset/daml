// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.error.ErrorsAssertions
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v2.admin.package_management_service.{
  PackageManagementServiceGrpc,
  UploadDarFileRequest,
  ValidateDarFileRequest,
  ValidateDarFileResponse,
}
import com.daml.tracing.DefaultOpenTelemetry
import com.daml.tracing.TelemetrySpecBase.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.{Offset, ProcessedDisclosedContract}
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.{
  InternalStateService,
  PruningResult,
  ReassignmentCommand,
  SubmissionResult,
  SubmitterInfo,
  TransactionMeta,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext}
import com.digitalasset.daml.lf.data.Ref.{ApplicationId, CommandId, Party, SubmissionId, WorkflowId}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.{GlobalKey, SubmittedTransaction}
import com.digitalasset.daml.lf.value.Value
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

  override def beforeEach(): Unit = {
    testTelemetrySetup = new TestTelemetrySetup()
  }

  override def afterEach(): Unit = {
    testTelemetrySetup.close()
  }

  "ApiPackageManagementService $suffix" should {
    "propagate trace context" in {
      val apiService = createApiService()
      val span = testTelemetrySetup.anEmptySpan()
      val scope = span.makeCurrent()
      apiService
        .uploadDarFile(UploadDarFileRequest(ByteString.EMPTY, aSubmissionId))
        .andThen { case _ =>
          scope.close()
          span.end()
        }
        .map { _ =>
          testTelemetrySetup.reportedSpanAttributes should contain(anApplicationIdSpanAttribute)
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
            .uploadDarFile(UploadDarFileRequest(ByteString.EMPTY, aSubmissionId))
            .map(_ => succeed)
        },
        { logEntries =>
          logEntries should not be empty

          val mdcs = logEntries.map(_.mdc)
          forEvery(mdcs) { _.getOrElse("trace-id", "") should not be empty }
        },
      )
    }

    "validate a dar" in {
      val apiService = createApiService()
      apiService
        .validateDarFile(ValidateDarFileRequest(ByteString.EMPTY, aSubmissionId))
        .map { case ValidateDarFileResponse() => succeed }
    }
  }

  private def createApiService(): PackageManagementServiceGrpc.PackageManagementService = {
    ApiPackageManagementService.createApiService(
      TestWriteService(testTelemetrySetup.tracer),
      telemetry = new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build()),
      loggerFactory = loggerFactory,
    )
  }
}

object ApiPackageManagementServiceSpec {
  private val aSubmissionId = "aSubmission"

  private final case class TestWriteService(tracer: Tracer) extends state.WriteService {
    override def uploadDar(
        dar: ByteString,
        submissionId: Ref.SubmissionId,
    )(implicit
        traceContext: TraceContext
    ): Future[SubmissionResult] = {
      val telemetryContext = traceContext.toDamlTelemetryContext(tracer)
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      Future.successful(state.SubmissionResult.Acknowledged)
    }

    override def validateDar(dar: ByteString, darName: String)(implicit
        traceContext: TraceContext
    ): Future[SubmissionResult] = {
      val telemetryContext = traceContext.toDamlTelemetryContext(tracer)
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      Future.successful(state.SubmissionResult.Acknowledged)
    }

    override def internalStateService: Option[InternalStateService] =
      throw new UnsupportedOperationException()

    override def registerInternalStateService(internalStateService: InternalStateService): Unit =
      throw new UnsupportedOperationException()

    override def unregisterInternalStateService(): Unit =
      throw new UnsupportedOperationException()

    override def currentHealth(): HealthStatus =
      throw new UnsupportedOperationException()

    override def submitTransaction(
        submitterInfo: SubmitterInfo,
        optDomainId: Option[DomainId],
        transactionMeta: TransactionMeta,
        transaction: SubmittedTransaction,
        estimatedInterpretationCost: Long,
        globalKeyMapping: Map[GlobalKey, Option[Value.ContractId]],
        processedDisclosedContracts: ImmArray[ProcessedDisclosedContract],
    )(implicit traceContext: TraceContext): CompletionStage[SubmissionResult] =
      throw new UnsupportedOperationException()

    override def submitReassignment(
        submitter: Party,
        applicationId: ApplicationId,
        commandId: CommandId,
        submissionId: Option[SubmissionId],
        workflowId: Option[WorkflowId],
        reassignmentCommand: ReassignmentCommand,
    )(implicit traceContext: TraceContext): CompletionStage[SubmissionResult] =
      throw new UnsupportedOperationException()

    override def allocateParty(
        hint: Option[Party],
        displayName: Option[String],
        submissionId: SubmissionId,
    )(implicit traceContext: TraceContext): CompletionStage[SubmissionResult] =
      throw new UnsupportedOperationException()

    override def prune(
        pruneUpToInclusive: Offset,
        submissionId: SubmissionId,
        pruneAllDivulgedContracts: Boolean,
    ): CompletionStage[PruningResult] =
      throw new UnsupportedOperationException()
  }
}
