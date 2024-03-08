// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.daml_lf_dev.DamlLf
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.ErrorsAssertions
import com.daml.error.utils.ErrorDetails
import com.daml.error.utils.ErrorDetails.RetryInfoDetail
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v2.admin.package_management_service.{
  PackageManagementServiceGrpc,
  UploadDarFileRequest,
}
import com.daml.lf.archive.testing.Encode
import com.daml.lf.archive.{Dar, GenDarReader}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast.Expr
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.tracing.TelemetrySpecBase.*
import com.daml.tracing.{DefaultOpenTelemetry, NoOpTelemetry}
import com.digitalasset.canton.ledger.api.domain.PackageEntry
import com.digitalasset.canton.ledger.api.domain.ParticipantOffset.Absolute
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  IndexPackagesService,
  IndexTransactionsService,
}
import com.digitalasset.canton.ledger.participant.state.v2.SubmissionResult
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.{LoggingContextWithTrace, SuppressionRule}
import com.digitalasset.canton.platform.store.packagemeta.{
  PackageMetadata,
  PackageMetadataSnapshot,
  PackageMetadataStore,
}
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext}
import com.digitalasset.canton.{BaseTest, DiscardOps}
import com.google.protobuf.ByteString
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import org.apache.pekko.stream.scaladsl.Source
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level.DEBUG

import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.zip.ZipInputStream
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

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
        .uploadDarFile(UploadDarFileRequest(ByteString.EMPTY, aSubmissionId, false))
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
            .uploadDarFile(UploadDarFileRequest(ByteString.EMPTY, aSubmissionId, false))
            .map(_ => succeed)
        },
        { logEntries =>
          logEntries should not be empty

          val mdcs = logEntries.map(_.mdc)
          forEvery(mdcs) { _.getOrElse("trace-id", "") should not be empty }
        },
      )
    }

    "close while uploading dar" in {
      val writeService = mock[state.WritePackagesService]
      when(
        writeService.uploadPackages(any[Ref.SubmissionId], any[List[Archive]], any[Option[String]])(
          any[TraceContext]
        )
      ).thenReturn(CompletableFuture.completedFuture(SubmissionResult.Acknowledged))

      val (
        mockDarReader,
        mockEngine,
        mockIndexTransactionsService,
        mockIndexPackagesService,
        mockPackageMetadataStore,
      ) =
        mockedServices()
      val promise = Promise[Unit]()

      when(
        mockIndexPackagesService.packageEntries(any[Option[Absolute]])(any[LoggingContextWithTrace])
      )
        .thenReturn(
          {
            promise.success(())
            Source.never
          }
        )

      val apiPackageManagementService = ApiPackageManagementService.createApiService(
        mockIndexPackagesService,
        mockIndexTransactionsService,
        mockPackageMetadataStore,
        writeService,
        Duration.Zero,
        mockEngine,
        mockDarReader,
        _ => Ref.SubmissionId.assertFromString("aSubmission"),
        telemetry = NoOpTelemetry,
        loggerFactory = loggerFactory,
        disableUpgradeValidation = false,
      )

      promise.future.map(_ => apiPackageManagementService.close()).discard

      apiPackageManagementService
        .uploadDarFile(
          UploadDarFileRequest(ByteString.EMPTY, aSubmissionId, false)
        )
        .transform {
          case Success(_) =>
            fail("Expected a failure, but received success")
          case Failure(err: StatusRuntimeException) =>
            assertError(
              actual = err,
              expectedStatusCode = Code.UNAVAILABLE,
              expectedMessage = "SERVER_IS_SHUTTING_DOWN(1,0): Server is shutting down",
              expectedDetails = List(
                ErrorDetails.ErrorInfoDetail(
                  "SERVER_IS_SHUTTING_DOWN",
                  Map(
                    "submissionId" -> s"'$aSubmissionId'",
                    "category" -> "1",
                    "definite_answer" -> "false",
                    "test" -> s"'${getClass.getSimpleName}'",
                  ),
                ),
                RetryInfoDetail(1.second),
              ),
              verifyEmptyStackTrace = true,
            )
            Success(succeed)
          case Failure(other) =>
            fail("Unexpected error", other)
        }
    }

  }

  private def mockedServices(): (
      GenDarReader[Archive],
      Engine,
      IndexTransactionsService,
      IndexPackagesService,
      PackageMetadataStore,
  ) = {
    val mockDarReader = mock[GenDarReader[Archive]]
    when(mockDarReader.readArchive(any[String], any[ZipInputStream], any[Int]))
      .thenReturn(Right(new Dar[Archive](anArchive, List.empty)))

    val mockEngine = mock[Engine]
    when(
      mockEngine.validatePackages(any[Map[PackageId, Ast.Package]])
    ).thenReturn(Right(()))

    val mockIndexTransactionsService = mock[IndexTransactionsService]
    when(mockIndexTransactionsService.currentLedgerEnd())
      .thenReturn(Future.successful(Absolute(Ref.LedgerString.assertFromString("0"))))

    val mockIndexPackagesService = mock[IndexPackagesService]
    when(
      mockIndexPackagesService.packageEntries(any[Option[Absolute]])(any[LoggingContextWithTrace])
    )
      .thenReturn(
        Source.single(
          PackageEntry.PackageUploadAccepted(aSubmissionId, Timestamp.Epoch)
        )
      )

    val mockPackageMetadataStore = mock[PackageMetadataStore]
    when(mockPackageMetadataStore.getSnapshot).thenReturn(
      new PackageMetadataSnapshot(PackageMetadata())
    )

    (
      mockDarReader,
      mockEngine,
      mockIndexTransactionsService,
      mockIndexPackagesService,
      mockPackageMetadataStore,
    )
  }

  private def createApiService(): PackageManagementServiceGrpc.PackageManagementService = {
    val (
      mockDarReader,
      mockEngine,
      mockIndexTransactionsService,
      mockIndexPackagesService,
      mockPackageMetadataStore,
    ) =
      mockedServices()

    ApiPackageManagementService.createApiService(
      mockIndexPackagesService,
      mockIndexTransactionsService,
      mockPackageMetadataStore,
      TestWritePackagesService(testTelemetrySetup.tracer),
      Duration.Zero,
      mockEngine,
      mockDarReader,
      _ => Ref.SubmissionId.assertFromString("aSubmission"),
      telemetry = new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build()),
      loggerFactory = loggerFactory,
      disableUpgradeValidation = false,
    )
  }
}

object ApiPackageManagementServiceSpec {
  private val aSubmissionId = "aSubmission"

  private val anArchive: Archive = {
    val pkg = Ast.GenPackage[Expr](
      Map.empty,
      Set.empty,
      LanguageVersion.default,
      Ast.PackageMetadata(
        Ref.PackageName.assertFromString("aPackage"),
        Ref.PackageVersion.assertFromString("0.0.0"),
        None,
      ),
    )
    Encode.encodeArchive(
      Ref.PackageId.assertFromString("-pkgId-") -> pkg,
      LanguageVersion.default,
    )
  }

  private final case class TestWritePackagesService(tracer: Tracer)
      extends state.WritePackagesService {
    override def uploadPackages(
        submissionId: Ref.SubmissionId,
        archives: List[DamlLf.Archive],
        sourceDescription: Option[String],
    )(implicit
        traceContext: TraceContext
    ): CompletionStage[state.SubmissionResult] = {
      val telemetryContext = traceContext.toDamlTelemetryContext(tracer)
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      CompletableFuture.completedFuture(state.SubmissionResult.Acknowledged)
    }
  }
}
