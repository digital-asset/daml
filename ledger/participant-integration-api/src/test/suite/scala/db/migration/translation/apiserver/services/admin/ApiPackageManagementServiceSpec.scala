// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.zip.ZipInputStream
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.domain.LedgerOffset.Absolute
import com.daml.ledger.api.domain.PackageEntry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.admin.package_management_service.{
  PackageManagementServiceGrpc,
  UploadDarFileRequest,
}
import com.daml.ledger.participant.state.index.v2.{IndexPackagesService, IndexTransactionsService}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.archive.testing.Encode
import com.daml.lf.archive.{Dar, GenDarReader}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast.Expr
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.testing.parser.Implicits.defaultParserParameters
import com.daml.logging.LoggingContext
import com.daml.telemetry.TelemetrySpecBase._
import com.daml.telemetry.{TelemetryContext, TelemetrySpecBase}
import com.google.protobuf.ByteString
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration.Duration

class ApiPackageManagementServiceSpec
    extends AsyncWordSpec
    with TelemetrySpecBase
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with AkkaBeforeAndAfterAll {

  import ApiPackageManagementServiceSpec._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "ApiPackageManagementService $suffix" should {
    "propagate trace context" in {
      val apiService = createApiService()

      val span = anEmptySpan()
      val scope = span.makeCurrent()
      apiService
        .uploadDarFile(UploadDarFileRequest(ByteString.EMPTY, aSubmissionId))
        .andThen { case _ =>
          scope.close()
          span.end()
        }
        .map { _ =>
          spanExporter.finishedSpanAttributes should contain(anApplicationIdSpanAttribute)
          succeed
        }
    }
  }

  private def createApiService(): PackageManagementServiceGrpc.PackageManagementService = {
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
    when(mockIndexPackagesService.packageEntries(any[Option[Absolute]])(any[LoggingContext]))
      .thenReturn(
        Source.single(
          PackageEntry.PackageUploadAccepted(aSubmissionId, Timestamp.Epoch)
        )
      )

    ApiPackageManagementService.createApiService(
      mockIndexPackagesService,
      mockIndexTransactionsService,
      TestWritePackagesService,
      Duration.Zero,
      mockEngine,
      mockDarReader,
      _ => Ref.SubmissionId.assertFromString("aSubmission"),
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
      Some(
        Ast.PackageMetadata(
          Ref.PackageName.assertFromString("aPackage"),
          Ref.PackageVersion.assertFromString("0.0.0"),
        )
      ),
    )
    Encode.encodeArchive(
      defaultParserParameters.defaultPackageId -> pkg,
      defaultParserParameters.languageVersion,
    )
  }

  private object TestWritePackagesService extends state.WritePackagesService {
    override def uploadPackages(
        submissionId: Ref.SubmissionId,
        archives: List[DamlLf.Archive],
        sourceDescription: Option[String],
    )(implicit
        loggingContext: LoggingContext,
        telemetryContext: TelemetryContext,
    ): CompletionStage[state.SubmissionResult] = {
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      CompletableFuture.completedFuture(state.SubmissionResult.Acknowledged)
    }
  }
}
