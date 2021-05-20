// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.time.{Duration, Instant}
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
import com.daml.ledger.participant.state.v1.{SubmissionId, SubmissionResult, WritePackagesService}
import com.daml.lf.archive.testing.Encode
import com.daml.lf.archive.{Dar, DarReader}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast.Expr
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.testing.parser.Implicits.defaultParserParameters
import com.daml.logging.LoggingContext
import com.daml.telemetry.{TelemetryContext, TelemetrySpecBase}
import com.google.protobuf.ByteString
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.util.Success

class ApiPackageManagementServiceSpec
    extends AsyncWordSpec
    with TelemetrySpecBase
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with AkkaBeforeAndAfterAll {

  import ApiPackageManagementServiceSpec._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "ApiPackageManagementService" should {
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
        }
    }
  }

  private def createApiService(): PackageManagementServiceGrpc.PackageManagementService = {
    val mockDarReader = mock[DarReader[Archive]]
    when(mockDarReader.readArchive(any[String], any[ZipInputStream], any[Int]))
      .thenReturn(Success(new Dar[Archive](anArchive, List.empty)))

    val mockEngine = mock[Engine]
    when(
      mockEngine.validatePackages(any[Set[PackageId]], any[Map[PackageId, Ast.Package]])
    ).thenReturn(Right(()))

    val mockIndexTransactionsService = mock[IndexTransactionsService]
    when(mockIndexTransactionsService.currentLedgerEnd())
      .thenReturn(Future.successful(Absolute(Ref.LedgerString.assertFromString("0"))))

    val mockIndexPackagesService = mock[IndexPackagesService]
    when(mockIndexPackagesService.packageEntries(any[Option[Absolute]])(any[LoggingContext]))
      .thenReturn(
        Source.single(
          PackageEntry.PackageUploadAccepted(aSubmissionId, Instant.EPOCH)
        )
      )

    ApiPackageManagementService.createApiService(
      mockIndexPackagesService,
      mockIndexTransactionsService,
      TestWritePackagesService,
      Duration.ZERO,
      mockEngine,
      mockDarReader,
    )
  }

  private object TestWritePackagesService extends WritePackagesService {
    override def uploadPackages(
        submissionId: SubmissionId,
        archives: List[DamlLf.Archive],
        sourceDescription: Option[String],
    )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] = {
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      CompletableFuture.completedFuture(SubmissionResult.Acknowledged)
    }
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
}
