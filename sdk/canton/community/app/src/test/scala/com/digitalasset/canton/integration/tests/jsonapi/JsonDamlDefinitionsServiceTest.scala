// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import better.files.File
import com.daml.ledger.api.v2.package_service.ListPackagesResponse
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.http.json.v2.JsPackageCodecs.*
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.AllTemplatesResponse
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.Codecs.allTemplatesResponseCodec
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.archive
import com.digitalasset.daml.lf.archive.DamlLf
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.typesig.reader.DamlLfArchiveReader
import com.google.protobuf
import com.google.protobuf.ByteString
import io.circe.parser.{decode, parse}
import monocle.Monocle.toAppliedFocusOps
import org.apache.pekko.http.scaladsl.model.{HttpHeader, StatusCode, StatusCodes, Uri}
import org.scalatest.Assertion

import java.nio.file.Files
import scala.concurrent.Future

class JsonDamlDefinitionsServiceTest
    extends AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken {

  private val RootTestResources =
    "community/ledger/ledger-json-api/target/scala-2.13/resource_managed/test"

  private val GoldenTestResources =
    "community/ledger/ledger-json-api/src/test/resources/daml-definitions-service-test-resources"

  // TODO(#21695): Use compile-time generated DARs once stable
  private val ReferenceTestDar = s"$RootTestResources/DamlDefinitionsServiceMain.dar"

  private val GoldenTestDar = s"$GoldenTestResources/DamlDefinitionsServiceMain.dar"

  private val TestedModules = Seq(
    "ExternalDep",
    "MainApp",
  )

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransform(
      ConfigTransforms.updateAllParticipantConfigs_(
        _.focus(_.httpLedgerApi.damlDefinitionsServiceEnabled)
          .replace(true)
      )
    )

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  "Daml definitions service" should {
    "output the definitions of the reference DAR" onlyRunWithOrGreaterThan (ProtocolVersion.dev) in httpTestFixture {
      fixture =>
        val darContent: ByteString =
          protobuf.ByteString.copyFrom(Files.readAllBytes(File(ReferenceTestDar).path))

        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
          for {
            _ <- fixture
              .postBinaryContent(Uri.Path("/v2/packages"), darContent, headers)
              .map { case (status, _) => status should be(StatusCodes.OK) }

            selectedPackagesIds <- findTestPackages(fixture, headers)
            _ = selectedPackagesIds.foreach { packageId =>
              File(s"$RootTestResources/$packageId")
                .createDirectoryIfNotExists(createParents = true)
                .discard
              fixture
                .getRequestString(Uri.Path(s"/v2/definitions/packages/$packageId"), headers)
                .map { case (code, packageSigResult) =>
                  code should be(StatusCodes.OK)
                  val prettyResult = prettySortedJsonString(packageSigResult)
                  File(s"$RootTestResources/$packageId/package-signature.json")
                    .createFileIfNotExists()
                    .overwrite(prettyResult)
                    .discard
                }
                .futureValue
            }

            templateIds <- fixture
              .getRequestString(Uri.Path("/v2/definitions/templates"), headers)
              .map(_._2)
              .map(
                decode[AllTemplatesResponse](_)
                  .getOrElse(fail("unexpectedly failed"))
                  .templates
                  .filter(templateId => selectedPackagesIds.contains(templateId.packageId))
              )

            _ = templateIds.foreach { templateId =>
              fixture
                .getRequestString(Uri.Path(s"/v2/definitions/templates/$templateId"), headers)
                .map { case (code, templateDefResult) =>
                  code should be(StatusCodes.OK)
                  val packageId = templateId.packageId

                  val templateIdPath =
                    s"$RootTestResources/$packageId/${windowsSafeTemplateId(templateId.toString())}.json"
                  val prettyResult = prettySortedJsonString(templateDefResult)
                  File(templateIdPath)
                    .createFileIfNotExists()
                    .overwrite(prettyResult)
                    .discard
                }
                .futureValue
            }
          } yield ()
        }
    }

    // TODO(#27652): re-enable after using DARs generated with SBT or non-dev dars
    "validate the definitions in the golden DAR against the golden files" onlyRunWithOrGreaterThan ProtocolVersion.dev ignore httpTestFixture {
      fixture =>
        val darContent: ByteString =
          protobuf.ByteString.copyFrom(Files.readAllBytes(File(GoldenTestDar).path))

        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
          def matchesExpected(
              pkgId: String,
              code: StatusCode,
              result: String,
              goldenFile: String,
          ): Assertion = {
            code should be(StatusCodes.OK)

            val expected =
              prettySortedJsonString(
                File(s"$RootTestResources/$pkgId/$goldenFile.json").contentAsString
              )

            val prettyResult = prettySortedJsonString(result)
            prettyResult shouldBe expected
          }

          for {
            _ <- fixture
              .postBinaryContent(Uri.Path("/v2/packages"), darContent, headers)
              .map { case (status, _) => status should be(StatusCodes.OK) }
            selectedGoldenPackagesIds <- findTestPackages(fixture, headers)
            _ = selectedGoldenPackagesIds.foreach { pkgId =>
              fixture
                .getRequestString(Uri.Path(s"/v2/definitions/packages/$pkgId"), headers)
                .map { case (code, packageSigResult) =>
                  matchesExpected(pkgId, code, packageSigResult, "package-signature")
                }
                .futureValue
            }

            templateIds <- fixture
              .getRequestString(Uri.Path("/v2/definitions/templates"), headers)
              .map(_._2)
              .map(
                decode[AllTemplatesResponse](_)
                  .getOrElse(fail("unexpectedly failed"))
                  .templates
                  .filter(templateId => selectedGoldenPackagesIds.contains(templateId.packageId))
              )

            _ = templateIds.foreach { templateId =>
              fixture
                .getRequestString(Uri.Path(s"/v2/definitions/templates/$templateId"), headers)
                .map { case (code, templateDefResult) =>
                  val pkgId = templateId.packageId
                  matchesExpected(
                    pkgId,
                    code,
                    templateDefResult,
                    windowsSafeTemplateId(templateId.toString()),
                  )
                }
                .futureValue
            }
          } yield ()
        }
    }
  }

  private def findTestPackages(
      fixture: HttpServiceTestFixtureData,
      headers: List[HttpHeader],
  ) = for {
    allPackages <- fixture
      .getRequestString(Uri.Path("/v2/packages"), headers)
      .map(_._2)
      .map(decode[ListPackagesResponse](_).value)
    result <- allPackages.packageIds.foldLeft(Future.successful(Seq.empty[String])) {
      case (s, pkgId) =>
        s.flatMap(seq =>
          getRequestBinaryData(fixture.uri withPath Uri.Path(s"/v2/packages/$pkgId"), headers)
            .map(_._2)
            .map { content =>
              val byteString = ByteString.copyFrom(content.toByteBuffer)
              val payload: DamlLf.ArchivePayload =
                archive.ArchivePayloadParser.assertFromByteString(byteString)
              val (packageId, astPackage) = DamlLfArchiveReader
                .readPackage(PackageId.assertFromString(pkgId), payload)
                .toEither
                .value
              val keys = astPackage.modules.keys.toSeq.map(_.toString)
              if (keys.exists(TestedModules.contains)) {
                seq :+ packageId
              } else {
                seq
              }
            }
        )
    }
  } yield result

  private def windowsSafeTemplateId(templateId: String): String =
    templateId.replaceAll("\\:", "_")

  private def prettySortedJsonString(s: String): String =
    parse(s) match {
      case Left(err) => throw new RuntimeException(s"Failed to parse JSON", err)
      case Right(j) => j.spaces2SortKeys
    }
}
