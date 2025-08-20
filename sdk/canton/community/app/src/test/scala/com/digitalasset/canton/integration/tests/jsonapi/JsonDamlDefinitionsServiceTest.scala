// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import better.files.File
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.AllTemplatesResponse
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.Codecs.allTemplatesResponseCodec
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf
import com.google.protobuf.ByteString
import io.circe.parser.decode
import monocle.Monocle.toAppliedFocusOps
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes, Uri}
import org.scalatest.Assertion
import spray.json.*

import java.nio.file.Files

class JsonDamlDefinitionsServiceTest
    extends AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken {

  private val RootTestResources =
    "community/ledger/ledger-json-api/src/test/resources/daml-definitions-service-test-resources"

  // TODO(#21695): Use compile-time generated DARs once stable
  private val TestDar = s"$RootTestResources/DamlDefinitionsServiceMain.dar"
  private val GoldenPackageIds = Seq(
    "8adf3cb77b1c56d0e94563d731b174f01b7cb535b4cec25458381aeb25db0fce", // Dependency package
    "c2cc6ec2dbe54b24daeb19166467b5440abd113bf73364116cb454099d06053e", // Dependant package
  )

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransform(
      ConfigTransforms.updateAllParticipantConfigs_(
        _.focus(_.httpLedgerApi)
          .modify(_.map(_.focus(_.damlDefinitionsServiceEnabled).replace(true)))
      )
    )

  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  "Daml definitions service" should {
    // TODO(#21695): Test only used to generate golden files.
    //               Ignore once stable
    "output the definitions of the golden DAR" ignore httpTestFixture { fixture =>
      val darContent: ByteString =
        protobuf.ByteString.copyFrom(Files.readAllBytes(File(TestDar).path))

      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        for {
          _ <- fixture
            .postBinaryContent(Uri.Path("/v2/packages"), darContent, headers)
            .map { case (status, _) => status should be(StatusCodes.OK) }

          _ = GoldenPackageIds.foreach { packageId =>
            File(s"$RootTestResources/$packageId")
              .createDirectoryIfNotExists(createParents = true)
              .discard

            fixture
              .getRequestString(Uri.Path(s"/v2/definitions/packages/$packageId"), headers)
              .map { case (code, packageSigResult) =>
                code should be(StatusCodes.OK)

                val prettyResult = packageSigResult.parseJson.sortedPrint
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
                .filter(templateId => GoldenPackageIds.contains(templateId.packageId))
            )

          _ = templateIds.foreach { templateId =>
            fixture
              .getRequestString(Uri.Path(s"/v2/definitions/templates/$templateId"), headers)
              .map { case (code, templateDefResult) =>
                code should be(StatusCodes.OK)
                val packageId = templateId.packageId

                val templateIdPath =
                  s"$RootTestResources/$packageId/${windowsSafeTemplateId(templateId.toString())}.json"
                val prettyResult = templateDefResult.parseJson.sortedPrint
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

    "validate the definitions in the golden DAR against the golden files" onlyRunWithOrGreaterThan ProtocolVersion.dev ignore httpTestFixture {
      fixture =>
        val darContent: ByteString =
          protobuf.ByteString.copyFrom(Files.readAllBytes(File(TestDar).path))

        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
          def matchesExpected(
              pkgId: String,
              code: StatusCode,
              result: String,
              goldenFile: String,
          ): Assertion = {
            code should be(StatusCodes.OK)

            val expected =
              File(
                s"$RootTestResources/$pkgId/$goldenFile.json"
              ).contentAsString.parseJson.sortedPrint

            val prettyResult = result.parseJson.sortedPrint
            prettyResult shouldBe expected
          }

          for {
            _ <- fixture
              .postBinaryContent(Uri.Path("/v2/packages"), darContent, headers)
              .map { case (status, _) => status should be(StatusCodes.OK) }

            _ = GoldenPackageIds.foreach { pkgId =>
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
                  .filter(templateId => GoldenPackageIds.contains(templateId.packageId))
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

  private def windowsSafeTemplateId(templateId: String): String =
    templateId.replaceAll("\\:", "_")
}
