// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.http.json.v2.ApiDocsGenerator
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}

final class OpenApiTests
    extends AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
  private val apiDocsGenerator = new ApiDocsGenerator(loggerFactory)
  private val expectedOpenApiServices = Seq(
    "commands",
    "events",
    "idps",
    "interactive-submission",
    "packages",
    "package-vetting",
    "parties",
    "state",
    "updates",
    "authenticated-user",
    "users",
    "version",
  )

  private val expectedAsyncApiServices = Seq(
    "commands",
    "state",
    "updates",
  )

  "JSON openapi documentation" should {
    val protoInfo = apiDocsGenerator.loadProtoData()
    val staticDocs = apiDocsGenerator.createStaticDocs(protoInfo)

    "should be consistent with live docs" in httpTestFixture { fixture =>
      /** We generate documentation "statically" without starting an http server However, when the
        * server is running the documentation is generated from actually running endpoints This test
        * checks that both of them are matching
        * @see
        *   ApiDocsGenerator.staticDocumentationEndpoints
        */
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        for {
          _ <- fixture.getRequestString(Uri.Path("/docs/openapi"), headers).map {
            case (status, result) =>
              status should be(StatusCodes.OK)
              result should be(staticDocs.openApi)
          }
          _ <- fixture.getRequestString(Uri.Path("/docs/asyncapi"), headers).map {
            case (status, result) =>
              status should be(StatusCodes.OK)
              result should be(staticDocs.asyncApi)
          }

        } yield ()
      }
    }

    "has proper version" in { _ =>
      val yaml = io.circe.yaml.parser.parse(staticDocs.openApi)
      val version = (yaml.value \\ "info").head \\ "version"

      val semVerRegex =
        """^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"""

      version.head.asString.getOrElse("") should fullyMatch regex semVerRegex
    }

    "contains expected openapi services " in { _ =>
      val yaml = io.circe.yaml.parser.parse(staticDocs.openApi)
      val openapiServices =
        (yaml.value \\ "paths").head.asObject.value.keys
          .map(_.split("/").drop(2).head)
          .toSet
      openapiServices should contain theSameElementsAs expectedOpenApiServices
    }

    "contains expected async services " in { _ =>
      val yaml = io.circe.yaml.parser.parse(staticDocs.asyncApi)
      val asyncServices =
        (yaml.value \\ "channels").head.asObject.value.keys
          .map(_.split("/").drop(2).head)
          .toSet

      asyncServices should contain theSameElementsAs expectedAsyncApiServices
    }
  }
}
