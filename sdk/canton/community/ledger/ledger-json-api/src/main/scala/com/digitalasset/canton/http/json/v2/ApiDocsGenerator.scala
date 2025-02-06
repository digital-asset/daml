// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.platform.apiserver.services.VersionFile
import sttp.apispec.asyncapi.AsyncAPI
import sttp.apispec.openapi.OpenAPI
import sttp.tapir.docs.asyncapi.AsyncAPIInterpreter
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.{AnyEndpoint, headers}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object ApiDocsGenerator {

  /** Endpoints used for static documents generation - should match with the live endpoints
    *  @see V2Routes.serverEndpoints
    */
  private val staticDocumentationEndpoints: Seq[AnyEndpoint] = {
    val services: Seq[DocumentationEndpoints] =
      Seq(
        JsCommandService,
        JsEventService,
        JsVersionService,
        JsPackageService,
        JsPartyManagementService,
        JsStateService,
        JsUpdateService,
        JsUserManagementService,
        JsIdentityProviderService,
        JsMeteringService,
        JsInteractiveSubmissionService,
      )
    services.flatMap(service => service.documentation)
  }

  def createDocs(lapiVersion: String, endpointDescriptions: Seq[AnyEndpoint]) = {
    val openApiDocs: OpenAPI = OpenAPIDocsInterpreter().toOpenAPI(
      endpointDescriptions,
      "JSON Ledger API HTTP endpoints",
      lapiVersion,
    )
    import sttp.apispec.openapi.circe.yaml.*

    val asyncApiDocs: AsyncAPI = AsyncAPIInterpreter().toAsyncAPI(
      endpointDescriptions,
      "JSON Ledger API WebSocket endpoints",
      lapiVersion,
    )
    import sttp.apispec.asyncapi.circe.yaml.*

    val openApiYaml: String = openApiDocs.toYaml
    val asyncApiYaml: String = asyncApiDocs.toYaml

    ApiDocs(openApiYaml, asyncApiYaml)
  }

  def createStaticDocs(): ApiDocs =
    createDocs(
      VersionFile.readVersion().getOrElse("unknown"),
      staticDocumentationEndpoints.map(addHeaders),
    )

  private def addHeaders(endpoint: AnyEndpoint) =
    endpoint.in(headers)
}

final case class ApiDocs(openApi: String, asyncApi: String)

object GenerateJSONApiDocs extends App {
  val docsDir = "target/apidocs"
  val docsTargetDir = Paths.get(docsDir)
  Files.createDirectories(docsTargetDir)
  val staticDocs = ApiDocsGenerator.createStaticDocs()
  Files.write(
    docsTargetDir.resolve("openapi.yaml"),
    staticDocs.openApi.getBytes(StandardCharsets.UTF_8),
  )
  Files.write(
    docsTargetDir.resolve("asyncapi.yaml"),
    staticDocs.asyncApi.getBytes(StandardCharsets.UTF_8),
  )
}
