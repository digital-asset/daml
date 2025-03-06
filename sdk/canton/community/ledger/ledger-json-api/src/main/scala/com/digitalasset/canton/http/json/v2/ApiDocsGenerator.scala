// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.platform.apiserver.services.VersionFile
import sttp.apispec.asyncapi.AsyncAPI
import sttp.apispec.{Schema, SchemaLike, asyncapi, openapi}
import sttp.tapir.docs.asyncapi.AsyncAPIInterpreter
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.{AnyEndpoint, headers}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.immutable.ListMap

object ApiDocsGenerator {

  /** Endpoints used for static documents generation - should match with the live endpoints
    * @see
    *   V2Routes.serverEndpoints
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

  private def supplyProtoDocs(initial: openapi.OpenAPI, proto: ProtoInfo): openapi.OpenAPI = {

    val updatedComponents = initial.components.map(component => supplyComponents(component, proto))
    initial.copy(components = updatedComponents)
  }

  private def supplyProtoDocs(initial: asyncapi.AsyncAPI, proto: ProtoInfo): asyncapi.AsyncAPI = {
    val updatedComponents = initial.components.map(component => supplyComponents(component, proto))
    initial.copy(components = updatedComponents)
  }

  private def supplyComponents(
      component: openapi.Components,
      proto: ProtoInfo,
  ): openapi.Components = {
    val updatedSchemas: ListMap[String, SchemaLike] =
      component.schemas.map(s => importSchemaLike(s, proto))
    component.copy(schemas = updatedSchemas)
  }

  private def supplyComponents(
      component: asyncapi.Components,
      proto: ProtoInfo,
  ): asyncapi.Components = {
    val updatedSchemas: ListMap[String, Schema] =
      component.schemas.map(s => importSchema(s._1, s._2, proto))
    component.copy(schemas = updatedSchemas)
  }

  private def importSchemaLike(
      component: (String, SchemaLike),
      proto: ProtoInfo,
  ): (String, SchemaLike) =
    component._2 match {
      case schema: Schema => importSchema(component._1, schema, proto)
      case _ => component
    }

  private def importSchema(
      componentName: String,
      componentSchema: Schema,
      proto: ProtoInfo,
  ): (String, Schema) =
    proto
      .findMessageInfo(componentName)
      .map { message =>
        val properties = componentSchema.properties.map { case (propertyName, propertySchema) =>
          message
            .getFieldComment(propertyName)
            .map { comments =>
              (
                propertyName,
                propertySchema match {
                  case pSchema: Schema =>
                    pSchema.copy(description = Some(comments))
                  case _ => propertySchema
                },
              )
            }
            .getOrElse(
              (propertyName, propertySchema)
            )
        }
        (
          componentName,
          componentSchema.copy(description = message.getComments(), properties = properties),
        )
      }
      .getOrElse((componentName, componentSchema))

  def createDocs(lapiVersion: String, endpointDescriptions: Seq[AnyEndpoint]): ApiDocs = {
    val openApiDocs: openapi.OpenAPI = OpenAPIDocsInterpreter().toOpenAPI(
      endpointDescriptions,
      "JSON Ledger API HTTP endpoints",
      lapiVersion,
    )

    val proto = new ProtoParser().readProto()
    val supplementedOpenApi = supplyProtoDocs(openApiDocs, proto)
    import sttp.apispec.openapi.circe.yaml.*

    val asyncApiDocs: AsyncAPI = AsyncAPIInterpreter().toAsyncAPI(
      endpointDescriptions,
      "JSON Ledger API WebSocket endpoints",
      lapiVersion,
    )
    val supplementedAsyncApi = supplyProtoDocs(asyncApiDocs, proto)
    import sttp.apispec.asyncapi.circe.yaml.*

    val openApiYaml: String = supplementedOpenApi.toYaml
    val asyncApiYaml: String = supplementedAsyncApi.toYaml

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
