// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.VersionFile
import com.digitalasset.canton.tracing.TraceContext
import com.softwaremill.quicklens.*
import sttp.apispec
import sttp.apispec.asyncapi.AsyncAPI
import sttp.apispec.openapi.OpenAPI
import sttp.apispec.{Schema, SchemaLike, asyncapi, openapi}
import sttp.tapir.docs.asyncapi.AsyncAPIInterpreter
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.{AnyEndpoint, headers}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.immutable.ListMap

class ApiDocsGenerator(override protected val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

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

  def createDocs(
      lapiVersion: String,
      endpointDescriptions: Seq[AnyEndpoint],
  )(implicit traceContext: TraceContext): ApiDocs = {
    val openApiDocs: openapi.OpenAPI = OpenAPIDocsInterpreter()
      .toOpenAPI(
        endpointDescriptions,
        "JSON Ledger API HTTP endpoints",
        lapiVersion,
      )
      .openapi("3.0.3")

    val protoData = ProtoInfo
      .loadData()
      .fold(
        error => {
          logger.warn(s"Cannot load proto data for documentation $error")
          // If we cannot load protoInfo data then we  generate docs with no supplemented comments
          ProtoInfo(ExtractedProtoComments(Map.empty, Map.empty))
        },
        identity,
      )
    val supplementedOpenApi = supplyProtoDocs(openApiDocs, protoData)
    import sttp.apispec.openapi.circe.yaml.*

    val asyncApiDocs: AsyncAPI = AsyncAPIInterpreter().toAsyncAPI(
      endpointDescriptions,
      "JSON Ledger API WebSocket endpoints",
      lapiVersion,
    )
    val supplementedAsyncApi = supplyProtoDocs(asyncApiDocs, protoData)
    import sttp.apispec.asyncapi.circe.yaml.*

    val fixed3_0_3Api: OpenAPI = OpenAPI3_0_3Fix.fixTupleDefinition(supplementedOpenApi)

    val openApiYaml: String = fixed3_0_3Api.toYaml3_0_3
    val asyncApiYaml: String = supplementedAsyncApi.toYaml

    ApiDocs(openApiYaml, asyncApiYaml)
  }

  def createStaticDocs()(implicit traceContext: TraceContext): ApiDocs =
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
  // No trace context needed -> it is used only for static yaml generation
  implicit val traceContext: TraceContext = TraceContext.empty
  val apiDocsGenerator = new ApiDocsGenerator(NamedLoggerFactory.root)
  val staticDocs = apiDocsGenerator.createStaticDocs()
  Files.write(
    docsTargetDir.resolve("openapi.yaml"),
    staticDocs.openApi.getBytes(StandardCharsets.UTF_8),
  )
  Files.write(
    docsTargetDir.resolve("asyncapi.yaml"),
    staticDocs.asyncApi.getBytes(StandardCharsets.UTF_8),
  )
}

object OpenAPI3_0_3Fix {
  def fixTupleDefinition(existingDefinition: OpenAPI): OpenAPI = existingDefinition
    .modify(_.components.each.schemas.at("Tuple2_String_String"))
    .using {
      case schema: Schema =>
        schema.copy(
          prefixItems = None,
          items = Some(sttp.apispec.Schema(`type` = Some(List(apispec.SchemaType.String)))),
          minItems = Some(2),
          maxItems = Some(2),
        )
      case other => other
    }

}
