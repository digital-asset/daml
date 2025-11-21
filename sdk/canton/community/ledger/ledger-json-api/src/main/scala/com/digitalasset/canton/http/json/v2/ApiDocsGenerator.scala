// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.VersionFile
import com.softwaremill.quicklens.*
import monocle.macros.syntax.lens.*
import sttp.apispec
import sttp.apispec.asyncapi.{AsyncAPI, ChannelItem, ReferenceOr}
import sttp.apispec.openapi.{OpenAPI, Operation, PathItem}
import sttp.apispec.{Schema, SchemaLike, asyncapi, openapi}
import sttp.tapir.AnyEndpoint
import sttp.tapir.docs.asyncapi.AsyncAPIInterpreter
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter

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
    val updatedPaths =
      initial.paths.pathItems.map { case (path, pathItem) =>
        (path, supplyPathItem(pathItem, proto))
      }
    initial.copy(
      components = updatedComponents,
      paths = initial.paths.copy(pathItems = updatedPaths),
    )
  }
  private def supplyPathItem(pathItem: PathItem, proto: ProtoInfo) =
    pathItem.copy(
      get = pathItem.get.map(withSuppliedServiceDescription(_, proto)),
      put = pathItem.put.map(withSuppliedServiceDescription(_, proto)),
      post = pathItem.post.map(withSuppliedServiceDescription(_, proto)),
      delete = pathItem.delete.map(withSuppliedServiceDescription(_, proto)),
      options = pathItem.options.map(withSuppliedServiceDescription(_, proto)),
      head = pathItem.head.map(withSuppliedServiceDescription(_, proto)),
      patch = pathItem.patch.map(withSuppliedServiceDescription(_, proto)),
      trace = pathItem.trace.map(withSuppliedServiceDescription(_, proto)),
    )

  private def supplyServiceDescriptions(description: String, proto: ProtoInfo) = {
    val lines: Seq[String] = description
      .split("\n")
      .toSeq
      .flatMap { (line: String) =>
        line match {
          case ProtoLink(protoFile, serviceName, methodName) =>
            Seq(proto.findServiceDescription(protoFile, serviceName, methodName))
          case other => Seq(other)
        }
      }
    lines.mkString("\n")
  }

  private def withSuppliedServiceDescription(operation: Operation, proto: ProtoInfo) =
    operation.focus(_.description).some.modify(supplyServiceDescriptions(_, proto))

  private def supplyProtoDocs(initial: asyncapi.AsyncAPI, proto: ProtoInfo): asyncapi.AsyncAPI = {
    val updatedComponents = initial.components.map(component => supplyComponents(component, proto))
    val updateChannels = initial.channels.map { case (channelName, channel) =>
      (channelName, updateChannel(channel, proto))
    }
    initial.copy(components = updatedComponents, channels = updateChannels)
  }

  private def updateChannel(
      channel: ReferenceOr[ChannelItem],
      proto: ProtoInfo,
  ): ReferenceOr[ChannelItem] = {
    def fixOpDescription(operation: Option[asyncapi.Operation], ch: ChannelItem) = operation.map {
      op =>
        op.copy(
          description = if (op.description == ch.description) {
            // Remove redundant descriptions
            None
          } else {
            op.description.map(description => supplyServiceDescriptions(description, proto))
          }
        )
    }
    channel.map { ch =>
      val subscribe = fixOpDescription(ch.subscribe, ch)
      val publish = fixOpDescription(ch.publish, ch)

      // format: off
      ch.focus(_.description).some.modify(supplyServiceDescriptions(_, proto))
        .focus(_.subscribe).replace(subscribe)
        .focus(_.publish).replace(publish)
      // format: on
    }
  }

  private def supplyComponents(
      component: openapi.Components,
      proto: ProtoInfo,
  ): openapi.Components = {
    val updatedSchemas: ListMap[String, SchemaLike] =
      component.schemas.map(s => importSchemaLike(s, proto, findUniqueParent(s, component.schemas)))
    component.copy(schemas = updatedSchemas)
  }

  private def findUniqueParent(
      component: (String, SchemaLike),
      schemas: ListMap[String, SchemaLike],
  ) = {
    val (name, schema) = component
    schema match {
      case s: Schema if s.oneOf.nonEmpty =>
        val referencingSchemas = schemas.collect {
          case (parentName, parent: Schema) if parent.properties.exists {
                case (_, prop: Schema) => prop.$ref.contains(s"#/components/schemas/$name")
                case _ => false
              } =>
            parentName
        }
        referencingSchemas match {
          case single :: Nil => Some(single)
          case _ => None
        }
      case _ => None
    }
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
      parentComponent: Option[String],
  ): (String, SchemaLike) =
    component._2 match {
      case schema: Schema => importSchema(component._1, schema, proto, parentComponent)
      case _ => component
    }

  private def importSchema(
      componentName: String,
      componentSchema: Schema,
      proto: ProtoInfo,
      parentComponent: Option[String] = None,
  ): (String, Schema) =
    proto
      .findMessageInfo(componentName, parentComponent)
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

  def loadProtoData(): ProtoInfo =
    ProtoInfo
      .loadData()
      .fold(
        error => {
          throw new IllegalStateException(s"Cannot load proto data for documentation $error")
        },
        identity,
      )

  def createDocs(
      lapiVersion: String,
      endpointDescriptions: Seq[AnyEndpoint],
      protoData: ProtoInfo,
  ): ApiDocs = {
    val openApiDocs: openapi.OpenAPI = OpenAPIDocsInterpreter()
      .toOpenAPI(
        endpointDescriptions,
        "JSON Ledger API HTTP endpoints",
        lapiVersion,
      )
      .openapi("3.0.3")

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

  def createStaticDocs(protoInfo: ProtoInfo): ApiDocs =
    createDocs(
      VersionFile.readVersion().getOrElse("unknown"),
      staticDocumentationEndpoints,
      protoInfo,
    )

}

final case class ApiDocs(openApi: String, asyncApi: String)

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
