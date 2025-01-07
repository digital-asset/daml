// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.value
import com.daml.ledger.api.v2.value.{Identifier, Value}
import com.digitalasset.canton.caching.CaffeineCache
import com.digitalasset.canton.fetchcontracts.util.IdentifierConverters
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidField
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{IdString, PackageId, PackageRef}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Ast.PackageSignature
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.transcode.codec.proto.GrpcValueCodec
import com.digitalasset.transcode.daml_lf.{Dictionary, SchemaEntity, SchemaProcessor}
import com.digitalasset.transcode.schema.SchemaVisitor
import com.digitalasset.transcode.{Codec, Converter}
import com.github.benmanes.caffeine.cache.Caffeine

import scala.concurrent.{ExecutionContext, Future}

class SchemaProcessors(
    val fetchSignatures: Option[String] => Future[Map[Ref.PackageId, Ast.PackageSignature]]
)(implicit
    val executionContext: ExecutionContext
) {

  private val cache = CaffeineCache[String, Map[Ref.PackageId, Ast.PackageSignature]](
    Caffeine
      .newBuilder()
      .maximumSize(SchemaProcessorsCache.MaxCacheSize),
    None,
  )

  private def nullToEmpty(jsonVal: ujson.Value): ujson.Value = jsonVal match {
    case ujson.Null => ujson.Obj()
    case any => any
  }

  private def extractTemplateValue[V](map: Map[Ref.Identifier, V])(key: Ref.Identifier)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): V =
    map.getOrElse(
      key,
      throw RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound
        .Reject(Seq(Left(key)))
        .asGrpcError,
    )

  private def convertGrpcToJson(templateId: Ref.Identifier, proto: value.Value)(
      token: Option[String]
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[ujson.Value] =
    prepareToJson(templateId.packageId, token).map(dict =>
      extractTemplateValue(dict.templates)(templateId).convert(proto)
    )

  def contractArgFromJsonToProto(
      template: value.Identifier,
      jsonArgsValue: ujson.Value,
  )(implicit
      token: Option[String],
      contextualizedErrorLogger: ContextualizedErrorLogger,
  ): Future[value.Value] =
    findTemplate(template, token).flatMap(templateId =>
      prepareToProto(templateId.packageId, token).map(dictionary =>
        extractTemplateValue(dictionary.templates)(templateId).convert(jsonArgsValue)
      )
    )

  def choiceArgsFromJsonToProto(
      template: value.Identifier,
      choiceName: IdString.Name,
      jsonArgsValue: ujson.Value,
  )(implicit
      token: Option[String],
      contextualizedErrorLogger: ContextualizedErrorLogger,
  ): Future[value.Value] =
    findTemplate(template, token).flatMap { templateId =>
      prepareToProto(templateId.packageId, token)
        .map(
          _.choiceArguments
            .getOrElse(
              (templateId, choiceName),
              throw invalidChoiceException(templateId, choiceName),
            )
            .convert(nullToEmpty(jsonArgsValue))
        )
    }

  def contractArgFromProtoToJson(
      template: value.Identifier,
      protoArgs: value.Record,
  )(implicit
      token: Option[String],
      contextualizedErrorLogger: ContextualizedErrorLogger,
  ): Future[ujson.Value] =
    findTemplate(template, token).flatMap { templateId =>
      convertGrpcToJson(templateId, value.Value(value.Value.Sum.Record(protoArgs)))(token)
    }

  def choiceArgsFromProtoToJson(
      template: value.Identifier,
      choiceName: IdString.Name,
      protoArgs: value.Value,
  )(implicit
      token: Option[String],
      contextualizedErrorLogger: ContextualizedErrorLogger,
  ): Future[ujson.Value] =
    findTemplate(template, token).flatMap(templateId =>
      prepareToJson(templateId.packageId, token).map(
        _.choiceArguments
          .getOrElse(
            (templateId, choiceName),
            throw invalidChoiceException(templateId, choiceName),
          )
          .convert(protoArgs)
      )
    )

  private def invalidChoiceException(templateId: Ref.Identifier, choiceName: IdString.Name)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) =
    InvalidArgument.Reject(s"Invalid template:$templateId or choice:$choiceName").asGrpcError

  def keyArgFromProtoToJson(
      template: value.Identifier,
      protoArgs: value.Value,
  )(implicit
      token: Option[String],
      contextualizedErrorLogger: ContextualizedErrorLogger,
  ): Future[ujson.Value] =
    findTemplate(template, token).flatMap(templateId =>
      prepareToJson(templateId.packageId, token).map(dict =>
        extractTemplateValue(dict.templates)(templateId).convert(protoArgs)
      )
    )

  def keyArgFromJsonToProto(
      template: value.Identifier,
      protoArgs: ujson.Value,
  )(implicit
      token: Option[String],
      contextualizedErrorLogger: ContextualizedErrorLogger,
  ): Future[value.Value] =
    findTemplate(template, token).flatMap(templateId =>
      prepareToProto(templateId.packageId, token).map(dict =>
        extractTemplateValue(dict.templateKeys)(templateId).convert(protoArgs)
      )
    )

  def exerciseResultFromProtoToJson(
      template: value.Identifier,
      choiceName: IdString.Name,
      v: value.Value,
  )(implicit
      token: Option[String],
      contextualizedErrorLogger: ContextualizedErrorLogger,
  ): Future[ujson.Value] =
    findTemplate(template, token).flatMap(templateId =>
      prepareToJson(templateId.packageId, token).map(
        _.choiceResults
          .getOrElse((templateId, choiceName), throw invalidChoiceException(templateId, choiceName))
          .convert(v)
      )
    )

  def exerciseResultFromJsonToProto(
      template: Identifier,
      choiceName: IdString.Name,
      value: ujson.Value,
  )(implicit
      token: Option[String],
      contextualizedErrorLogger: ContextualizedErrorLogger,
  ): Future[Option[Value]] = value match {
    case ujson.Null => Future(None)
    case _ =>
      findTemplate(template, token).flatMap(templateId =>
        prepareToProto(templateId.packageId, token)
          .flatMap(
            _.choiceResults
              .get(templateId -> choiceName)
              .map(Future.successful)
              .getOrElse(Future.failed(invalidChoiceException(templateId, choiceName)))
              .map(_.convert(value))
          )
          .map(Some(_))
      )
  }

  private def reloadSignatures(
      token: Option[String]
  ): Future[Map[Ref.PackageId, Ast.PackageSignature]] =
    fetchSignatures(token).map { signatures =>
      cache.put(token.getOrElse(""), signatures)
      signatures
    }

  private def findTemplate(
      template: value.Identifier,
      token: Option[String],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Ref.Identifier] =
    PackageRef.fromString(template.packageId) match {
      case Right(PackageRef.Name(name)) =>
        loadTemplate(name, template, token).map(IdentifierConverters.lfIdentifier)
      case Right(PackageRef.Id(_)) =>
        Future(IdentifierConverters.lfIdentifier(template))
      case Left(error) =>
        Future.failed(invalidField("package reference", error))
    }

  private def loadTemplate(
      packageName: String,
      template: Identifier,
      token: Option[String],
  ): Future[value.Identifier] = {

    def filterMatching(signatures: Map[PackageId, PackageSignature]) =
      signatures.filter { case (_, signature) =>
        signature.metadata.name == packageName
      }.toSeq

    val signatures: Map[PackageId, PackageSignature] =
      cache.getIfPresent(token.getOrElse("")).getOrElse(Map.empty)
    val matchingPackages = filterMatching(signatures)

    val packagesMatchingName =
      if (matchingPackages.isEmpty) {
        reloadSignatures(token).map(filterMatching)
      } else {
        Future(matchingPackages)
      }

    packagesMatchingName.map { packages =>
      val topPackage: (PackageId, PackageSignature) = packages
        .sortBy { case (_, signature) =>
          signature.metadata.version
        }
        .reverse
        .headOption
        .getOrElse(
          throw new RuntimeException(
            s"Failed to load template for $template because there were no packages with name $packageName"
          )
        )
      Identifier(topPackage._1, template.moduleName, template.entityName)
    }
  }

  private def ensurePackage(
      packageId: Ref.PackageId,
      token: Option[String],
  ): Future[Map[Ref.PackageId, Ast.PackageSignature]] = {
    val tokenKey = token.getOrElse("")
    val signatures = cache.getIfPresent(tokenKey)
    signatures.fold {
      // TODO(i20707) use the new Ledger API's package metadata view
      reloadSignatures(token)
    } { signatures =>
      if (signatures.contains(packageId)) {
        Future(signatures)
      } else {
        reloadSignatures(token)
      }
    }
  }

  private def prepareToProto(
      packageId: Ref.PackageId,
      token: Option[String],
  ): Future[Dictionary[Converter[ujson.Value, Value]]] =
    ensurePackage(packageId, token).map { signatures =>
      val visitor: SchemaVisitor { type Type = (Codec[ujson.Value], Codec[value.Value]) } =
        SchemaVisitor.compose(new JsonCodec(), GrpcValueCodec)
      val collector =
        Dictionary.collect[Converter[ujson.Value, value.Value]] compose SchemaEntity
          .map((v: visitor.Type) => Converter(v._1, v._2))

      SchemaProcessor
        .process(packages = signatures)(visitor)(
          collector
        )
        .fold(error => throw new IllegalStateException(error), identity)
    }

  private def prepareToJson(
      packageId: Ref.PackageId,
      token: Option[String],
  ): Future[Dictionary[Converter[Value, ujson.Value]]] =
    ensurePackage(packageId, token).map { signatures =>
      val visitor: SchemaVisitor { type Type = (Codec[value.Value], Codec[ujson.Value]) } =
        SchemaVisitor.compose(GrpcValueCodec, new JsonCodec())
      val collector =
        Dictionary.collect[Converter[value.Value, ujson.Value]] compose SchemaEntity
          .map((v: visitor.Type) => Converter(v._1, v._2))

      SchemaProcessor
        .process(packages = signatures)(visitor)(
          collector
        )
        .fold(error => throw new IllegalStateException(error), identity)
    }
}

object SchemaProcessorsCache {
  val MaxCacheSize: Long = 100
}
