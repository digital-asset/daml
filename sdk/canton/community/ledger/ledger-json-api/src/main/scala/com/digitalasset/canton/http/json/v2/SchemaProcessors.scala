// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.value
import com.daml.ledger.api.v2.value.{Identifier, Value}
import com.digitalasset.canton.fetchcontracts.util.IdentifierConverters
import com.digitalasset.canton.http.json.v2.SchemaProcessors.{
  JsonDict,
  PackageSignatures,
  ProtoDict,
  ResultOps,
}
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidField
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.NotFound.PackageNamesNotFound
import com.digitalasset.canton.logging.ContextualizedErrorLogger
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterAsyncOps
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{IdString, PackageRef}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.transcode.codec.proto.GrpcValueCodec
import com.digitalasset.transcode.daml_lf.{Dictionary, SchemaEntity, SchemaProcessor}
import com.digitalasset.transcode.schema.SchemaVisitor
import com.digitalasset.transcode.{Codec, Converter}
import io.grpc.StatusRuntimeException

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class SchemaProcessors(
    fetchSignatures: ContextualizedErrorLogger => PackageSignatures
)(implicit executionContext: ExecutionContext) {

  def contractArgFromJsonToProto(
      template: value.Identifier,
      jsonArgsValue: ujson.Value,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      protoDict <- prepareProtoDict
      templateValueConverter <- extractTemplateValue(protoDict.templates)(templateId)
    } yield templateValueConverter.convert(jsonArgsValue)

  def contractArgFromProtoToJson(
      template: value.Identifier,
      protoArgs: value.Record,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[ujson.Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      dict <- prepareJsonDict
      templateValueConverter <- extractTemplateValue(dict.templates)(templateId)
    } yield templateValueConverter.convert(value.Value(value.Value.Sum.Record(protoArgs)))

  def choiceArgsFromJsonToProto(
      template: value.Identifier,
      choiceName: IdString.Name,
      jsonArgsValue: ujson.Value,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      protoDict <- prepareProtoDict
      templateValueConverter <- protoDict.choiceArguments
        .get(templateId -> choiceName)
        .toRight(invalidChoiceException(templateId, choiceName))
        .toFuture
    } yield templateValueConverter.convert(nullToEmpty(jsonArgsValue))

  def choiceArgsFromProtoToJson(
      template: value.Identifier,
      choiceName: IdString.Name,
      protoArgs: value.Value,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[ujson.Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      jsonDict <- prepareJsonDict
      choiceArgsConverter <- jsonDict.choiceArguments
        .get(templateId -> choiceName)
        .toRight(invalidChoiceException(templateId, choiceName))
        .toFuture
    } yield choiceArgsConverter.convert(protoArgs)

  def keyArgFromProtoToJson(
      template: value.Identifier,
      protoArgs: value.Value,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[ujson.Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      jsonDict <- prepareJsonDict
      templateValueConverter <- extractTemplateValue(jsonDict.templateKeys)(templateId)
    } yield templateValueConverter.convert(protoArgs)

  def keyArgFromJsonToProto(
      template: value.Identifier,
      protoArgs: ujson.Value,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      protoDict <- prepareProtoDict
      templateValueConverter <- extractTemplateValue(protoDict.templateKeys)(templateId)
    } yield templateValueConverter.convert(protoArgs)

  def exerciseResultFromProtoToJson(
      template: value.Identifier,
      choiceName: IdString.Name,
      v: value.Value,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[ujson.Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      jsonDict <- prepareJsonDict
      choiceResultConverter <- jsonDict.choiceResults
        .get(templateId -> choiceName)
        .toRight(invalidChoiceException(templateId, choiceName))
        .toFuture
    } yield choiceResultConverter.convert(v)

  def exerciseResultFromJsonToProto(
      template: Identifier,
      choiceName: IdString.Name,
      value: ujson.Value,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Option[Value]] = value match {
    case ujson.Null => Future(None)
    case _ =>
      for {
        templateId <- resolveIdentifier(template).toFuture
        protoDict <- prepareProtoDict
        choiceResultConverter <- protoDict.choiceResults
          .get(templateId -> choiceName)
          .toRight(invalidChoiceException(templateId, choiceName))
          .toFuture
      } yield Some(choiceResultConverter.convert(value))
  }

  private def invalidChoiceException(templateId: Ref.Identifier, choiceName: IdString.Name)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    InvalidArgument.Reject(s"Invalid template:$templateId or choice:$choiceName").asGrpcError

  private def resolveIdentifier(
      template: value.Identifier
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Identifier] =
    PackageRef
      .fromString(template.packageId)
      .left
      .map(err => invalidField("package reference", err))
      .flatMap {
        case PackageRef.Name(name) =>
          resolvePackageNameReference(name, template)
        case PackageRef.Id(id) => Right(template)
      }
      .map(IdentifierConverters.lfIdentifier)

  private def resolvePackageNameReference(
      packageName: String,
      template: Identifier,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, value.Identifier] =
    for {
      packageNameRef <- Ref.PackageName
        .fromString(packageName)
        .left
        .map(err => invalidField("package name", err))
      result <- fetchSignatures(contextualizedErrorLogger).view
        .filter(_._2.metadata.name == packageNameRef)
        .maxByOption { case (_pkgId, signature) => signature.metadata.version }
        .toRight(PackageNamesNotFound.Reject(Set(packageNameRef)).asGrpcError)
        .map { case (mostPreferredPackageId, _pkgSig) =>
          template.copy(packageId = mostPreferredPackageId)
        }
    } yield result

  private def prepareProtoDict(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Dictionary[Converter[ujson.Value, Value]]] =
    memoizedDictionaries(contextualizedErrorLogger).map(_._1)

  private def prepareJsonDict(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Dictionary[Converter[value.Value, ujson.Value]]] =
    memoizedDictionaries(contextualizedErrorLogger).map(_._2)

  private def computeProtoDict(
      signatures: PackageSignatures
  ): Dictionary[Converter[ujson.Value, Value]] = {
    val visitor: SchemaVisitor { type Type = (Codec[ujson.Value], Codec[value.Value]) } =
      SchemaVisitor.compose(new JsonCodec(), GrpcValueCodec)
    val collector =
      Dictionary.collect[Converter[ujson.Value, value.Value]] compose SchemaEntity
        .map((v: visitor.Type) => Converter(v._1, v._2))

    SchemaProcessor
      .process(packages = signatures)(visitor)(collector)
      .fold(error => throw new IllegalStateException(error), identity)
  }

  private def computeJsonDict(
      signatures: PackageSignatures
  ): Dictionary[Converter[Value, ujson.Value]] = {
    val visitor: SchemaVisitor { type Type = (Codec[value.Value], Codec[ujson.Value]) } =
      SchemaVisitor.compose(GrpcValueCodec, new JsonCodec())
    val collector =
      Dictionary.collect[Converter[value.Value, ujson.Value]] compose SchemaEntity
        .map((v: visitor.Type) => Converter(v._1, v._2))

    SchemaProcessor
      .process(packages = signatures)(visitor)(collector)
      .fold(error => throw new IllegalStateException(error), identity)
  }

  private val memoizedDictionaries: ContextualizedErrorLogger => Future[(ProtoDict, JsonDict)] = {
    val ref = new AtomicReference[Option[(PackageSignatures, Future[(ProtoDict, JsonDict)])]]()

    (contextualizedErrorLogger: ContextualizedErrorLogger) =>
      {
        ref.updateAndGet { curr =>
          val currentSignatures = fetchSignatures(contextualizedErrorLogger)
          curr match {
            case curr @ Some((signatures, _dictF))
                // Use identity instead to avoid deep equality comparison of the packageMeta
                if signatures eq currentSignatures =>
              curr
            case _other =>
              contextualizedErrorLogger.debug(
                "Package metadata view changed or not initialized. Recomputing converter dictionaries."
              )
              Some(
                currentSignatures -> Future {
                  computeProtoDict(currentSignatures) -> computeJsonDict(currentSignatures)
                }
                  // We allow "caching" a failed future since it's not clear that we can recover anyway
                  // We do not explicitly crash the participant to allow graceful degradation
                  // (gRPC API or other JSON endpoints could still be working fine)
                  .thereafter {
                    case Failure(error) =>
                      contextualizedErrorLogger.error(
                        "Failed to compute JSON API converter dictionaries. This is likely a programming error. Please contact support",
                        error,
                      )
                    case Success(_) => ()
                  }
              )
          }
        }
      }.map(_._2)
        .getOrElse(
          throw new IllegalStateException("Schema processor dictionary references not populated")
        )
  }

  private def nullToEmpty(jsonVal: ujson.Value): ujson.Value = jsonVal match {
    case ujson.Null => ujson.Obj()
    case any => any
  }

  private def extractTemplateValue[V](map: Map[Ref.Identifier, V])(key: Ref.Identifier)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[V] =
    map
      .get(key)
      .toRight(
        RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound
          .Reject(Seq(Left(key)))
          .asGrpcError
      )
      .toFuture
}

object SchemaProcessors {
  type JsonDict = Dictionary[Converter[Value, ujson.Value]]
  type ProtoDict = Dictionary[Converter[ujson.Value, Value]]
  type PackageSignatures = Map[Ref.PackageId, Ast.PackageSignature]
  implicit class ResultOps[T](val result: Either[StatusRuntimeException, T]) extends AnyVal {
    def toFuture: Future[T] = result match {
      case Left(error) => Future.failed(error)
      case Right(value) => Future.successful(value)
    }
  }
}
