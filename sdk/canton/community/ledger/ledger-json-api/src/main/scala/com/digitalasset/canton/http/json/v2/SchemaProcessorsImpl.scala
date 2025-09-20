// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.value
import com.digitalasset.canton.http.json.v2.SchemaProcessorsImpl.*
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidField
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterAsyncOps
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{
  DottedName,
  IdString,
  ModuleName,
  PackageId,
  PackageRef,
  QualifiedName,
}
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

class SchemaProcessorsImpl(
    fetchSignatures: ErrorLoggingContext => PackageSignatures,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SchemaProcessors
    with NamedLogging {

  override def contractArgFromJsonToProto(
      template: value.Identifier,
      jsonArgsValue: ujson.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[value.Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      protoDict <- prepareProtoDict
      templateValueConverter <- extractTemplateValue(protoDict.templates)(templateId)
    } yield templateValueConverter.convert(jsonArgsValue)

  override def contractArgFromProtoToJson(
      template: value.Identifier,
      protoArgs: value.Record,
  )(implicit
      traceContext: TraceContext
  ): Future[ujson.Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      dict <- prepareJsonDict
      templateValueConverter <- extractTemplateValue(dict.templates)(templateId)
    } yield templateValueConverter.convert(value.Value(value.Value.Sum.Record(protoArgs)))

  override def choiceArgsFromJsonToProto(
      template: value.Identifier,
      choiceName: IdString.Name,
      jsonArgsValue: ujson.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[value.Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      protoDict <- prepareProtoDict
      templateValueConverter <- protoDict.choiceArguments
        .get(templateId -> choiceName)
        .toRight(invalidChoiceException(templateId, choiceName))
        .toFuture
    } yield templateValueConverter.convert(nullToEmpty(jsonArgsValue))

  override def choiceArgsFromProtoToJson(
      template: value.Identifier,
      choiceName: IdString.Name,
      protoArgs: value.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[ujson.Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      jsonDict <- prepareJsonDict
      choiceArgsConverter <- jsonDict.choiceArguments
        .get(templateId -> choiceName)
        .toRight(invalidChoiceException(templateId, choiceName))
        .toFuture
    } yield choiceArgsConverter.convert(protoArgs)

  override def keyArgFromProtoToJson(
      template: value.Identifier,
      protoArgs: value.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[ujson.Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      jsonDict <- prepareJsonDict
      templateValueConverter <- extractTemplateValue(jsonDict.templateKeys)(templateId)
    } yield templateValueConverter.convert(protoArgs)

  override def keyArgFromJsonToProto(
      template: value.Identifier,
      protoArgs: ujson.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[value.Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      protoDict <- prepareProtoDict
      templateValueConverter <- extractTemplateValue(protoDict.templateKeys)(templateId)
    } yield templateValueConverter.convert(protoArgs)

  override def exerciseResultFromProtoToJson(
      template: value.Identifier,
      choiceName: IdString.Name,
      v: value.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[ujson.Value] =
    for {
      templateId <- resolveIdentifier(template).toFuture
      jsonDict <- prepareJsonDict
      choiceResultConverter <- jsonDict.choiceResults
        .get(templateId -> choiceName)
        .toRight(invalidChoiceException(templateId, choiceName))
        .toFuture
    } yield choiceResultConverter.convert(v)

  override def exerciseResultFromJsonToProto(
      template: value.Identifier,
      choiceName: IdString.Name,
      jvalue: ujson.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[value.Value]] = jvalue match {
    case ujson.Null => Future(None)
    case _ =>
      for {
        templateId <- resolveIdentifier(template).toFuture
        protoDict <- prepareProtoDict
        choiceResultConverter <- protoDict.choiceResults
          .get(templateId -> choiceName)
          .toRight(invalidChoiceException(templateId, choiceName))
          .toFuture
      } yield Some(choiceResultConverter.convert(jvalue))
  }

  private def invalidChoiceException(templateId: Ref.Identifier, choiceName: IdString.Name)(implicit
      traceContext: TraceContext
  ): StatusRuntimeException =
    InvalidArgument.Reject(s"Invalid template:$templateId or choice:$choiceName").asGrpcError

  private def resolveIdentifier(
      template: value.Identifier
  )(implicit
      traceContext: TraceContext
  ): Either[StatusRuntimeException, Ref.Identifier] =
    PackageRef
      .fromString(template.packageId)
      .left
      .map(err => invalidField("package reference", err))
      .flatMap {
        case PackageRef.Name(_) =>
          Left(
            LedgerApiErrors.InternalError
              .Generic(
                s"Unexpected package-name format for identifier $template, expected package id. This is likely a programming error. Please contact support"
              )
              .asGrpcError
          )
        case PackageRef.Id(_) => Right(template)
      }
      .map(lfIdentifier)

  private def prepareProtoDict(implicit
      traceContext: TraceContext
  ): Future[Dictionary[Converter[ujson.Value, value.Value]]] =
    memoizedDictionaries(errorLoggingContext).map(_._1)

  private def prepareJsonDict(implicit
      traceContext: TraceContext
  ): Future[Dictionary[Converter[value.Value, ujson.Value]]] =
    memoizedDictionaries(errorLoggingContext).map(_._2)

  private def computeProtoDict(
      signatures: PackageSignatures
  ): Dictionary[Converter[ujson.Value, value.Value]] = {
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
  ): Dictionary[Converter[value.Value, ujson.Value]] = {
    val visitor: SchemaVisitor { type Type = (Codec[value.Value], Codec[ujson.Value]) } =
      SchemaVisitor.compose(GrpcValueCodec, new JsonCodec())
    val collector =
      Dictionary.collect[Converter[value.Value, ujson.Value]] compose SchemaEntity
        .map((v: visitor.Type) => Converter(v._1, v._2))

    SchemaProcessor
      .process(packages = signatures)(visitor)(collector)
      .fold(error => throw new IllegalStateException(error), identity)
  }

  private val memoizedDictionaries: ErrorLoggingContext => Future[(ProtoDict, JsonDict)] = {
    val ref = new AtomicReference[Option[(PackageSignatures, Future[(ProtoDict, JsonDict)])]]()

    (errorLoggingContext: ErrorLoggingContext) =>
      {
        ref.updateAndGet { curr =>
          val currentSignatures = fetchSignatures(errorLoggingContext)
          curr match {
            case curr @ Some((signatures, _dictF))
                // Use identity instead to avoid deep equality comparison of the packageMeta
                if signatures eq currentSignatures =>
              curr
            case _other =>
              errorLoggingContext.debug(
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
                      errorLoggingContext.error(
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
      traceContext: TraceContext
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

object SchemaProcessorsImpl {
  type JsonDict = Dictionary[Converter[value.Value, ujson.Value]]
  type ProtoDict = Dictionary[Converter[ujson.Value, value.Value]]
  type PackageSignatures = Map[Ref.PackageId, Ast.PackageSignature]
  implicit class ResultOps[T](val result: Either[StatusRuntimeException, T]) extends AnyVal {
    def toFuture: Future[T] = result match {
      case Left(error) => Future.failed(error)
      case Right(value) => Future.successful(value)
    }
  }

  def lfIdentifier(a: value.Identifier): Ref.Identifier =
    Ref.Identifier(
      pkg = PackageId.assertFromString(a.packageId),
      qualifiedName = QualifiedName(
        module = ModuleName.assertFromString(a.moduleName),
        name = DottedName.assertFromString(a.entityName),
      ),
    )
}
