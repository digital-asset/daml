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
import com.digitalasset.transcode.codec.proto.ProtobufCodec
import com.digitalasset.transcode.daml_lf.LfSchemaProcessor
import com.digitalasset.transcode.schema.{Dictionary, IdentifierFilter, SchemaVisitor}
import com.digitalasset.transcode.{Converter, schema}
import io.grpc.StatusRuntimeException

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class SchemaProcessorsImpl(
    fetchSignatures: ErrorLoggingContext => PackageSignatures,
    val loggerFactory: NamedLoggerFactory,
    // used in testing to create JSON requests with missing arguments
    allowMissingFields: Boolean = false,
)(implicit executionContext: ExecutionContext)
    extends SchemaProcessors
    with NamedLogging {

  override def contractArgFromJsonToProto(
      template: value.Identifier,
      jsonArgsValue: ujson.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[value.Value] = {
    val signatures = fetchSignatures(errorLoggingContext)
    for {
      templateId <- resolveIdentifier(template, signatures).toFuture
      protoDict <- prepareProtoDict(signatures)
      templateValueConverter <- extractTemplateValue(protoDict)(templateId)
    } yield templateValueConverter.convert(jsonArgsValue)
  }

  override def contractArgFromProtoToJson(
      template: value.Identifier,
      protoArgs: value.Record,
  )(implicit
      traceContext: TraceContext
  ): Future[ujson.Value] = {
    val signatures = fetchSignatures(errorLoggingContext)
    for {
      templateId <- resolveIdentifier(template, signatures).toFuture
      jsonDict <- prepareJsonDict(signatures)
      templateValueConverter <- extractTemplateValue(jsonDict)(templateId)
    } yield templateValueConverter.convert(value.Value(value.Value.Sum.Record(protoArgs)))
  }

  override def choiceArgsFromJsonToProto(
      template: value.Identifier,
      choiceName: IdString.Name,
      jsonArgsValue: ujson.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[value.Value] = {
    val signatures = fetchSignatures(errorLoggingContext)
    for {
      templateId <- resolveIdentifier(template, signatures).toFuture
      protoDict <- prepareProtoDict(signatures)
      choiceArgsConverter <- extractChoiceArgument(protoDict)(templateId, choiceName)
    } yield choiceArgsConverter.convert(nullToEmpty(jsonArgsValue))
  }

  override def choiceArgsFromProtoToJson(
      template: value.Identifier,
      choiceName: IdString.Name,
      protoArgs: value.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[ujson.Value] = {
    val signatures = fetchSignatures(errorLoggingContext)
    for {
      templateId <- resolveIdentifier(template, signatures).toFuture
      jsonDict <- prepareJsonDict(signatures)
      choiceArgsConverter <- extractChoiceArgument(jsonDict)(templateId, choiceName)
    } yield choiceArgsConverter.convert(protoArgs)
  }

  override def keyArgFromProtoToJson(
      template: value.Identifier,
      protoArgs: value.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[ujson.Value] = {
    val signatures = fetchSignatures(errorLoggingContext)
    for {
      templateId <- resolveIdentifier(template, signatures).toFuture
      jsonDict <- prepareJsonDict(signatures)
      templateKeyConverter <- extractTemplateKey(jsonDict)(templateId)
    } yield templateKeyConverter.convert(protoArgs)
  }

  override def keyArgFromJsonToProto(
      template: value.Identifier,
      jsonArgs: ujson.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[value.Value] = {
    val signatures = fetchSignatures(errorLoggingContext)
    for {
      templateId <- resolveIdentifier(template, signatures).toFuture
      protoDict <- prepareProtoDict(signatures)
      templateKeyConverter <- extractTemplateKey(protoDict)(templateId)
    } yield templateKeyConverter.convert(jsonArgs)
  }

  override def exerciseResultFromProtoToJson(
      template: value.Identifier,
      choiceName: IdString.Name,
      v: value.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[ujson.Value] = {
    val signatures = fetchSignatures(errorLoggingContext)
    for {
      templateId <- resolveIdentifier(template, signatures).toFuture
      jsonDict <- prepareJsonDict(signatures)
      choiceResultConverter <- extractChoiceResult(jsonDict)(templateId, choiceName)
    } yield choiceResultConverter.convert(v)
  }

  override def exerciseResultFromJsonToProto(
      template: value.Identifier,
      choiceName: IdString.Name,
      jvalue: ujson.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[value.Value]] = {
    val signatures = fetchSignatures(errorLoggingContext)
    for {
      templateId <- resolveIdentifier(template, signatures).toFuture
      protoDict <- prepareProtoDict(signatures)
      choiceResultConverter <- extractChoiceResult(protoDict)(templateId, choiceName)
    } yield Some(choiceResultConverter.convert(jvalue))
  }

  private def invalidChoiceException(templateId: schema.Identifier, choiceName: IdString.Name)(
      implicit traceContext: TraceContext
  ): StatusRuntimeException =
    InvalidArgument
      .Reject(s"Invalid template:${lfIdentifier(templateId)} or choice:$choiceName")
      .asGrpcError

  private def resolveIdentifier(
      template: value.Identifier,
      signatures: PackageSignatures,
  )(implicit
      traceContext: TraceContext
  ): Either[StatusRuntimeException, schema.Identifier] =
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
        case PackageRef.Id(packageId) =>
          signatures
            .get(packageId)
            .toRight(
              RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound
                .Reject(Seq(Left(lfIdentifier(template))))
                .asGrpcError
            )
            .map { pkg =>
              schema.Identifier(
                schema.PackageId(packageId),
                schema.PackageName(pkg.metadata.name),
                schema.PackageVersion(pkg.metadata.version.toString),
                schema.ModuleName(template.moduleName),
                schema.EntityName(template.entityName),
              )
            }
      }

  private def prepareProtoDict(signatures: PackageSignatures)(implicit
      traceContext: TraceContext
  ): Future[Dictionary[Converter[ujson.Value, value.Value]]] =
    prepareDictionaries(signatures).map(_._1)

  private def prepareJsonDict(signatures: PackageSignatures)(implicit
      traceContext: TraceContext
  ): Future[Dictionary[Converter[value.Value, ujson.Value]]] =
    prepareDictionaries(signatures).map(_._2)

  private val memoizedDictionaries =
    new AtomicReference[(PackageSignatures, Future[(ProtoDict, JsonDict)])]()
  private def prepareDictionaries(
      signatures: PackageSignatures
  )(implicit traceContext: TraceContext): Future[(ProtoDict, JsonDict)] =
    memoizedDictionaries.updateAndGet { prev =>
      prev match {
        // Use identity instead to avoid deep equality comparison of the packageMeta
        case prev @ (prevSigs, _) if prevSigs eq signatures => prev
        case _ =>
          errorLoggingContext.debug(
            "Package metadata view changed or not initialized. Recomputing converter dictionaries."
          )
          signatures -> Future(computeDictionaries(signatures))
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
      }
    }._2

  private def computeDictionaries(
      signatures: PackageSignatures
  ): (
      Dictionary[Converter[ujson.Value, value.Value]],
      Dictionary[Converter[value.Value, ujson.Value]],
  ) = {
    val jsonCodec =
      new JsonCodec(allowMissingFields = allowMissingFields, removeTrailingNonesInRecords = true)
    val visitor = SchemaVisitor.compose(jsonCodec, ProtobufCodec)

    val (jsonDic, protoDic) = LfSchemaProcessor
      .process(packages = signatures, IdentifierFilter.AcceptAll)(visitor)
      .fold(error => throw new IllegalStateException(error), identity)
    (
      jsonDic.useStrictPackageMatching(true).zipWith(protoDic)(Converter(_, _)),
      protoDic.useStrictPackageMatching(true).zipWith(jsonDic)(Converter(_, _)),
    )
  }

  private def nullToEmpty(jsonVal: ujson.Value): ujson.Value = jsonVal match {
    case ujson.Null => ujson.Obj()
    case any => any
  }

  private def extractTemplateValue[V](dict: Dictionary[V])(templateId: schema.Identifier)(implicit
      traceContext: TraceContext
  ): Future[V] =
    dict
      .getTemplate(templateId)
      .toRight(
        RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound
          .Reject(Seq(Left(lfIdentifier(templateId))))
          .asGrpcError
      )
      .toFuture

  private def extractTemplateKey[V](dict: Dictionary[V])(templateId: schema.Identifier)(implicit
      traceContext: TraceContext
  ): Future[V] =
    dict
      .getTemplateKey(templateId)
      .toRight(
        RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound
          .Reject(Seq(Left(lfIdentifier(templateId))))
          .asGrpcError
      )
      .toFuture

  private def extractChoiceArgument[V](
      dict: Dictionary[V]
  )(templateId: schema.Identifier, choiceName: IdString.Name)(implicit
      traceContext: TraceContext
  ): Future[V] =
    dict
      .getChoiceArgument(templateId, schema.ChoiceName(choiceName))
      .toRight(invalidChoiceException(templateId, choiceName))
      .toFuture

  private def extractChoiceResult[V](
      dict: Dictionary[V]
  )(templateId: schema.Identifier, choiceName: IdString.Name)(implicit
      traceContext: TraceContext
  ): Future[V] =
    dict
      .getChoiceResult(templateId, schema.ChoiceName(choiceName))
      .toRight(invalidChoiceException(templateId, choiceName))
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

  def lfIdentifier(a: schema.Identifier): Ref.Identifier =
    Ref.Identifier(
      pkg = PackageId.assertFromString(a.packageId),
      qualifiedName = QualifiedName(
        module = ModuleName.assertFromString(a.moduleName),
        name = DottedName.assertFromString(a.entityName),
      ),
    )

  def lfIdentifier(a: value.Identifier): Ref.Identifier =
    Ref.Identifier(
      pkg = PackageId.assertFromString(a.packageId),
      qualifiedName = QualifiedName(
        module = ModuleName.assertFromString(a.moduleName),
        name = DottedName.assertFromString(a.entityName),
      ),
    )
}
