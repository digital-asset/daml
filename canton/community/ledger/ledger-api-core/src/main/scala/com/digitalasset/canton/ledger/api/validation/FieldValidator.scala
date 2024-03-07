// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.value.Identifier
import com.daml.lf.data.Ref.{PackageRef, Party, TypeConRef}
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{
  IdentityProviderId,
  JwksUrl,
  LedgerId,
  TemplateFilter,
}
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.ledger.api.validation.ResourceAnnotationValidator.{
  AnnotationsSizeExceededError,
  EmptyAnnotationsValueError,
  InvalidAnnotationsKeyError,
}
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.*
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.topology.DomainId
import com.google.protobuf.timestamp.Timestamp
import io.grpc.StatusRuntimeException

import scala.util.{Failure, Success, Try}

object FieldValidator {
  def matchLedgerId(
      ledgerId: LedgerId
  )(receivedO: Option[LedgerId])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[LedgerId]] = receivedO match {
    case None => Right(None)
    case Some(`ledgerId`) => Right(Some(ledgerId))
    case Some(mismatching) =>
      import scalaz.syntax.tag.*
      Left(
        RequestValidationErrors.LedgerIdMismatch
          .Reject(ledgerId.unwrap, mismatching.unwrap)
          .asGrpcError
      )
  }

  def requireNonEmptyString(s: String, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, String] =
    Either.cond(s.nonEmpty, s, missingField(fieldName))

  def requireParty(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Party] =
    Ref.Party.fromString(s).left.map(invalidArgument)

  def requirePartyField(s: String, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Party] =
    Ref.Party.fromString(s).left.map(invalidField(fieldName, _))

  def requireResourceVersion(raw: String, fieldName: String)(implicit
      errorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Long] = {
    Try {
      raw.toLong
    } match {
      case Success(resourceVersionNumber) => Right(resourceVersionNumber)
      case Failure(_) =>
        Left(
          invalidField(fieldName = fieldName, message = "Invalid resource version number")
        )
    }
  }

  def requireJwksUrl(raw: String, fieldName: String)(implicit
      errorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, JwksUrl] =
    for {
      _ <- requireNonEmptyString(raw, fieldName)
      value <- JwksUrl.fromString(raw).left.map { error =>
        invalidField(fieldName = fieldName, message = s"Malformed URL: $error")
      }
    } yield value

  def requireParties(parties: Set[String])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Set[Party]] =
    parties.foldLeft[Either[StatusRuntimeException, Set[Party]]](Right(Set.empty)) {
      (acc, partyTxt) =>
        for {
          parties <- acc
          party <- requireParty(partyTxt)
        } yield parties + party
    }

  def requireUserId(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.UserId] =
    requireNonEmptyParsedId(Ref.UserId.fromString)(s, fieldName)

  def requireApplicationId(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.ApplicationId] =
    requireNonEmptyParsedId(Ref.ApplicationId.fromString)(s, fieldName)

  def requireLedgerString(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.LedgerString] =
    requireNonEmptyParsedId(Ref.LedgerString.fromString)(s, fieldName)

  def eventSequentialId(raw: String, fieldName: String, message: String)(implicit
      errorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Long] = {
    Try {
      raw.toLong
    } match {
      case Success(seqId) => Right(seqId)
      case Failure(_) =>
        // Do not mention event sequential id as this should be opaque externally
        Left(invalidField(fieldName = fieldName, message))
    }
  }

  def optionalEventSequentialId(
      s: String,
      fieldName: String,
      message: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[Long]] = optionalString(s) { s =>
    eventSequentialId(s, fieldName, message)
  }

  def requireIdentityProviderId(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, IdentityProviderId.Id] =
    for {
      _ <- requireNonEmptyString(s, fieldName)
      value <- IdentityProviderId.Id.fromString(s).left.map(invalidField(fieldName, _))
    } yield value

  def optionalIdentityProviderId(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, IdentityProviderId] = {
    if (s.isEmpty) Right(IdentityProviderId.Default)
    else
      IdentityProviderId.Id.fromString(s).left.map(invalidField(fieldName, _))
  }

  def requireLedgerString(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.LedgerString] =
    Ref.LedgerString.fromString(s).left.map(invalidArgument)

  def validateSubmissionId(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[domain.SubmissionId]] =
    optionalString(s) { nonEmptyString =>
      Ref.SubmissionId
        .fromString(nonEmptyString)
        .map(domain.SubmissionId(_))
        .left
        .map(invalidField("submission_id", _))
    }

  def requireSubmissionId(s: String, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.SubmissionId] =
    Ref.SubmissionId
      .fromString(s)
      .left
      .map(invalidField(fieldName, _))

  def requireCommandId(s: String, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.CommandId] =
    Ref.CommandId
      .fromString(s)
      .left
      .map(invalidField(fieldName, _))

  def requireWorkflowId(s: String, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.WorkflowId] =
    Ref.WorkflowId
      .fromString(s)
      .left
      .map(invalidField(fieldName, _))

  def requireDomainId(s: String, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, DomainId] =
    DomainId
      .fromString(s)
      .left
      .map(invalidField(fieldName, _))

  def requireContractId(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ContractId] =
    if (s.isEmpty) Left(missingField(fieldName))
    else ContractId.fromString(s).left.map(invalidField(fieldName, _))

  def requireNonEmpty[M[_] <: Iterable[_], T](
      s: M[T],
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, M[T]] =
    if (s.nonEmpty) Right(s)
    else Left(missingField(fieldName))

  def validateTypeConRef(identifier: Identifier)(upgradingEnabled: Boolean)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, TypeConRef] =
    for {
      qualifiedName <- validateTemplateQualifiedName(identifier.moduleName, identifier.entityName)
      pkgRef <- Ref.PackageRef
        .fromString(identifier.packageId)
        .left
        .map(invalidField("package reference", _))
      _ <- pkgRef match {
        case PackageRef.Name(_) if !upgradingEnabled =>
          Left(
            invalidArgument(
              "package-name scoping for requests is only possible when smart contract upgrading feature is enabled"
            )
          )
        case _ => Right(())
      }
    } yield Ref.TypeConRef(pkgRef, qualifiedName)

  def validateIdentifierWithPackageUpgrading(
      identifier: Identifier,
      includeCreatedEventBlob: Boolean,
  )(upgradingEnabled: Boolean)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, TemplateFilter] =
    for {
      typeRef <- validateTypeConRef(identifier)(upgradingEnabled)
      templateFilter = TemplateFilter(
        templateTypeRef = typeRef,
        includeCreatedEventBlob = includeCreatedEventBlob,
      )
    } yield templateFilter

  def optionalString[T](s: String)(
      someValidation: String => Either[StatusRuntimeException, T]
  ): Either[StatusRuntimeException, Option[T]] =
    if (s.isEmpty) Right(None)
    else someValidation(s).map(Option(_))

  def requireEmptyString(s: String, fieldName: String)(implicit
      errorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, String] =
    Either.cond(s.isEmpty, s, invalidArgument(s"field $fieldName must be not set"))

  def verifyMetadataAnnotations(
      annotations: Map[String, String],
      allowEmptyValues: Boolean,
      fieldName: String,
  )(implicit
      errorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Map[String, String]] = {
    ResourceAnnotationValidator.validateAnnotationsFromApiRequest(
      annotations,
      allowEmptyValues = allowEmptyValues,
    ) match {
      case Left(AnnotationsSizeExceededError) =>
        Left(
          invalidArgument(
            s"annotations from field '$fieldName' are larger than the limit of ${ResourceAnnotationValidator.MaxAnnotationsSizeInKiloBytes}kb"
          )
        )
      case Left(e: InvalidAnnotationsKeyError) => Left(invalidArgument(e.reason))
      case Left(e: EmptyAnnotationsValueError) => Left(invalidArgument(e.reason))
      case Right(_) => Right(annotations)
    }
  }

  def validateTimestamp(timestamp: Timestamp, fieldName: String)(implicit
      errorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Time.Timestamp] =
    Try(
      TimestampConversion
        .toLf(
          protoTimestamp = timestamp,
          mode = TimestampConversion.ConversionMode.Exact,
        )
    ).toEither.left
      .map(errMsg =>
        invalidArgument(
          s"Can not represent $fieldName ($timestamp) as a Daml timestamp: ${errMsg.getMessage}"
        )
      )

  def validateOptional[T, U](t: Option[T])(
      validation: T => Either[StatusRuntimeException, U]
  ): Either[StatusRuntimeException, Option[U]] =
    t.map(validation).map(_.map(Some(_))).getOrElse(Right(None))

}
