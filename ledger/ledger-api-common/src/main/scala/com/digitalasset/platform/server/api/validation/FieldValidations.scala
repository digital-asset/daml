// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.api.util.TimestampConversion
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.api.validation.ValidationErrors._
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{Bytes, Ref, Time}
import com.daml.lf.data.Ref.Party
import com.daml.lf.value.Value.ContractId
import com.daml.ledger.errors.LedgerApiErrors
import com.daml.platform.server.api.validation.ResourceAnnotationValidation.{
  AnnotationsSizeExceededError,
  EmptyAnnotationsValueError,
  InvalidAnnotationsKeyError,
}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import io.grpc.StatusRuntimeException

import scala.util.{Failure, Success, Try}

object FieldValidations {

  def matchLedgerId(
      ledgerId: LedgerId
  )(receivedO: Option[LedgerId])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[LedgerId]] = receivedO match {
    case None => Right(None)
    case Some(`ledgerId`) => Right(Some(ledgerId))
    case Some(mismatching) =>
      import scalaz.syntax.tag._
      Left(
        LedgerApiErrors.RequestValidation.LedgerIdMismatch
          .Reject(ledgerId.unwrap, mismatching.unwrap)
          .asGrpcError
      )
  }

  def requireNonEmptyString(s: String, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, String] =
    Either.cond(s.nonEmpty, s, missingField(fieldName))

  def requireIdentifier(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Name] =
    Ref.Name.fromString(s).left.map(invalidArgument)

  private def requireNonEmptyParsedId[T](parser: String => Either[String, T])(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, T] =
    if (s.isEmpty)
      Left(missingField(fieldName))
    else
      parser(s).left.map(invalidField(fieldName, _))

  def requireName(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Name] =
    requireNonEmptyParsedId(Ref.Name.fromString)(s, fieldName)

  def requirePackageId(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.PackageId] =
    requireNonEmptyParsedId(Ref.PackageId.fromString)(s, fieldName)

  def requireParty(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Party] =
    Ref.Party.fromString(s).left.map(invalidArgument)

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

  def requireContractId(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ContractId] =
    if (s.isEmpty) Left(missingField(fieldName))
    else ContractId.fromString(s).left.map(invalidField(fieldName, _))

  def requireDottedName(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.DottedName] =
    Ref.DottedName.fromString(s).left.map(invalidField(fieldName, _))

  def requireNonEmpty[M[_] <: Iterable[_], T](
      s: M[T],
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, M[T]] =
    if (s.nonEmpty) Right(s)
    else Left(missingField(fieldName))

  def requirePresence[T](option: Option[T], fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, T] =
    option.fold[Either[StatusRuntimeException, T]](
      Left(missingField(fieldName))
    )(Right(_))

  def validateIdentifier(identifier: Identifier)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Identifier] =
    for {
      packageId <- requirePackageId(identifier.packageId, "package_id")
      mn <- requireDottedName(identifier.moduleName, "module_name")
      en <- requireDottedName(identifier.entityName, "entity_name")
    } yield Ref.Identifier(packageId, Ref.QualifiedName(mn, en))

  def optionalString[T](s: String)(
      someValidation: String => Either[StatusRuntimeException, T]
  ): Either[StatusRuntimeException, Option[T]] =
    if (s.isEmpty) Right(None)
    else someValidation(s).map(Option(_))

  def validateHash(
      value: ByteString,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[Hash]] =
    if (value.isEmpty) {
      Right(None)
    } else {
      val bytes = Bytes.fromByteString(value)
      Hash
        .fromBytes(bytes)
        .map(Some(_))
        .left
        .map(invalidField(fieldName, _))
    }

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
    ResourceAnnotationValidation.validateAnnotationsFromApiRequest(
      annotations,
      allowEmptyValues = allowEmptyValues,
    ) match {
      case Left(AnnotationsSizeExceededError) =>
        Left(
          invalidArgument(
            s"annotations from field '$fieldName' are larger than the limit of ${ResourceAnnotationValidation.MaxAnnotationsSizeInKiloBytes}kb"
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
}
