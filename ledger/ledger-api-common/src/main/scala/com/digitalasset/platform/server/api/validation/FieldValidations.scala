// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import java.nio.charset.StandardCharsets

import com.daml.error.ContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.api.validation.ValidationErrors._
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.value.Value.ContractId
import io.grpc.StatusRuntimeException

import scala.util.matching.Regex

object FieldValidations {

  val MaxAnnotationsSizeInBytes: Int = 256 * 1024

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

  def requireEmptyString(s: String, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, String] =
    Either.cond(s.isEmpty, s, invalidArgument(s"field $fieldName must be not set"))

  def verifyMetadataAnnotations(annotations: Map[String, String], fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Map[String, String]] = {
    verifyMetadataAnnotations2(annotations) match {
      case Left(ExceededAnnotationsSizeError(actual)) =>
        Left(
          invalidArgument(
            s"annotations from field $fieldName are larger than the limit of 256kb, actual size: $actual bytes"
          )
        )
      case Left(InvalidAnnotationsKeySyntaxError()) => Left(invalidArgument(s"invalid key syntax"))
      case Right(_) => Right(annotations)
    }
  }

  sealed trait MetadataAnnotationsError
  final case class ExceededAnnotationsSizeError(actualSizeInBytes: Long)
      extends MetadataAnnotationsError
  final case class InvalidAnnotationsKeySyntaxError() extends MetadataAnnotationsError

  // TODO pbatko: Cleanup impl
  def verifyMetadataAnnotations2(
      annotations: Map[String, String]
  ): Either[MetadataAnnotationsError, Unit] = {
    val totalSize = annotations.iterator.foldLeft(0L) { case (size, (key, value)) =>
      val keySize = key.getBytes(StandardCharsets.UTF_8).length
      val valSize = value.getBytes(StandardCharsets.UTF_8).length
      size + keySize + valSize
    }
    if (totalSize > MaxAnnotationsSizeInBytes) {
      Left(ExceededAnnotationsSizeError(actualSizeInBytes = totalSize))
    } else {
      if (!annotations.keys.forall(isValidKey)) {
        Left(InvalidAnnotationsKeySyntaxError())
      } else {
        Right(())
      }

    }
  }

  // Based on K8s annotations and labels
  val NamePattern = "([a-zA-Z0-9]+[a-zA-Z0-9-]*)?[a-zA-Z0-9]+"
  val AnnotationsKeyRegex: Regex = "^([a-zA-Z0-9]+[a-zA-Z0-9.\\-_]*)?[a-zA-Z0-9]+$".r
  val DnsSubdomainRegex: Regex = ("^(" + NamePattern + "[.])*" + NamePattern + "$").r

  def isValidKey(v: String): Boolean = {
    v.split('/') match {
      case Array(name) => isValidKeyNameSegment(name)
      case Array(prefix, name) =>
        isValidKeyPrefixSegment(prefix) && isValidKeyNameSegment(name)
      case _ => false
    }
  }

  def isValidKeyPrefixSegment(v: String): Boolean = {
    if (v.length > 253) {
      false
    } else {
      DnsSubdomainRegex.matches(v)
    }
  }

  def isValidKeyNameSegment(v: String): Boolean = {
    if (v.length > 63) {
      false
    } else {
      AnnotationsKeyRegex.matches(v)
    }
  }

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

  def requirePresence2[A, B](s: Option[A], fieldName: String)(
      someValidation: A => Either[StatusRuntimeException, B]
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, B] = {
    s match {
      case None => Left(missingField(fieldName))
      case Some(v) => someValidation(v)
    }
  }

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

}
