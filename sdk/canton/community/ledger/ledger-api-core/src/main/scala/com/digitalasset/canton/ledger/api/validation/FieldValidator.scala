// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.implicits.toBifunctorOps
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.value.Identifier
import com.digitalasset.canton.ledger.api.validation.ResourceAnnotationValidator.{
  AnnotationsSizeExceededError,
  EmptyAnnotationsValueError,
  InvalidAnnotationsKeyError,
}
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.*
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.ledger.api.{IdentityProviderId, JwksUrl, SubmissionId, WorkflowId}
import com.digitalasset.canton.topology.{ParticipantId, PartyId as TopologyPartyId, SynchronizerId}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{Party, TypeConRef}
import com.digitalasset.daml.lf.value.Value.ContractId
import io.grpc.StatusRuntimeException

import scala.util.{Failure, Success, Try}

object FieldValidator {

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

  def requireTopologyPartyIdField(s: String, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, TopologyPartyId] = for {
    lf <- requirePartyField(s, fieldName)
    id <- TopologyPartyId
      .fromLfParty(lf)
      .leftMap(err => invalidField(fieldName = fieldName, message = err))
  } yield id

  def optionalParticipantId(participantId: String, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[ParticipantId]] = optionalString(participantId) { s =>
    ParticipantId
      .fromProtoPrimitive("PAR::" + s, fieldName)
      .left
      .map(err => invalidField(fieldName = fieldName, message = err.message))
  }

  def requireResourceVersion(raw: String, fieldName: String)(implicit
      errorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Long] =
    Try {
      raw.toLong
    } match {
      case Success(resourceVersionNumber) => Right(resourceVersionNumber)
      case Failure(_) =>
        Left(
          invalidField(fieldName = fieldName, message = "Invalid resource version number")
        )
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
  ): Either[StatusRuntimeException, Long] =
    Try {
      raw.toLong
    } match {
      case Success(seqId) => Right(seqId)
      case Failure(_) =>
        // Do not mention event sequential id as this should be opaque externally
        Left(invalidField(fieldName = fieldName, message))
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
  ): Either[StatusRuntimeException, IdentityProviderId] =
    if (s.isEmpty) Right(IdentityProviderId.Default)
    else
      IdentityProviderId.Id.fromString(s).left.map(invalidField(fieldName, _))

  def requireLedgerString(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.LedgerString] =
    Ref.LedgerString.fromString(s).left.map(invalidArgument)

  def validateWorkflowId(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[WorkflowId]] =
    if (s.isEmpty) Right(None)
    else requireLedgerString(s).map(x => Some(WorkflowId(x)))

  def validateSubmissionId(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[SubmissionId]] =
    optionalString(s) { nonEmptyString =>
      Ref.SubmissionId
        .fromString(nonEmptyString)
        .map(SubmissionId(_))
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

  def requireSynchronizerId(s: String, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, SynchronizerId] =
    if (s.isEmpty) Left(missingField(fieldName))
    else SynchronizerId.fromString(s).left.map(invalidField(fieldName, _))

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

  def validateTypeConRef(identifier: Identifier)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, TypeConRef] =
    for {
      qualifiedName <- validateTemplateQualifiedName(identifier.moduleName, identifier.entityName)
      pkgRef <- Ref.PackageRef
        .fromString(identifier.packageId)
        .left
        .map(invalidField("package reference", _))
    } yield Ref.TypeConRef(pkgRef, qualifiedName)

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
  ): Either[StatusRuntimeException, Map[String, String]] =
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

  def validateOptional[T, U](t: Option[T])(
      validation: T => Either[StatusRuntimeException, U]
  ): Either[StatusRuntimeException, Option[U]] =
    t.map(validation).map(_.map(Some(_))).getOrElse(Right(None))

  def requireOptional[T, U](t: Option[T], fieldName: String)(
      validation: T => Either[StatusRuntimeException, U]
  )(implicit
      errorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, U] =
    t.map(validation).getOrElse(Left(missingField(fieldName)))

}
