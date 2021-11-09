// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import java.time.Duration

import com.daml.api.util.DurationConversion
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.commands.Commands.{DeduplicationPeriod => DeduplicationPeriodProto}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.api.{DeduplicationPeriod, domain}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.value.Value.ContractId
import com.google.protobuf.duration.{Duration => DurationProto}
import io.grpc.StatusRuntimeException

// TODO error codes: Remove default usage of ErrorFactories
class FieldValidations private (errorFactories: ErrorFactories) {
  import errorFactories._

  def matchLedgerId(
      ledgerId: LedgerId
  )(received: LedgerId)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, LedgerId] =
    if (ledgerId == received) Right(received)
    else Left(ledgerIdMismatch(ledgerId, received, definiteAnswer = Some(false)))

  def requireNonEmptyString(s: String, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, String] =
    Either.cond(s.nonEmpty, s, missingField(fieldName, definiteAnswer = Some(false)))

  def requireIdentifier(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Name] =
    Ref.Name.fromString(s).left.map(invalidArgument(definiteAnswer = Some(false)))

  def requireName(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Name] =
    if (s.isEmpty)
      Left(missingField(fieldName, definiteAnswer = Some(false)))
    else
      Ref.Name.fromString(s).left.map(invalidField(fieldName, _, definiteAnswer = Some(false)))

  def requirePackageId(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.PackageId] =
    if (s.isEmpty) Left(missingField(fieldName, definiteAnswer = Some(false)))
    else
      Ref.PackageId.fromString(s).left.map(invalidField(fieldName, _, definiteAnswer = Some(false)))

  def requireParty(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Party] =
    Ref.Party.fromString(s).left.map(invalidArgument(definiteAnswer = Some(false)))

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

  def requireLedgerString(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.LedgerString] =
    if (s.isEmpty) Left(missingField(fieldName, definiteAnswer = Some(false)))
    else
      Ref.LedgerString
        .fromString(s)
        .left
        .map(invalidField(fieldName, _, definiteAnswer = Some(false)))

  def requireLedgerString(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.LedgerString] =
    Ref.LedgerString.fromString(s).left.map(invalidArgument(definiteAnswer = Some(false)))

  def validateSubmissionId(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[domain.SubmissionId]] =
    if (s.isEmpty) {
      Right(None)
    } else {
      Ref.SubmissionId
        .fromString(s)
        .map(submissionId => Some(domain.SubmissionId(submissionId)))
        .left
        .map(invalidField("submission_id", _, definiteAnswer = Some(false)))
    }

  def requireContractId(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ContractId] =
    if (s.isEmpty) Left(missingField(fieldName, definiteAnswer = Some(false)))
    else ContractId.fromString(s).left.map(invalidField(fieldName, _, definiteAnswer = Some(false)))

  def requireDottedName(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.DottedName] =
    Ref.DottedName.fromString(s).left.map(invalidField(fieldName, _, definiteAnswer = Some(false)))

  def requireNonEmpty[M[_] <: Iterable[_], T](
      s: M[T],
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, M[T]] =
    if (s.nonEmpty) Right(s)
    else Left(missingField(fieldName, definiteAnswer = Some(false)))

  def requirePresence[T](option: Option[T], fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, T] =
    option.fold[Either[StatusRuntimeException, T]](
      Left(missingField(fieldName, definiteAnswer = Some(false)))
    )(Right(_))

  /** We validate only using current time because we set the currentTime as submitTime so no need to check both
    */
  def validateDeduplicationPeriod(
      deduplicationPeriod: DeduplicationPeriodProto,
      optMaxDeduplicationDuration: Option[Duration],
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, DeduplicationPeriod] = {

    optMaxDeduplicationDuration.fold[Either[StatusRuntimeException, DeduplicationPeriod]](
      Left(missingLedgerConfig(definiteAnswer = Some(false)))
    )(maxDeduplicationDuration => {
      def validateDuration(duration: Duration, exceedsMaxDurationMessage: String) = {
        if (duration.isNegative)
          Left(invalidField(fieldName, "Duration must be positive", definiteAnswer = Some(false)))
        else if (duration.compareTo(maxDeduplicationDuration) > 0)
          Left(
            invalidDeduplicationDuration(
              fieldName,
              exceedsMaxDurationMessage,
              definiteAnswer = Some(false),
              maxDeduplicationDuration,
            )
          )
        else Right(duration)
      }

      def protoDurationToDurationPeriod(duration: DurationProto) = {
        val result = DurationConversion.fromProto(duration)
        validateDuration(
          result,
          s"The given deduplication duration of $result exceeds the maximum deduplication time of $maxDeduplicationDuration",
        ).map(DeduplicationPeriod.DeduplicationDuration)
      }

      deduplicationPeriod match {
        case DeduplicationPeriodProto.Empty =>
          Right(DeduplicationPeriod.DeduplicationDuration(maxDeduplicationDuration))
        case DeduplicationPeriodProto.DeduplicationTime(duration) =>
          protoDurationToDurationPeriod(duration)
        case DeduplicationPeriodProto.DeduplicationDuration(duration) =>
          protoDurationToDurationPeriod(duration)
      }
    })
  }

  def validateIdentifier(identifier: Identifier)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Identifier] =
    for {
      packageId <- requirePackageId(identifier.packageId, "package_id")
      mn <- requireDottedName(identifier.moduleName, "module_name")
      en <- requireDottedName(identifier.entityName, "entity_name")
    } yield Ref.Identifier(packageId, Ref.QualifiedName(mn, en))

}

object FieldValidations {
  def apply(errorFactories: ErrorFactories): FieldValidations =
    new FieldValidations(errorFactories)
}
