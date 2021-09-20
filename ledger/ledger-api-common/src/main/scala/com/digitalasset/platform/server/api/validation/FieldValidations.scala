// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.ledger.api.{DeduplicationPeriod, domain}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.commands.Commands.{DeduplicationPeriod => DeduplicationPeriodProto}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.value.Value.ContractId
import com.daml.platform.server.api.validation.ErrorFactories._
import com.google.protobuf.duration.{Duration => DurationProto}
import io.grpc.StatusRuntimeException

import java.time.Duration
import scala.util.Try

trait FieldValidations {

  def matchLedgerId(
      ledgerId: LedgerId
  )(received: LedgerId): Either[StatusRuntimeException, LedgerId] =
    if (ledgerId == received) Right(received)
    else Left(ledgerIdMismatch(ledgerId, received, definiteAnswer = Some(false)))

  def requireNonEmptyString(s: String, fieldName: String): Either[StatusRuntimeException, String] =
    Either.cond(s.nonEmpty, s, missingField(fieldName, definiteAnswer = Some(false)))

  def requireIdentifier(s: String): Either[StatusRuntimeException, Ref.Name] =
    Ref.Name.fromString(s).left.map(invalidArgument(definiteAnswer = Some(false)))

  def requireName(
      s: String,
      fieldName: String,
  ): Either[StatusRuntimeException, Ref.Name] =
    if (s.isEmpty)
      Left(missingField(fieldName, definiteAnswer = Some(false)))
    else
      Ref.Name.fromString(s).left.map(invalidField(fieldName, _, definiteAnswer = Some(false)))

  def requireNumber(s: String, fieldName: String): Either[StatusRuntimeException, Long] =
    for {
      s <- requireNonEmptyString(s, fieldName)
      number <- Try(s.toLong).toEither.left.map(t =>
        invalidField(fieldName, t.getMessage, definiteAnswer = Some(false))
      )
    } yield number

  def requirePackageId(
      s: String,
      fieldName: String,
  ): Either[StatusRuntimeException, Ref.PackageId] =
    if (s.isEmpty) Left(missingField(fieldName, definiteAnswer = Some(false)))
    else
      Ref.PackageId.fromString(s).left.map(invalidField(fieldName, _, definiteAnswer = Some(false)))

  def requirePackageId(s: String): Either[StatusRuntimeException, Ref.PackageId] =
    Ref.PackageId.fromString(s).left.map(invalidArgument(definiteAnswer = Some(false)))

  def requireParty(s: String): Either[StatusRuntimeException, Ref.Party] =
    Ref.Party.fromString(s).left.map(invalidArgument(definiteAnswer = Some(false)))

  def requireParties(parties: Set[String]): Either[StatusRuntimeException, Set[Party]] =
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
  ): Either[StatusRuntimeException, Ref.LedgerString] =
    if (s.isEmpty) Left(missingField(fieldName, definiteAnswer = Some(false)))
    else
      Ref.LedgerString
        .fromString(s)
        .left
        .map(invalidField(fieldName, _, definiteAnswer = Some(false)))

  def requireLedgerString(s: String): Either[StatusRuntimeException, Ref.LedgerString] =
    Ref.LedgerString.fromString(s).left.map(invalidArgument(definiteAnswer = Some(false)))

  def requireSubmissionId(s: String): Either[StatusRuntimeException, domain.SubmissionId] = {
    val fieldName = "submission_id"
    if (s.isEmpty) {
      Left(missingField(fieldName, definiteAnswer = Some(false)))
    } else {
      Ref.SubmissionId
        .fromString(s)
        .map(domain.SubmissionId(_))
        .left
        .map(invalidField(fieldName, _, definiteAnswer = Some(false)))
    }
  }

  def requireContractId(
      s: String,
      fieldName: String,
  ): Either[StatusRuntimeException, ContractId] =
    if (s.isEmpty) Left(missingField(fieldName, definiteAnswer = Some(false)))
    else ContractId.fromString(s).left.map(invalidField(fieldName, _, definiteAnswer = Some(false)))

  def requireDottedName(
      s: String,
      fieldName: String,
  ): Either[StatusRuntimeException, Ref.DottedName] =
    Ref.DottedName.fromString(s).left.map(invalidField(fieldName, _, definiteAnswer = Some(false)))

  def requireNonEmpty[M[_] <: Iterable[_], T](
      s: M[T],
      fieldName: String,
  ): Either[StatusRuntimeException, M[T]] =
    if (s.nonEmpty) Right(s)
    else Left(missingField(fieldName, definiteAnswer = Some(false)))

  def requirePresence[T](option: Option[T], fieldName: String): Either[StatusRuntimeException, T] =
    option.fold[Either[StatusRuntimeException, T]](
      Left(missingField(fieldName, definiteAnswer = Some(false)))
    )(Right(_))

  /** We validate only using current time because we set the currentTime as submitTime so no need to check both
    */
  def validateDeduplicationPeriod(
      deduplicationPeriod: DeduplicationPeriodProto,
      maxDeduplicationTimeO: Option[Duration],
      fieldName: String,
  ): Either[StatusRuntimeException, DeduplicationPeriod] = {

    maxDeduplicationTimeO.fold[Either[StatusRuntimeException, DeduplicationPeriod]](
      Left(missingLedgerConfig(definiteAnswer = Some(false)))
    )(maxDeduplicationDuration => {
      def validateDuration(duration: Duration, exceedsMaxDurationMessage: String) = {
        if (duration.isNegative)
          Left(invalidField(fieldName, "Duration must be positive", definiteAnswer = Some(false)))
        else if (duration.compareTo(maxDeduplicationDuration) > 0)
          Left(invalidField(fieldName, exceedsMaxDurationMessage, definiteAnswer = Some(false)))
        else Right(duration)
      }

      def protoDurationToDurationPeriod(duration: DurationProto) = {
        val result = Duration.ofSeconds(duration.seconds, duration.nanos.toLong)
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

  def validateIdentifier(identifier: Identifier): Either[StatusRuntimeException, Ref.Identifier] =
    for {
      packageId <- requirePackageId(identifier.packageId, "package_id")
      mn <- requireDottedName(identifier.moduleName, "module_name")
      en <- requireDottedName(identifier.entityName, "entity_name")
    } yield Ref.Identifier(packageId, Ref.QualifiedName(mn, en))

}

object FieldValidations extends FieldValidations
