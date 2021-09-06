// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import java.time.Duration

import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.commands.Commands.{DeduplicationPeriod => DeduplicationPeriodProto}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.value.Value.ContractId
import com.daml.platform.server.api.validation.ErrorFactories.{
  invalidArgument,
  invalidField,
  ledgerIdMismatch,
  missingField,
  missingLedgerConfig,
}
import com.google.protobuf.duration.{Duration => DurationProto}
import io.grpc.StatusRuntimeException

import scala.util.Try

trait FieldValidations {

  def matchLedgerId(
      ledgerId: LedgerId
  )(received: LedgerId): Either[StatusRuntimeException, LedgerId] =
    if (ledgerId == received) Right(received)
    else {
      // TODO self-service error codes: Refactor using the new API
      Left(ledgerIdMismatch(ledgerId, received))
    }

  def requireNonEmptyString(s: String, fieldName: String): Either[StatusRuntimeException, String] =
    // TODO self-service error codes: Refactor using the new API
    Either.cond(s.nonEmpty, s, missingField(fieldName))

  def requireIdentifier(s: String): Either[StatusRuntimeException, Ref.Name] =
    // TODO self-service error codes: Refactor using the new API
    Ref.Name.fromString(s).left.map(invalidArgument)

  def requireName(
      s: String,
      fieldName: String,
  ): Either[StatusRuntimeException, Ref.Name] =
    if (s.isEmpty)
      // TODO self-service error codes: Refactor using the new API
      Left(missingField(fieldName))
    else
      // TODO self-service error codes: Refactor using the new API
      Ref.Name.fromString(s).left.map(invalidField(fieldName, _))

  def requireNumber(s: String, fieldName: String): Either[StatusRuntimeException, Long] =
    for {
      s <- requireNonEmptyString(s, fieldName)
      number <- Try(s.toLong).toEither.left.map(t => invalidField(fieldName, t.getMessage))
    } yield number

  def requirePackageId(
      s: String,
      fieldName: String,
  ): Either[StatusRuntimeException, Ref.PackageId] =
    // TODO self-service error codes: Refactor using the new API
    if (s.isEmpty) Left(missingField(fieldName))
    // TODO self-service error codes: Refactor using the new API
    else Ref.PackageId.fromString(s).left.map(invalidField(fieldName, _))

  def requirePackageId(s: String): Either[StatusRuntimeException, Ref.PackageId] =
    // TODO self-service error codes: Refactor using the new API
    Ref.PackageId.fromString(s).left.map(invalidArgument)

  def requireParty(s: String): Either[StatusRuntimeException, Ref.Party] =
    // TODO self-service error codes: Refactor using the new API
    Ref.Party.fromString(s).left.map(invalidArgument)

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
    // TODO self-service error codes: Refactor using the new API
    if (s.isEmpty) Left(missingField(fieldName))
    // TODO self-service error codes: Refactor using the new API
    else Ref.LedgerString.fromString(s).left.map(invalidField(fieldName, _))

  def requireLedgerString(s: String): Either[StatusRuntimeException, Ref.LedgerString] =
    // TODO self-service error codes: Refactor using the new API
    Ref.LedgerString.fromString(s).left.map(invalidArgument)

  def requireContractId(
      s: String,
      fieldName: String,
  ): Either[StatusRuntimeException, ContractId] =
    // TODO self-service error codes: Refactor using the new API
    if (s.isEmpty) Left(missingField(fieldName))
    // TODO self-service error codes: Refactor using the new API
    else ContractId.fromString(s).left.map(invalidField(fieldName, _))

  def requireDottedName(
      s: String,
      fieldName: String,
  ): Either[StatusRuntimeException, Ref.DottedName] =
    // TODO self-service error codes: Refactor using the new API
    Ref.DottedName.fromString(s).left.map(invalidField(fieldName, _))

  def requireNonEmpty[M[_] <: Iterable[_], T](
      s: M[T],
      fieldName: String,
  ): Either[StatusRuntimeException, M[T]] =
    if (s.nonEmpty) Right(s)
    // TODO self-service error codes: Refactor using the new API
    else Left(missingField(fieldName))

  def requirePresence[T](option: Option[T], fieldName: String): Either[StatusRuntimeException, T] =
    // TODO self-service error codes: Refactor using the new API
    option.fold[Either[StatusRuntimeException, T]](Left(missingField(fieldName)))(Right(_))

  /** We validate only using current time because we set the currentTime as submitTime so no need to check both
    */
  def validateDeduplicationPeriod(
      deduplicationPeriod: DeduplicationPeriodProto,
      maxDeduplicationTimeO: Option[Duration],
      fieldName: String,
  ): Either[StatusRuntimeException, DeduplicationPeriod] = {

    maxDeduplicationTimeO.fold[Either[StatusRuntimeException, DeduplicationPeriod]](
      // TODO self-service error codes: Validate presence of maxDeduplicationTime0 upstream and pass value here
      Left(missingLedgerConfig())
    )(maxDeduplicationDuration => {
      def validateDuration(duration: Duration, exceedsMaxDurationMessage: String) = {
        if (duration.isNegative) {
          // TODO self-service error codes: Refactor using the new API
          Left(invalidField(fieldName, "Duration must be positive"))
        } else if (duration.compareTo(maxDeduplicationDuration) > 0) {
          // TODO self-service error codes: Refactor using the new API
          Left(invalidField(fieldName, exceedsMaxDurationMessage))
        } else Right(duration)
      }

      def protoDurationToDurationPeriod(duration: DurationProto) = {
        val result = Duration.ofSeconds(duration.seconds, duration.nanos.toLong)
        validateDuration(
          result,
          s"The given deduplication time of $result exceeds the maximum deduplication time of $maxDeduplicationDuration",
        ).map(DeduplicationPeriod.DeduplicationDuration)
      }

      deduplicationPeriod match {
        case DeduplicationPeriodProto.Empty =>
          Right(DeduplicationPeriod.DeduplicationDuration(maxDeduplicationDuration))
        case DeduplicationPeriodProto.DeduplicationTime(duration) =>
          protoDurationToDurationPeriod(duration)
        case DeduplicationPeriodProto.DeduplicationOffset(offset) =>
          Right(
            DeduplicationPeriod.DeduplicationOffset(
              Offset.fromHexString(Ref.HexString.assertFromString(offset))
            )
          )
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
