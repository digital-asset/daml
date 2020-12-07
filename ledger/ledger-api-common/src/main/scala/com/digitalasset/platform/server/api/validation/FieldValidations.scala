// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import java.time.Duration

import com.daml.lf.data.Ref
import com.daml.lf.value.Value.ContractId
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.value.Identifier
import com.daml.lf.data.Ref.Party
import com.daml.platform.server.api.validation.ErrorFactories._
import io.grpc.StatusRuntimeException

import scala.language.higherKinds
import scala.util.Try

trait FieldValidations {

  def matchLedgerId(ledgerId: LedgerId)(
      received: LedgerId): Either[StatusRuntimeException, LedgerId] =
    if (ledgerId == received) Right(received)
    else Left(ledgerIdMismatch(ledgerId, received))

  def requireNonEmptyString(s: String, fieldName: String): Either[StatusRuntimeException, String] =
    Either.cond(s.nonEmpty, s, missingField(fieldName))

  def requireIdentifier(s: String): Either[StatusRuntimeException, Ref.Name] =
    Ref.Name.fromString(s).left.map(invalidArgument)

  def requireName(
      s: String,
      fieldName: String
  ): Either[StatusRuntimeException, Ref.Name] =
    if (s.isEmpty)
      Left(missingField(fieldName))
    else
      Ref.Name.fromString(s).left.map(invalidField(fieldName, _))

  def requireNumber(s: String, fieldName: String): Either[StatusRuntimeException, Long] =
    for {
      s <- requireNonEmptyString(s, fieldName)
      number <- Try(s.toLong).toEither.left.map(t => invalidField(fieldName, t.getMessage))
    } yield number

  def requirePackageId(
      s: String,
      fieldName: String): Either[StatusRuntimeException, Ref.PackageId] =
    if (s.isEmpty) Left(missingField(fieldName))
    else Ref.PackageId.fromString(s).left.map(invalidField(fieldName, _))

  def requirePackageId(s: String): Either[StatusRuntimeException, Ref.PackageId] =
    Ref.PackageId.fromString(s).left.map(invalidArgument)

  def requireParty(s: String): Either[StatusRuntimeException, Ref.Party] =
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
      fieldName: String
  ): Either[StatusRuntimeException, Ref.LedgerString] =
    if (s.isEmpty) Left(missingField(fieldName))
    else Ref.LedgerString.fromString(s).left.map(invalidField(fieldName, _))

  def requireLedgerString(s: String): Either[StatusRuntimeException, Ref.LedgerString] =
    Ref.LedgerString.fromString(s).left.map(invalidArgument)

  def requireContractId(
      s: String,
      fieldName: String
  ): Either[StatusRuntimeException, ContractId] =
    if (s.isEmpty) Left(missingField(fieldName))
    else ContractId.fromString(s).left.map(invalidField(fieldName, _))

  def requireDottedName(
      s: String,
      fieldName: String): Either[StatusRuntimeException, Ref.DottedName] =
    Ref.DottedName.fromString(s).left.map(invalidField(fieldName, _))

  def requireNonEmpty[M[_] <: Iterable[_], T](
      s: M[T],
      fieldName: String): Either[StatusRuntimeException, M[T]] =
    if (s.nonEmpty) Right(s)
    else Left(missingField(fieldName))

  def requirePresence[T](option: Option[T], fieldName: String): Either[StatusRuntimeException, T] =
    option.fold[Either[StatusRuntimeException, T]](Left(missingField(fieldName)))(Right(_))

  def validateDeduplicationTime(
      durationO: Option[com.google.protobuf.duration.Duration],
      maxDeduplicationTimeO: Option[Duration],
      fieldName: String): Either[StatusRuntimeException, Duration] =
    maxDeduplicationTimeO.fold[Either[StatusRuntimeException, Duration]](
      Left(missingLedgerConfig()))(maxDeduplicationTime =>
      durationO match {
        case None =>
          Right(maxDeduplicationTime)
        case Some(duration) =>
          val result = Duration.ofSeconds(duration.seconds, duration.nanos.toLong)
          if (result.isNegative)
            Left(invalidField(fieldName, "Duration must be positive"))
          else if (result.compareTo(maxDeduplicationTime) > 0)
            Left(invalidField(
              fieldName,
              s"The given deduplication time of $result exceeds the maximum deduplication time of $maxDeduplicationTime"))
          else
            Right(result)
    })

  def validateIdentifier(identifier: Identifier): Either[StatusRuntimeException, Ref.Identifier] =
    for {
      packageId <- requirePackageId(identifier.packageId, "package_id")
      mn <- requireDottedName(identifier.moduleName, "module_name")
      en <- requireDottedName(identifier.entityName, "entity_name")
    } yield Ref.Identifier(packageId, Ref.QualifiedName(mn, en))

}

object FieldValidations extends FieldValidations
