// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.validation

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.platform.server.api.validation.ErrorFactories._
import io.grpc.StatusRuntimeException

import scala.language.higherKinds
import scala.util.Try

trait FieldValidations {

  def matchLedgerId(ledgerId: String)(received: String): Either[StatusRuntimeException, String] =
    if (ledgerId == received) Right(received)
    else Left(ledgerIdMismatch(ledgerId, received))

  def requireNonEmptyString(s: String, fieldName: String): Either[StatusRuntimeException, String] =
    if (s.nonEmpty) Right(s)
    else Left(missingField(fieldName))

  def requireNumber(s: String, fieldName: String): Either[StatusRuntimeException, Long] =
    for {
      s <- requireNonEmptyString(s, fieldName)
      number <- Try(s.toLong).toEither.left.map(t => invalidField(fieldName, t.getMessage))
    } yield number

  def requirePackageId(
      s: String,
      fieldName: String): Either[StatusRuntimeException, Ref.PackageId] =
    Ref.PackageId.fromString(s).left.map(invalidField(fieldName, _))

  def requirePackageId(s: String): Either[StatusRuntimeException, Ref.PackageId] =
    Ref.PackageId.fromString(s).left.map(invalidArgument)

  def requireParty(s: String, fieldName: String): Either[StatusRuntimeException, Ref.Party] =
    Ref.Party.fromString(s).left.map(invalidField(fieldName, _))

  def requireParty(s: String): Either[StatusRuntimeException, Ref.Party] =
    Ref.Party.fromString(s).left.map(invalidArgument)

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

}

object FieldValidations extends FieldValidations
