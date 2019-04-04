// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.validation

import io.grpc.StatusRuntimeException
import com.digitalasset.platform.server.api.validation.ErrorFactories._

trait CommandValidations {

  def matchLedgerId(ledgerId: String)(received: String): Either[StatusRuntimeException, String] =
    Either.cond(ledgerId == received, received, ledgerIdMismatch(ledgerId, received))

  def requireNonEmptyString(s: String, fieldName: String): Either[StatusRuntimeException, Unit] =
    Either.cond(s.nonEmpty, (), missingField(fieldName))

  def requireNonEmpty[T](s: Seq[T], fieldName: String): Either[StatusRuntimeException, Seq[T]] =
    Either.cond(s.nonEmpty, s, missingField(fieldName))

  def requirePresence[T](option: Option[T], fieldName: String): Either[StatusRuntimeException, T] =
    option.toRight(missingField(fieldName))

}

object CommandValidations extends CommandValidations
