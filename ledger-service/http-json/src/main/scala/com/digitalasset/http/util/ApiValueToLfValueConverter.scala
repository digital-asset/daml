// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import com.digitalasset.daml.lf
import com.digitalasset.ledger.api
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.validation.CommandsValidator
import com.digitalasset.ledger.api.{v1 => lav1}
import io.grpc.StatusRuntimeException
import scalaz.{Show, \/}

object ApiValueToLfValueConverter {
  final case class Error(cause: StatusRuntimeException)

  object Error {
    implicit val ErrorShow: Show[Error] = new Show[Error] {
      override def shows(e: Error): String =
        s"ApiValueToLfValueConverter.Error: ${e.cause.getMessage}"
    }
  }

  type ApiValueToLfValue =
    lav1.value.Value => Error \/ lf.value.Value[lf.value.Value.AbsoluteContractId]

  def apiValueToLfValue(ledgerId: lar.LedgerId): ApiValueToLfValue = {
    val commandsValidator = new CommandsValidator(domainLedgerId(ledgerId))

    a: lav1.value.Value =>
      \/.fromEither(commandsValidator.validateValue(a)).leftMap(e => Error(e))
  }

  private def domainLedgerId(a: lar.LedgerId): api.domain.LedgerId =
    api.domain.LedgerId(lar.LedgerId.unwrap(a))
}
