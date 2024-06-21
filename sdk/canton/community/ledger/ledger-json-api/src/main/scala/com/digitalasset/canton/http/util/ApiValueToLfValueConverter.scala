// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.error.NoLogging
import com.digitalasset.daml.lf
import com.digitalasset.canton.ledger.api.validation.StricterValueValidator
import com.daml.ledger.api.{v2 => lav2}
import io.grpc.StatusRuntimeException
import scalaz.{Show, \/}

object ApiValueToLfValueConverter {
  final case class Error(cause: StatusRuntimeException)

  object Error {
    implicit val ErrorShow: Show[Error] = Show shows { e =>
      import com.daml.scalautil.ExceptionOps._
      s"ApiValueToLfValueConverter.Error: ${e.cause.description}"
    }
  }

  type ApiValueToLfValue =
    lav2.value.Value => Error \/ lf.value.Value

  def apiValueToLfValue: ApiValueToLfValue = { a =>
    \/.fromEither(StricterValueValidator.validateValue(a)(NoLogging)).leftMap(e => Error(e))
  }
}
