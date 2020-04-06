// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.lf
import com.daml.ledger.api.validation.ValueValidator
import com.daml.ledger.api.{v1 => lav1}
import io.grpc.StatusRuntimeException
import scalaz.{Show, \/}

object ApiValueToLfValueConverter {
  final case class Error(cause: StatusRuntimeException)

  object Error {
    implicit val ErrorShow: Show[Error] = Show shows { e =>
      import com.daml.util.ExceptionOps._
      s"ApiValueToLfValueConverter.Error: ${e.cause.description}"
    }
  }

  type ApiValueToLfValue =
    lav1.value.Value => Error \/ lf.value.Value[lf.value.Value.AbsoluteContractId]

  def apiValueToLfValue: ApiValueToLfValue = { a: lav1.value.Value =>
    \/.fromEither(ValueValidator.validateValue(a)).leftMap(e => Error(e))
  }
}
