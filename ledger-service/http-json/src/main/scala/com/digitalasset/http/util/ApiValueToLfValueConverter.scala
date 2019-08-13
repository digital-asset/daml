// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import com.digitalasset.daml.lf
import com.digitalasset.ledger.api.validation.ValueValidator
import com.digitalasset.ledger.api.{v1 => lav1}
import io.grpc.StatusRuntimeException
import scalaz.{Show, \/}

object ApiValueToLfValueConverter {
  final case class Error(cause: StatusRuntimeException)

  object Error {
    implicit val ErrorShow: Show[Error] = Show shows { e =>
      s"ApiValueToLfValueConverter.Error: ${e.cause.getMessage}"
    }
  }

  type ApiValueToLfValue =
    lav1.value.Value => Error \/ lf.value.Value[lf.value.Value.AbsoluteContractId]

  def apiValueToLfValue: ApiValueToLfValue = { a: lav1.value.Value =>
    \/.fromEither(ValueValidator.validateValue(a)).leftMap(e => Error(e))
  }
}
