// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv

import com.daml.lf.value.ValueCoder

sealed abstract class ConversionError(val errorMessage: String)
    extends RuntimeException(errorMessage)

object ConversionError {
  final case class ParseError(override val errorMessage: String)
      extends ConversionError(errorMessage)
  final case class DecodeError(cause: ValueCoder.DecodeError)
      extends ConversionError(cause.errorMessage)
  final case class EncodeError(cause: ValueCoder.EncodeError)
      extends ConversionError(cause.errorMessage)
  final case class InternalError(override val errorMessage: String)
      extends ConversionError(errorMessage)
}
