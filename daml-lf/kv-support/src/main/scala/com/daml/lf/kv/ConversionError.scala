package com.daml.lf.kv

import com.daml.lf.value.ValueCoder

sealed abstract class ConversionError(val errorMessage: String)
    extends RuntimeException(errorMessage)

object ConversionError {
  final case class ParseError(override val errorMessage: String)
      extends ConversionError(errorMessage)
  final case class DecodeError(cause: ValueCoder.DecodeError)
      extends ConversionError(cause.errorMessage)
}
