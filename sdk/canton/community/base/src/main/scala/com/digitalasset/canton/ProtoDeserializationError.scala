// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.config.RequireTypes.InvariantViolation as PureInvariantViolation
import com.digitalasset.canton.error.CantonErrorGroups.ProtoDeserializationErrorGroup
import com.digitalasset.canton.error.{CantonBaseError, CantonError, ContextualizedCantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.version.ProtoVersion
import com.google.protobuf.InvalidProtocolBufferException

sealed trait ProtoDeserializationError extends Product with Serializable {
  def inField(field: String): ProtoDeserializationError.ValueDeserializationError =
    ProtoDeserializationError.ValueDeserializationError(field, message)

  def message: String
}

object ProtoDeserializationError extends ProtoDeserializationErrorGroup {
  final case class BufferException(error: InvalidProtocolBufferException)
      extends ProtoDeserializationError {
    override val message = error.getMessage
  }
  final case class CryptoDeserializationError(error: DeserializationError)
      extends ProtoDeserializationError {
    override val message = error.message
  }
  final case class TransactionDeserialization(message: String) extends ProtoDeserializationError
  final case class ValueDeserializationError(field: String, message: String)
      extends ProtoDeserializationError
  final case class StringConversionError(error: String, field: Option[String] = None)
      extends ProtoDeserializationError {
    val message = field.fold(error)(field => s"Unable to parse string in $field: $error")
  }
  final case class UnrecognizedField(message: String) extends ProtoDeserializationError
  final case class UnrecognizedEnum(field: String, value: String, validValues: Seq[String])
      extends ProtoDeserializationError {
    private val validValuesMessage =
      if (validValues.nonEmpty) s"\nValid values are: ${validValues.toSet.mkString(", ")}" else ""
    override val message =
      s"Unrecognized value `$value` in enum field `$field`" + validValuesMessage
  }
  object UnrecognizedEnum {
    def apply(field: String, value: Int): UnrecognizedEnum =
      UnrecognizedEnum(field, value.toString, Nil)
  }
  final case class FieldNotSet(field: String) extends ProtoDeserializationError {
    override val message = s"Field `$field` is not set"
  }
  final case class TimestampConversionError(message: String) extends ProtoDeserializationError
  final case class ValueConversionError(field: String, error: String)
      extends ProtoDeserializationError {
    override val message = s"Unable to convert field `$field`: $error"
  }
  final case class RefinedDurationConversionError(field: String, error: String)
      extends ProtoDeserializationError {
    override val message = s"Unable to convert numeric field `$field`: $error"
  }

  final case class InvariantViolation(field: Option[String], error: String)
      extends ProtoDeserializationError {
    override def message =
      field.fold(error)(field => s"Invariant violation in field `$field`: $error")
  }

  final case class MaxBytesToDecompressExceeded(error: String) extends ProtoDeserializationError {
    override def message = error
  }
  final case class OtherError(error: String) extends ProtoDeserializationError {
    override def message = error
  }
  final case class UnknownProtoVersion(version: ProtoVersion, protoMessage: String)
      extends ProtoDeserializationError {
    override def message =
      s"Message $protoMessage has no versioning information corresponding to protobuf $version"
  }

  /** Common Deserialization error code
    *
    * USE THIS ERROR CODE ONLY WITHIN A GRPC SERVICE, PARSING THE INITIAL REQUEST. Don't used it for
    * something like transaction processing or reading from the database.
    */
  @Explanation(
    """This error indicates that an incoming administrative command could not be processed due to a malformed message."""
  )
  @Resolution("Inspect the error details and correct your application")
  object ProtoDeserializationFailure
      extends ErrorCode(
        id = "PROTO_DESERIALIZATION_FAILURE",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {

    final case class Wrap(reason: ProtoDeserializationError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Deserialization of protobuf message failed"
        )
        with ContextualizedCantonError

    final case class WrapNoLogging(reason: ProtoDeserializationError)
        extends CantonBaseError.Impl(
          cause = "Deserialization of protobuf message failed"
        )

    final case class WrapNoLoggingStr(reason: String)
        extends CantonBaseError.Impl(
          cause = "Deserialization of protobuf message failed"
        )
  }

  object InvariantViolation {
    def toProtoDeserializationError(field: String, e: PureInvariantViolation): InvariantViolation =
      InvariantViolation(field = Some(field), error = e.message)
    def apply(field: String, e: PureInvariantViolation): InvariantViolation =
      InvariantViolation(field = Some(field), error = e.message)
    def apply(field: String, error: String): InvariantViolation =
      InvariantViolation(field = Some(field), error = error)
  }

}
