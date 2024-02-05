// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.config.RequireTypes.InvariantViolation as PureInvariantViolation
import com.digitalasset.canton.error.CantonErrorGroups.ProtoDeserializationErrorGroup
import com.digitalasset.canton.error.{BaseCantonError, CantonError}
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
  final case class StringConversionError(message: String) extends ProtoDeserializationError
  final case class UnrecognizedField(message: String) extends ProtoDeserializationError
  final case class UnrecognizedEnum(field: String, value: Int) extends ProtoDeserializationError {
    override val message = s"Unrecognized value `$value` in enum field `$field`"
  }
  final case class FieldNotSet(field: String) extends ProtoDeserializationError {
    override val message = s"Field `$field` is not set"
  }
  final case class TimestampConversionError(message: String) extends ProtoDeserializationError
  final case class TimeModelConversionError(message: String) extends ProtoDeserializationError
  final case class ValueConversionError(field: String, error: String)
      extends ProtoDeserializationError {
    override val message = s"Unable to convert field `$field`: $error"
  }
  final case class RefinedDurationConversionError(field: String, error: String)
      extends ProtoDeserializationError {
    override val message = s"Unable to convert numeric field `$field`: $error"
  }
  final case class InvariantViolation(error: String) extends ProtoDeserializationError {
    override def message = error
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
      s"Message ${protoMessage} has no versioning information corresponding to protobuf $version"
  }

  /** Common Deserialization error code
    *
    * USE THIS ERROR CODE ONLY WITHIN A GRPC SERVICE, PARSING THE INITIAL REQUEST.
    * Don't used it for something like transaction processing or reading from the database.
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
        with CantonError

    final case class WrapNoLogging(reason: ProtoDeserializationError)
        extends BaseCantonError.Impl(
          cause = "Deserialization of protobuf message failed"
        )

    final case class WrapNoLoggingStr(reason: String)
        extends BaseCantonError.Impl(
          cause = "Deserialization of protobuf message failed"
        )
  }

  object InvariantViolation {
    def toProtoDeserializationError(e: PureInvariantViolation): InvariantViolation =
      InvariantViolation(e.message)
    def apply(e: PureInvariantViolation): InvariantViolation = InvariantViolation(e.message)
  }

}
