// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import com.daml.grpc.GrpcStatus
import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import io.grpc.{StatusRuntimeException, protobuf}

sealed abstract class SubmissionResult extends Product with Serializable {
  def description: String
}

object SubmissionResult {

  /** The request has been received */
  case object Acknowledged extends SubmissionResult {
    override val description: String = "The request has been received"
  }

  /** The submission has failed with a synchronous error.
    *
    * Asynchronous errors are reported via the command completion stream as a [[Update.CommandRejected]]
    *
    * See the documentation in `error.proto` for how to report common submission errors.
    */
  final case class SynchronousError(status: com.google.rpc.status.Status) extends SubmissionResult {
    override val description: String = s"Submission failed with error ${status.message}"

    def exception: StatusRuntimeException =
      protobuf.StatusProto.toStatusRuntimeException(GrpcStatus.toJavaProto(status))
  }

  object SynchronousError {
    implicit val `SynchronousError to LoggingValue`: ToLoggingValue[SynchronousError] =
      error =>
        LoggingValue.Nested.fromEntries(
          "status" -> LoggingValue.Nested.fromEntries(
            "code" -> error.status.code,
            "message" -> error.status.message,
          )
        )
  }
}
