// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.error.{ErrorCodeLoggingContext, ErrorCodesVersionSwitcher}
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.grpc.GrpcStatuses
import com.daml.platform.server.api.ApiException
import com.daml.platform.server.api.validation.ErrorFactories.{
  addDefiniteAnswerDetails,
  definiteAnswers,
}
import com.google.protobuf.{Any => AnyProto}
import com.google.rpc.{ErrorInfo, Status}
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import scalaz.syntax.tag._

trait ErrorFactories {
  def errorCodesVersionSwitcher: ErrorCodesVersionSwitcher

  def duplicateCommandException(implicit
      errorCodeLoggingContext: ErrorCodeLoggingContext
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val exception = grpcError(
          Status
            .newBuilder()
            .setCode(Code.ALREADY_EXISTS.value())
            .setMessage("Duplicate command")
            .addDetails(definiteAnswers(false))
            .build()
        )
        errorCodeLoggingContext.info(exception.getMessage)
        exception
      },
      v2 = LedgerApiErrors.CommandPreparation.DuplicateCommand.Reject().asGrpcError,
    )

  /** @param expected Expected ledger id.
    * @param received Received ledger id.
    * @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.NOT_FOUND]] status code.
    */
  def ledgerIdMismatch(
      expected: LedgerId,
      received: LedgerId,
      definiteAnswer: Option[Boolean],
  )(implicit errorCodeLoggingContext: ErrorCodeLoggingContext): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        require(!definiteAnswer.contains(true), "Wrong ledger ID can never be a definite answer.")
        val statusBuilder = Status
          .newBuilder()
          .setCode(Code.NOT_FOUND.value())
          .setMessage(
            s"Ledger ID '${received.unwrap}' not found. Actual Ledger ID is '${expected.unwrap}'."
          )
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      v2 = LedgerApiErrors.CommandValidation.LedgerIdMismatch
        .Reject(
          s"Ledger ID '${received.unwrap}' not found. Actual Ledger ID is '${expected.unwrap}'"
        )
        .asGrpcError,
    )

  /** @param fieldName A missing field's name.
    * @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.INVALID_ARGUMENT]] status code.
    */
  def missingField(fieldName: String, definiteAnswer: Option[Boolean]): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(Code.INVALID_ARGUMENT.value())
          .setMessage(s"Missing field: $fieldName")
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      v2 = ???,
    )

  /** @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @param message A status' message.
    * @return An exception with the [[Code.INVALID_ARGUMENT]] status code.
    */
  def invalidArgument(definiteAnswer: Option[Boolean])(message: String): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(Code.INVALID_ARGUMENT.value())
          .setMessage(s"Invalid argument: $message")
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      v2 = ???,
    )

  /** @param fieldName An invalid field's name.
    * @param message A status' message.
    * @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.INVALID_ARGUMENT]] status code.
    */
  def invalidField(
      fieldName: String,
      message: String,
      definiteAnswer: Option[Boolean],
  ): StatusRuntimeException = errorCodesVersionSwitcher.choose(
    v1 = {
      val statusBuilder = Status
        .newBuilder()
        .setCode(Code.INVALID_ARGUMENT.value())
        .setMessage(s"Invalid field $fieldName: $message")
      addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
      grpcError(statusBuilder.build())
    },
    v2 = ???,
  )

  def outOfRange(description: String): StatusRuntimeException = errorCodesVersionSwitcher.choose(
    v1 = grpcError(
      Status
        .newBuilder()
        .setCode(Code.OUT_OF_RANGE.value())
        .setMessage(description)
        .build()
    ),
    v2 = ???,
  )

  /** @param message A status' message.
    * @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.ABORTED]] status code.
    */
  def aborted(message: String, definiteAnswer: Option[Boolean]): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(Code.ABORTED.value())
          .setMessage(message)
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      v2 = ???,
    )

  // permission denied is intentionally without description to ensure we don't leak security relevant information by accident
  def permissionDenied(): StatusRuntimeException = errorCodesVersionSwitcher.choose(
    v1 = grpcError(
      Status
        .newBuilder()
        .setCode(Code.PERMISSION_DENIED.value())
        .build()
    ),
    v2 = ???,
  )

  def unauthenticated(): StatusRuntimeException = errorCodesVersionSwitcher.choose(
    v1 = grpcError(
      Status
        .newBuilder()
        .setCode(Code.UNAUTHENTICATED.value())
        .build()
    ),
    v2 = ???,
  )

  /** @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.UNAVAILABLE]] status code.
    */
  def missingLedgerConfig(definiteAnswer: Option[Boolean]): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(Code.UNAVAILABLE.value())
          .setMessage("The ledger configuration is not available.")
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      v2 = ???,
    )

  def missingLedgerConfigUponRequest(): StatusRuntimeException = errorCodesVersionSwitcher.choose(
    v1 = grpcError(
      Status
        .newBuilder()
        .setCode(Code.NOT_FOUND.value())
        .setMessage("The ledger configuration is not available.")
        .build()
    ),
    v2 = ???,
  )

  def participantPrunedDataAccessed(message: String): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = grpcError(
        Status
          .newBuilder()
          .setCode(Code.NOT_FOUND.value())
          .setMessage(message)
          .build()
      ),
      v2 = ???,
    )

  /** @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.UNAVAILABLE]] status code.
    */
  def serviceNotRunning(definiteAnswer: Option[Boolean]): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(Code.UNAVAILABLE.value())
          .setMessage("Service has been shut down.")
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      v2 = ???,
    )

  /** Transforms Protobuf [[Status]] objects, possibly including metadata packed as [[ErrorInfo]] objects,
    * into exceptions with metadata in the trailers.
    *
    * Asynchronous errors, i.e. failed completions, contain Protobuf [[Status]] objects themselves.
    *
    * @param status A Protobuf [[Status]] object.
    * @return An exception without a stack trace.
    */
  def grpcError(status: Status): StatusRuntimeException = new ApiException(
    StatusProto.toStatusRuntimeException(status)
  )
}

object ErrorFactories extends ErrorFactories {
  // TODO error codes: Remove default
  override def errorCodesVersionSwitcher: ErrorCodesVersionSwitcher =
    new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes = false)

  def apply(errorCodesVersionSwitcherArg: ErrorCodesVersionSwitcher): ErrorFactories =
    new ErrorFactories {
      override def errorCodesVersionSwitcher: ErrorCodesVersionSwitcher =
        errorCodesVersionSwitcherArg
    }

  private[daml] lazy val definiteAnswers = Map(
    true -> AnyProto.pack[ErrorInfo](
      ErrorInfo.newBuilder().putMetadata(GrpcStatuses.DefiniteAnswerKey, "true").build()
    ),
    false -> AnyProto.pack[ErrorInfo](
      ErrorInfo.newBuilder().putMetadata(GrpcStatuses.DefiniteAnswerKey, "false").build()
    ),
  )

  private def addDefiniteAnswerDetails(
      definiteAnswer: Option[Boolean],
      statusBuilder: Status.Builder,
  ): Unit = {
    definiteAnswer.foreach { definiteAnswer =>
      statusBuilder.addDetails(definiteAnswers(definiteAnswer))
    }
  }
}
