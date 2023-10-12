// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.ErrorCode
import com.digitalasset.canton.ledger.participant.state.v2.SubmissionResult
import com.google.rpc.code.Code
import com.google.rpc.status.Status as RpcStatus

trait TransactionError extends BaseCantonError {

  // Determines the value of the `definite_answer` key in the error details
  def definiteAnswer: Boolean = false

  /** Parameter has no effect at the moment, as submission ranks are not supported.
    * Setting to false for the time being.
    */
  final override def definiteAnswerO: Option[Boolean] = Some(definiteAnswer)
}

/** Transaction errors are derived from BaseCantonError and need to be logged explicitly */
abstract class TransactionErrorImpl(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
    override val definiteAnswer: Boolean = false,
)(implicit override val code: ErrorCode)
    extends TransactionError

trait TransactionParentError[T <: TransactionError]
    extends TransactionError
    with ParentCantonError[T]

object TransactionError {
  val NoErrorDetails = Seq.empty[com.google.protobuf.any.Any]
  val NotSupported: SubmissionResult.SynchronousError = SubmissionResult.SynchronousError(
    RpcStatus.of(Code.UNIMPLEMENTED.value, "Not supported", TransactionError.NoErrorDetails)
  )
  val PassiveNode: SubmissionResult.SynchronousError = SubmissionResult.SynchronousError(
    RpcStatus.of(Code.UNAVAILABLE.value, "Node is passive", TransactionError.NoErrorDetails)
  )

  def internalError(reason: String): SubmissionResult.SynchronousError =
    SubmissionResult.SynchronousError(RpcStatus.of(Code.INTERNAL.value, reason, NoErrorDetails))

  val shutdownError: SubmissionResult.SynchronousError =
    SubmissionResult.SynchronousError(
      RpcStatus.of(Code.CANCELLED.value, "Node is shutting down", NoErrorDetails)
    )

}
