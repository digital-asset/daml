// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.{ContextualizedErrorLogger, ErrorCode}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

trait TransactionError extends CantonBaseError {

  // Determines the value of the `definite_answer` key in the error details
  def definiteAnswer: Boolean = false

  /** Parameter has no effect at the moment, as submission ranks are not supported. Setting to false
    * for the time being.
    */
  final override def definiteAnswerO: Option[Boolean] = Some(definiteAnswer)
}

trait TransactionErrorPrettyPrinting extends TransactionError with PrettyPrinting {
  override protected def pretty: Pretty[this.type] =
    this.prettyOfString(_ =>
      this.code.toMsg(
        cause,
        correlationId = None,
        limit = None,
      ) + "; " + ContextualizedErrorLogger.formatContextAsString(
        context
      )
    )
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
