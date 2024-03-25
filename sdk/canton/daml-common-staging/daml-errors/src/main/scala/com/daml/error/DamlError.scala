// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

class DamlError(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
    extraContext: Map[String, Any] = Map(),
)(implicit
    override val code: ErrorCode,
    override val errorContext: ContextualizedErrorLogger,
) extends ContextualizedError {

  // Automatically log the error on generation
  errorContext.logError(this, Map())

  override def context: Map[String, String] =
    super.context ++ extraContext.view.mapValues(_.toString)
}

/** @param definiteAnswer Determines the value of the `definite_answer` key in the error details
  */
class DamlErrorWithDefiniteAnswer(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
    val definiteAnswer: Boolean = false,
    extraContext: Map[String, Any] = Map(),
)(implicit
    override val code: ErrorCode,
    loggingContext: ContextualizedErrorLogger,
) extends DamlError(cause = cause, throwableO = throwableO, extraContext = extraContext) {

  final override def definiteAnswerO: Option[Boolean] = Some(definiteAnswer)

}
