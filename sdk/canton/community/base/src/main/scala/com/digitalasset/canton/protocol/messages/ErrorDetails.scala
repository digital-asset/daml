// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.LocalError

/** localError: when set, the method `logRejection` will log the `localError` at ERROR level. You
  * should set the localError in phase 3 and if the verdict is approved but the validation from
  * Phase 3 failed.
  */
final case class ErrorDetails(
    reason: com.google.rpc.status.Status,
    isMalformed: Boolean,
    localError: Option[LocalError] = None,
) extends PrettyPrinting {

  override protected def pretty: Pretty[ErrorDetails.this.type] = prettyOfClass(
    unnamedParam(_.reason),
    param("isMalformed", _.isMalformed),
  )

  /** Logs the [[localError]] if defined. Otherwise, logs the [[reason]] at INFO level.
    * @param extra
    *   Additional information to be included in the log.
    */
  def logRejection(
      extra: Map[String, String]
  )(implicit errorLoggingContext: ErrorLoggingContext): Unit =
    localError match {
      case Some(local) => errorLoggingContext.logError(local, extra)
      case None =>
        // Log with level INFO, leave it to LocalRejectError to log the details.
        errorLoggingContext.withContext(extra) {
          val action = if (isMalformed) "malformed" else "rejected"
          errorLoggingContext.info(show"Request is finalized as $action. $reason")
        }
    }
}

object ErrorDetails {
  def fromLocalError(localError: LocalError): ErrorDetails =
    ErrorDetails(localError.reason(), localError.isMalformed, Some(localError))
}
