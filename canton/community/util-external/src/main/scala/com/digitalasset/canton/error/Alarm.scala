// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.ErrorCategory.SecurityAlert
import com.daml.error.{BaseError, ContextualizedErrorLogger, ErrorClass, ErrorCode}
import io.grpc.StatusRuntimeException

/** An alarm indicates that a different node is behaving maliciously.
  * Alarms include situations where an attack has been mitigated successfully.
  * Alarms are security relevant events that need to be logged in a standardized way for monitoring and auditing.
  */
abstract class AlarmErrorCode(id: String)(implicit parent: ErrorClass)
    extends ErrorCode(id, SecurityAlert) {
  implicit override val code: AlarmErrorCode = this

  // Overriding this, because the implementation in ErrorCode truncates the cause after 512 characters.
  // But we need to be able to provide very detailed information in some cases for a post mortem investigation,
  // so it does not make sense to truncate the cause.
  override def toMsg(cause: => String, correlationId: Option[String]): String =
    s"${codeStr(correlationId)}: $cause"
}

trait BaseAlarm extends BaseError {
  override def code: AlarmErrorCode

  override def context: Map[String, String] =
    super.context ++ BaseError.extractContext(this)

  /** Report the alarm to the logger. */
  def report()(implicit logger: ContextualizedErrorLogger): Unit = logWithContext()

  /** Reports the alarm to the logger.
    * @return this alarm
    */
  def reported()(implicit logger: ContextualizedErrorLogger): this.type = {
    report()
    this
  }

  def asGrpcError(implicit logger: ContextualizedErrorLogger): StatusRuntimeException =
    ErrorCode.asGrpcError(this)(logger)
}

abstract class Alarm(override val cause: String, override val throwableO: Option[Throwable] = None)(
    implicit override val code: AlarmErrorCode
) extends BaseAlarm
