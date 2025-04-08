// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.base.error

import com.digitalasset.base.error.ErrorCategory.{SecurityAlert, UnredactedSecurityAlert}
import io.grpc.StatusRuntimeException

/** An alarm indicates that a different node is behaving maliciously. Alarms include situations
  * where an attack has been mitigated successfully. Alarms are security relevant events that need
  * to be logged in a standardized way for monitoring and auditing.
  */
abstract class AlarmErrorCode(id: String, redactDetails: Boolean = true)(implicit
    parent: ErrorClass
) extends ErrorCode(id, if (redactDetails) SecurityAlert else UnredactedSecurityAlert) {
  implicit override val code: AlarmErrorCode = this

}

trait BaseAlarm extends BaseError {
  override def code: AlarmErrorCode

  override def context: Map[String, String] =
    super.context ++ BaseError.extractContext(this)

  /** Report the alarm to the logger. */
  def report()(implicit logger: BaseErrorLogger): Unit = logWithContext()

  /** Reports the alarm to the logger.
    *
    * @return
    *   this alarm
    */
  def reported()(implicit logger: BaseErrorLogger): this.type = {
    report()
    this
  }

  def asGrpcError(implicit logger: BaseErrorLogger): StatusRuntimeException =
    ErrorCode.asGrpcError(this)(logger)
}

abstract class Alarm(override val cause: String, override val throwableO: Option[Throwable] = None)(
    implicit override val code: AlarmErrorCode
) extends BaseAlarm
