// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.retrying

import java.time.Instant

import com.daml.api.util.TimeProvider
import com.daml.ledger.client.binding.retrying.CommandRetryFlow.Out
import com.daml.util.Ctx

final case class RetryInfo[C, T](
    value: T,
    nrOfRetries: Int,
    firstSubmissionTime: Instant,
    ctx: C,
) {
  def newRetry: RetryInfo[C, T] = copy(nrOfRetries = nrOfRetries + 1)
}

object RetryInfo {

  def wrap[C, T](timeProvider: TimeProvider)(request: Ctx[C, T]): Ctx[RetryInfo[C, T], T] =
    request match {
      case Ctx(context, value, telemetryContext) =>
        Ctx(
          RetryInfo(value, 0, timeProvider.getCurrentTime, context),
          value,
          telemetryContext,
        )
    }

  def unwrap[C, T](request: Out[RetryInfo[C, T]]): Out[C] = request match {
    case Ctx(RetryInfo(_, _, _, context), completion, telemetryContext) =>
      Ctx(context, completion, telemetryContext)
  }

}
