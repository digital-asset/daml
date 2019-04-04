// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.retrying

import java.time.Instant

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.client.binding.retrying.CommandRetryFlow.{In, Out}
import com.digitalasset.util.Ctx

case class RetryInfo[C](
    request: SubmitRequest,
    nrOfRetries: Int,
    firstSubmissionTime: Instant,
    ctx: C) {
  def newRetry: RetryInfo[C] = copy(nrOfRetries = nrOfRetries + 1)
}

object RetryInfo {

  def wrap[C](timeProvider: TimeProvider)(request: In[C]): In[RetryInfo[C]] = request match {
    case Ctx(context, submitRequest) =>
      Ctx(RetryInfo(submitRequest, 0, timeProvider.getCurrentTime, context), submitRequest)
  }

  def unwrap[C](request: Out[RetryInfo[C]]): Out[C] = request match {
    case Ctx(RetryInfo(_, _, _, context), completion) =>
      Ctx(context, completion)
  }

}
