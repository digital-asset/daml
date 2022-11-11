// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import scala.concurrent.duration._

/** @param parallelism The number of submitSingleCommand invocations each trigger will
  *                    attempt to execute in parallel. Note that this does not in any
  *                    way bound the number of already-submitted, but not completed,
  *                    commands that may be pending.
  * @param maxRetries Maximum number of retries for a failing ledger API command submission.
  * @param maxInFlightCommands Maximum number of in-flight commands that we shall allow
  *                            *before* the ledger client automatically fails ledger client submission requests.
  * @param maxSubmissionRequests Used to control rate at which we throttle ledger client submission requests.
  * @param maxSubmissionDuration Used to control rate at which we throttle ledger client submission requests.
  * @param submissionFailureQueueSize Size of the queue holding ledger API command submission failures.
  */
final case class TriggerRunnerConfig(
    parallelism: Int,
    maxRetries: Int,
    maxInFlightCommands: Int,
    maxSubmissionRequests: Int,
    maxSubmissionDuration: FiniteDuration,
    submissionFailureQueueSize: Int,
)

object TriggerRunnerConfig {
  val DefaultTriggerRunnerConfig: TriggerRunnerConfig = {
    val parallelism = 8

    TriggerRunnerConfig(
      parallelism = parallelism,
      maxRetries = 6,
      maxInFlightCommands = 50,
      maxSubmissionRequests = 100,
      maxSubmissionDuration = 5.seconds,
      // 256 here comes from the default ExecutionContext.
      submissionFailureQueueSize = 256 + parallelism,
    )
  }
}
