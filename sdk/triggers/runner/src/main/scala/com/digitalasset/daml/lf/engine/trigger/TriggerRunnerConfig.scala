// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import scala.concurrent.duration._

/** Trigger hard limits. If any of these values are exceeded, then the current trigger instance will throw a
  * `TriggerHardLimitException` and stop running.
  *
  * @param maximumActiveContracts Maximum number of active contracts that we will store at any point in time.
  * @param inFlightCommandOverflowCount When the number of in-flight command submissions exceeds this value, then we
  *                                     kill the trigger instance by throwing an InFlightCommandOverflowException.
  * @param allowInFlightCommandOverflows flag to control whether we allow in-flight command overflows or not.
  * @param ruleEvaluationTimeout If the trigger rule evaluator takes longer than this timeout value, then we throw a
  *                               TriggerRuleEvaluationTimeout.
  * @param stepInterpreterTimeout If the trigger rule step evaluator (during rule evaluation) takes longer than this
  *                                timeout value, then we throw a TriggerRuleStepInterpretationTimeout.
  * @param allowTriggerTimeouts flag to control whether we allow rule evaluation and step interpreter timeouts or not.
  */
final case class TriggerRunnerHardLimits(
    maximumActiveContracts: Long,
    inFlightCommandOverflowCount: Int,
    allowInFlightCommandOverflows: Boolean,
    ruleEvaluationTimeout: FiniteDuration,
    stepInterpreterTimeout: FiniteDuration,
    allowTriggerTimeouts: Boolean,
)

/** @param parallelism The number of submitSingleCommand invocations each trigger will attempt to execute in parallel.
  *                    Note that this does not in any way bound the number of already-submitted, but not completed,
  *                    commands that may be pending.
  * @param maxRetries Maximum number of retries when the ledger client fails an API command submission.
  * @param maxSubmissionRequests Used to control rate at which we throttle ledger client submission requests.
  * @param maxSubmissionDuration Used to control rate at which we throttle ledger client submission requests.
  * @param inFlightCommandBackPressureCount When the number of in-flight command submissions exceeds this value, then we
  *                                         enable Daml rule evaluation to apply backpressure (by failing emitCommands
  *                                         evaluations).
  * @param submissionFailureQueueSize Size of the queue holding ledger API command submission failures.
  * @param maximumBatchSize Maximum number of messages triggers will batch (for rule evaluation/processing).
  * @param batchingDuration Period of time we will wait before emitting a message batch (for rule evaluation/processing).
  */
final case class TriggerRunnerConfig(
    parallelism: Int,
    maxRetries: Int,
    maxSubmissionRequests: Int,
    maxSubmissionDuration: FiniteDuration,
    inFlightCommandBackPressureCount: Long,
    submissionFailureQueueSize: Int,
    maximumBatchSize: Long,
    batchingDuration: FiniteDuration,
    hardLimit: TriggerRunnerHardLimits,
)

object TriggerRunnerConfig {
  val DefaultTriggerRunnerConfig: TriggerRunnerConfig = {
    val parallelism = 8
    val maxSubmissionRequests = 100
    val maxSubmissionDuration = 5.seconds

    TriggerRunnerConfig(
      parallelism = parallelism,
      maxRetries = 6,
      maxSubmissionRequests = maxSubmissionRequests,
      maxSubmissionDuration = maxSubmissionDuration,
      inFlightCommandBackPressureCount = 1000,
      // 256 here comes from the default ExecutionContext.
      submissionFailureQueueSize = 256 + parallelism,
      maximumBatchSize = 1000,
      batchingDuration = 250.milliseconds,
      hardLimit = TriggerRunnerHardLimits(
        maximumActiveContracts = 20000,
        inFlightCommandOverflowCount = 10000,
        allowInFlightCommandOverflows = true,
        // 50% extra on the maxSubmissionDuration value
        ruleEvaluationTimeout = maxSubmissionDuration * 3 / 2,
        // 50% extra on mean time between submission requests
        stepInterpreterTimeout = (maxSubmissionDuration / maxSubmissionRequests.toLong) * 3 / 2,
        allowTriggerTimeouts = false,
      ),
    )
  }
}
