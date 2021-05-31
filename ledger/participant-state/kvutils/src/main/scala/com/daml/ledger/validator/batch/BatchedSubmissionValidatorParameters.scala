// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

/** Parameters for batch validator to control the amount of parallelism.
  *
  * @param cpuParallelism Amount of parallelism to use for CPU bound steps.
  * @param readParallelism Amount of parallelism to use for ledger read operations.
  */
case class BatchedSubmissionValidatorParameters(
    cpuParallelism: Int,
    readParallelism: Int,
)

object BatchedSubmissionValidatorParameters {
  lazy val reasonableDefault: BatchedSubmissionValidatorParameters = {
    val availableProcessors = Runtime.getRuntime.availableProcessors()
    BatchedSubmissionValidatorParameters(
      cpuParallelism = availableProcessors,
      readParallelism = availableProcessors,
    )
  }
}
