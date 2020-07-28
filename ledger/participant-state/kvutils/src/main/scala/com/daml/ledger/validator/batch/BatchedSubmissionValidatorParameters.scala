// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

/** Parameters for batch validator to control the amount of parallelism.
  *
  * @param cpuParallelism Amount of parallelism to use for CPU bound steps.
  * @param readParallelism Amount of parallelism to use for ledger read operations.
  * @param commitParallelism Amount of parallelism to use for ledger commit operations.
  */
case class BatchedSubmissionValidatorParameters(
    cpuParallelism: Int,
    readParallelism: Int,
    commitParallelism: Int
)

object BatchedSubmissionValidatorParameters {
  lazy val default: BatchedSubmissionValidatorParameters = {
    val availableProcessors = Runtime.getRuntime.availableProcessors()
    BatchedSubmissionValidatorParameters(
      cpuParallelism = availableProcessors,
      readParallelism = availableProcessors,
      commitParallelism = 1
    )
  }
}
