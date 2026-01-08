// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import com.digitalasset.canton.participant.pruning.AcsCommitmentBenchmark
import org.openjdk.jmh.annotations.{Level, Param, Setup}

// The benchmarks are created with the use of the JMH harness and one can run them from the
// sbt terminal by running:
//    sbt:canton> microbench / Jmh / run AcsCommitmentBenchmarkCatchupConfig

/** [info] Benchmark (overwrittenConfigCatchUpIntervalSkip) Mode Cnt Score Error Units [info]
  * AcsCommitmentBenchmarkCatchupConfig.localParticipantCatchUp 100 avgt 20 3.615 ± 0.137 s/op
  * [info] AcsCommitmentBenchmarkCatchupConfig.localParticipantCatchUp 25 avgt 20 4.003 ± 1.184 s/op
  * [info] AcsCommitmentBenchmarkCatchupConfig.localParticipantCatchUp 5 avgt 20 4.511 ± 1.122 s/op
  * [info] AcsCommitmentBenchmarkCatchupConfig.localParticipantCatchUp 2 avgt 20 5.546 ± 1.170 s/op
  * [info] AcsCommitmentBenchmarkCatchupConfig.localParticipantCatchUp 100000 avgt 20 9.993 ± 0.175
  * s/op
  */

/** This benchmark measure the impact of catchupIntervalSkip, by skipping intervals we perform less
  * computations are able to do these faster. "100000" is used as the last value to give a
  * comparative benchmark when there is no catch up happening.
  */
@SuppressWarnings(
  Array("org.wartremover.warts.Var", "org.wartremover.warts.Null")
)
class AcsCommitmentBenchmarkCatchupConfig extends AcsCommitmentBenchmark {

  @Param(Array("100", "25", "5", "2", "100000"))
  var overwrittenConfigCatchUpIntervalSkip: Int = _

  var overwrittenConfigNrIntervalsToTriggerCatchUp: Int = 5

  @Setup(Level.Trial)
  override def setupBenchmarkRun(): Unit = {

    configCatchUpIntervalSkip = overwrittenConfigCatchUpIntervalSkip
    configNrIntervalsToTriggerCatchUp = overwrittenConfigNrIntervalsToTriggerCatchUp
    super.setupBenchmarkRun()
  }
}
