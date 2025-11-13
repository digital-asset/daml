// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import org.openjdk.jmh.annotations.{Level, Param, Setup}

// The benchmarks are created with the use of the JMH harness and one can run them from the
// sbt terminal by running:
//    sbt:canton> microbench / Jmh / run AcsCommitmentBenchmarkLoad
/** [info] Benchmark (overwrittenCounterParticipants) (overwrittenStakeholderGroup) Mode Cnt Score
  * Error Units [info] AcsCommitmentBenchmarkLoad.localParticipantCatchUp 3 x3 avgt 20 16.846 ±
  * 0.216 s/op [info] AcsCommitmentBenchmarkLoad.localParticipantCatchUp 3 massive avgt 20 13.813 ±
  * 0.124 s/op [info] AcsCommitmentBenchmarkLoad.localParticipantCatchUp 6 x3 avgt 20 50.600 ± 0.521
  * s/op [info] AcsCommitmentBenchmarkLoad.localParticipantCatchUp 6 massive avgt 20 25.832 ± 0.341
  * s/op [info] AcsCommitmentBenchmarkLoad.localParticipantCatchUp 9 x3 avgt 20 98.411 ± 2.457 s/op
  * [info] AcsCommitmentBenchmarkLoad.localParticipantCatchUp 9 massive avgt 20 37.752 ± 0.653 s/op
  */
/** This benchmark measures the impact on performance when the amount of counter participants
  * increases. Increasing the amount of participants should negatively impact performance. Massive
  * should have a highly reduced amount compared to x3, since x3 causes many more stakeholder groups
  * to be generated (and more computations needed).
  */
@SuppressWarnings(
  Array("org.wartremover.warts.Var", "org.wartremover.warts.Null")
)
class AcsCommitmentBenchmarkLoad extends AcsCommitmentBenchmark {

  @Param(Array("3", "6", "9"))
  var overwrittenCounterParticipants: Int = _

  @Param(Array("x3", "massive"))
  var overwrittenStakeholderGroup: String = _

  @Setup(Level.Trial)
  override def setupBenchmarkRun(): Unit = {

    counterParticipants = overwrittenCounterParticipants
    stakeholderGroups = overwrittenStakeholderGroup
    super.setupBenchmarkRun()
  }
}
