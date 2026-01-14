// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.participant.pruning.AcsCommitmentBenchmark
import org.openjdk.jmh.annotations.{Level, Param, Setup}

// The benchmarks are created with the use of the JMH harness and one can run them from the
// sbt terminal by running:
//    sbt:canton> microbench / Jmh / run AcsCommitmentBenchmarkContractTypes

/** [info] Benchmark (overwrittenActiveContracts) (overwrittenPassiveContracts) Mode Cnt Score Error
  * Units [info] AcsCommitmentBenchmarkContractTypes.localParticipantCatchUp 5 5 avgt 20 10.457 ±
  * 0.222 s/op [info] AcsCommitmentBenchmarkContractTypes.localParticipantCatchUp 5 100 avgt 20
  * 10.506 ± 0.195 s/op [info] AcsCommitmentBenchmarkContractTypes.localParticipantCatchUp 100 5
  * avgt 20 20.295 ± 0.405 s/op [info] AcsCommitmentBenchmarkContractTypes.localParticipantCatchUp
  * 100 100 avgt 20 20.090 ± 0.270 s/op
  */

/** This benchmark measures the impact on performance when the amount of active and passive
  * contracts increases. active contracts should show an increasement in runtime, however an
  * increase in passive contracts should have a minimal impact.
  */
@SuppressWarnings(
  Array("org.wartremover.warts.Var", "org.wartremover.warts.Null")
)
class AcsCommitmentBenchmarkContractTypes extends AcsCommitmentBenchmark {

  @Param(Array("5", "100"))
  var overwrittenActiveContracts: Int = _

  @Param(Array("5", "100"))
  var overwrittenPassiveContracts: Int = _

  @Setup(Level.Trial)
  override def setupBenchmarkRun(): Unit = {

    activeContracts = overwrittenActiveContracts
    passiveContracts = overwrittenPassiveContracts
    super.setupBenchmarkRun()
  }
}
