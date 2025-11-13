// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.participant.pruning.AcsCommitmentBenchmark
import org.openjdk.jmh.annotations.{Level, Param, Setup}

// The benchmarks are created with the use of the JMH harness and one can run them from the
// sbt terminal by running:
//    sbt:canton> microbench / Jmh / run AcsCommitmentBenchmarkCaching
/** [info] Benchmark (cachingEnabled) (overwrittenStakeholderGroup) Mode Cnt Score Error Units
  * [info] AcsCommitmentBenchmarkCaching.localParticipantCatchUp false x3 avgt 20 41.710 ± 5.190
  * s/op [info] AcsCommitmentBenchmarkCaching.localParticipantCatchUp false x3withManyStakeholders
  * avgt 20 43.269 ± 4.809 s/op [info] AcsCommitmentBenchmarkCaching.localParticipantCatchUp true x3
  * avgt 20 43.343 ± 5.919 s/op [info] AcsCommitmentBenchmarkCaching.localParticipantCatchUp true
  * x3withManyStakeholders avgt 20 37.451 ± 2.251 s/op
  */

/** This benchmark measures the impact of caching, "x3" and "x3withManyStakeholders" have the same
  * amount of counter participants, but x3 have one stakeholder group with many parties whereas
  * x3withManyStakeholders has many stakeholders with one party. we should see an improvement in
  * performance for x3WithManyStakeholders when caching is turned on, compared to x3.
  */
@SuppressWarnings(
  Array("org.wartremover.warts.Var", "org.wartremover.warts.Null")
)
class AcsCommitmentBenchmarkCaching extends AcsCommitmentBenchmark {

  var overwrittenActiveContracts: Int = 5

  var overwrittenCounterParticipants: Int = 5

  var overwrittenPartiesPerCounterParticipants: Int = 3

  @Param(Array("false", "true"))
  var cachingEnabled: Boolean = _

  @Param(Array("x3", "x3withManyStakeholders"))
  var overwrittenStakeholderGroup: String = _

  @Setup(Level.Trial)
  override def setupBenchmarkRun(): Unit = {

    activeContracts = overwrittenActiveContracts
    counterParticipants = overwrittenCounterParticipants
    stakeholderGroups = overwrittenStakeholderGroup
    enableCommitmentCaching = cachingEnabled
    partiesPerCounterParticipant = overwrittenPartiesPerCounterParticipants
    super.setupBenchmarkRun()
  }
}
