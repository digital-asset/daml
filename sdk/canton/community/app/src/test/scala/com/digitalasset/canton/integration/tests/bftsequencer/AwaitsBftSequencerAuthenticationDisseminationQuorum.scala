// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsequencer

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.WriteReadiness
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.{BaseTest, integration}
import org.scalatest.Assertion

import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait AwaitsBftSequencerAuthenticationDisseminationQuorum {
  this: BaseTest =>

  /** Helper to wait until all sequencers are stably ready to write, i.e., have stably authenticated
    * at least a weak quorum of BFT peers (i.e., a quorum that allows the dissemination sub-protocol
    * to run and thus prevents rejection of submission requests).
    *
    * Useful upon multi-sequencer test/benchmark initialization to prevent potential flaky test log
    * noise due to submission rejections such as "P2P connectivity is not ready (authenticated = 1 <
    * dissemination quorum = 2)"
    */
  protected def waitUntilAllBftSequencersAuthenticateDisseminationQuorum(
      timeUntilSuccess: FiniteDuration = 60.seconds,
      timeOfSuccessStability: FiniteDuration = 10.seconds,
  )(implicit
      env: integration.TestConsoleEnvironment
  ): Assertion = {
    val weakQuorumSize = OrderingTopology.weakQuorumSize(env.sequencers.all.size)
    clue(
      "make sure all sequencers are ready to accept send requests " +
        s"(i.e., have connected to a dissemination quorum of at least $weakQuorumSize other sequencers)"
    ) {
      eventually(timeUntilSuccess) {
        always(timeOfSuccessStability) {
          val readinessReports =
            env.sequencers.all.map { sequencer =>
              val readiness = sequencer.bft.get_write_readiness()
              logger.debug(
                s"Sequencer $sequencer has reported the following write readiness: $readiness"
              )
              readiness
            }
          forEvery(readinessReports)(_ shouldBe a[WriteReadiness.Ready])
        }
      }
    }
  }
}
