// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.digitalasset.canton.admin.api.client.data.PruningSchedule
import com.digitalasset.canton.console.commands.PruningSchedulerAdministration
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import io.grpc.stub.AbstractStub

/** The PruningDocumentationIntegrationTest illustrates best practices on how to prune the canton
  * nodes for freeing up storage space.
  *
  * (If pruning was not an enterprise feature, this would be an ExampleIntegrationTest.)
  */
abstract class PruningDocumentationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  "test that ensures the documentation is up to date with how to configure scheduled pruning" in {
    implicit env =>
      import env.*
      val participant = participant1
      val sequencer = sequencer1
      val mediator = mediator1
      import com.digitalasset.canton.config
      import scala.concurrent.duration.*

      val _ = (participant, sequencer, mediator)

      @scala.annotation.unused
      def checkCompiles(): Unit = {
        // user-manual-entry-begin: AutoPruneAllNodes
        participant.pruning.set_schedule("0 0 8 ? * SAT", 8.hours, 90.days)
        sequencer.pruning.set_schedule("0 0 8 ? * SAT", 8.hours, 90.days)
        mediator.pruning.set_schedule("0 0 8 ? * SAT", 8.hours, 90.days)
        // user-manual-entry-end: AutoPruneAllNodes
      }

      def set_schedule(
          cron: String,
          maxDuration: config.PositiveDurationSeconds,
          retention: config.PositiveDurationSeconds,
      ): Unit =
        PruningSchedule(cron, maxDuration, retention).discard

      val retention = 90.days

      // user-manual-entry-begin: PruningScheduleExamples
      // Prune every evening at 8pm GMT for two hours
      set_schedule("0 0 20 * * ?", 2.hours, retention)

      // Prune every 5 minutes for one minute
      set_schedule("0 /5 * * * ?", 1.minute, retention)

      // Prune for one specific day
      set_schedule("0 0 0 31 12 ? 2025", 1.day, retention)
      // user-manual-entry-end: PruningScheduleExamples

      def testAutomaticPruningFunctions[T <: AbstractStub[T]](
          pruning: PruningSchedulerAdministration[T]
      ): Unit = {
        // user-manual-entry-begin: AutoPruneAllMethods
        // Set a pruning schedule with a duration and a retention period.
        pruning.set_schedule("0 0 8 ? * SAT", 8.hours, 90.days)

        // Retrieve the current pruning schedule returning `None` if no schedule is set.
        val pruningSchedule = pruning.get_schedule()

        // Set individual fields to modify the existing pruning schedule.
        pruning.set_cron("0 /5 * * * ?")
        pruning.set_retention(30.days)
        pruning.set_max_duration(2.hours)

        // Clear the pruning schedule disabling automatic pruning on a specific node.
        pruning.clear_schedule()
        // user-manual-entry-end: AutoPruneAllMethods

        pruningSchedule.discard
      }

      testAutomaticPruningFunctions(participant1.pruning)
  }

  "test that ensures the 3.x participant documentation is up to date with how to configure scheduled pruning" in {
    implicit env =>
      import env.*
      val participant = participant1
      import scala.concurrent.duration.*

      val _ = participant

      // user-manual-entry-begin: AutoPruneParticipantNode
      participant.pruning.set_participant_schedule(
        cron = "0 0 8 ? * SAT",
        maxDuration = 8.hours,
        retention = 90.days,
        pruneInternallyOnly = false,
      )
      // user-manual-entry-end: AutoPruneParticipantNode
      participant.pruning.clear_schedule()
  }
}

class PruningDocumentationIntegrationTestH2 extends PruningDocumentationIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

//class PruningDocumentationIntegrationTestPostgres extends PruningDocumentationIntegrationTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(
//    new UseBftSequencer(loggerFactory)
//  )
//}
