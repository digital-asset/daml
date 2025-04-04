// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.digitalasset.canton.admin.api.client.data.PruningSchedule
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}

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
      set_schedule("0 0 20 * * ?", 2.hours, retention) // run every evening at 8pm GMT for two hours
      set_schedule("0 /5 * * * ?", 1.minute, retention) // run every 5 minutes for one minute
      set_schedule("0 0 0 31 12 ? 2023", 1.day, retention) // run for one specific day
    // user-manual-entry-end: PruningScheduleExamples

  }

}

class PruningDocumentationIntegrationTestH2 extends PruningDocumentationIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory)
  )
}

//class PruningDocumentationIntegrationTestPostgres extends PruningDocumentationIntegrationTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(
//    new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
//  )
//}
