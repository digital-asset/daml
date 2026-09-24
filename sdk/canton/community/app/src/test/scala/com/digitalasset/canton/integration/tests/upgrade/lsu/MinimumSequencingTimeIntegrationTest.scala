// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.networking.grpc.GrpcError.GrpcRequestRefusedByServer
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.RequestRefused
import com.digitalasset.canton.sequencing.protocol.Batch
import com.digitalasset.canton.sequencing.protocol.SendAsyncError.{
  SendAsyncErrorDirect,
  SendAsyncErrorGrpc,
}
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.SubmissionRequestRefused
import com.digitalasset.canton.tracing.TraceContext
import monocle.syntax.all.*
import org.slf4j.event.Level

/*
 * This test is used to test the logical synchronizer upgrade.
 * It uses 1 participant, 1 sequencer, and 1 mediator.
 */
abstract class MinimumSequencingTimeIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with LogicalUpgradeUtils {

  override protected def testName: String = "logical-synchronizer-upgrade"

  registerPlugin(new UsePostgres(loggerFactory))

  private val sequencingTimeLowerBoundExclusive = CantonTimestamp.Epoch.plusSeconds(60)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .addConfigTransforms(
        ConfigTransforms
          .updateAllSequencerConfigs_(
            _.focus(_.parameters.sequencingTimeLowerBoundExclusive)
              .replace(Some(sequencingTimeLowerBoundExclusive))
          )
      )

  "Logical synchronizer upgrade" should {
    "initialize the nodes for the upgraded synchronizer" in { implicit env =>
      import env.*

      // advance the time to some point before the minimum sequencing time
      environment.simClock.value.advanceTo(sequencingTimeLowerBoundExclusive.minusSeconds(10))

      val errorMessage =
        s"Cannot submit before or at the lower bound for sequencing time $sequencingTimeLowerBoundExclusive; time is currently at ${environment.clock.now}"
      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        {
          sequencer1.underlying.value.sequencer.client
            .send(Batch(Nil, testedProtocolVersion))(TraceContext.empty, MetricsContext.Empty)
            .futureValueUS shouldBe Left(RequestRefused(SendAsyncErrorDirect(errorMessage)))

          mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator.sequencerClient
            .send(Batch(Nil, testedProtocolVersion))(TraceContext.empty, MetricsContext.Empty)
            .futureValueUS should matchPattern {
            case Left(
                  RequestRefused(
                    SendAsyncErrorGrpc(GrpcRequestRefusedByServer(_, _, _, _, Some(cantonError)))
                  )
                )
                if cantonError.code.id == SubmissionRequestRefused.id && cantonError.cause == errorMessage =>
          }

          // advance the clock to the minimum sequencing time
          environment.simClock.value.advanceTo(sequencingTimeLowerBoundExclusive.immediateSuccessor)

          eventually() {
            waitForTargetTimeOnSequencer(sequencer1, sequencingTimeLowerBoundExclusive)
          }
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              entry => {
                entry.shouldBeCantonErrorCode(SubmissionRequestRefused)
                entry.warningMessage should include(errorMessage)
              },
              "submission request refused warning",
            )
          )
        ),
      )

      participant1.synchronizers.connect_local(sequencer1, daName)
      participant1.health.maybe_ping(participant1) should not be empty
    }
  }
}

class MinimumSequencingTimeReferenceIntegrationTest extends MinimumSequencingTimeIntegrationTest

class MinimumSequencingTimeBftOrderingIntegrationTest extends MinimumSequencingTimeIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
}
