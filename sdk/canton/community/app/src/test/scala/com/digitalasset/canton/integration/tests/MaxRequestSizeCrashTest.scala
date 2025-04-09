// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.annotations.UnstableTest
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou.Dummy
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessor
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.util.ResourceUtil.withResource
import monocle.macros.syntax.lens.*

import java.util.UUID
import scala.jdk.CollectionConverters.*

sealed abstract class MaxRequestSizeCrashTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S2M2
      .addConfigTransforms(
        // Disable retries in the ping service so that any submission error is reported reliably
        // This makes the log messages more deterministic.
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.adminWorkflow.retries).replace(false)
        )
      )
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      }

  final private def setOverrideMaxRequestSizeWithNewEnv(
      oldEnv: TestConsoleEnvironment,
      overrideMaxRequestSize: NonNegativeInt,
  )(f: TestConsoleEnvironment => Unit): Unit = {
    stop(oldEnv)
    val newEnv = manualCreateEnvironmentWithPreviousState(
      oldEnv.actualConfig,
      _ =>
        ConfigTransforms.applyMultiple(
          Seq(
            ConfigTransforms
              .updateAllSequencerConfigs_(
                _.focus(_.publicApi.overrideMaxRequestSize)
                  .replace(Some(overrideMaxRequestSize))
              ),
            ConfigTransforms
              .updateAllMediatorConfigs_(
                _.focus(_.sequencerClient.overrideMaxRequestSize)
                  .replace(Some(overrideMaxRequestSize))
              ),
            ConfigTransforms.updateAllParticipantConfigs_(
              _.focus(_.sequencerClient.overrideMaxRequestSize)
                .replace(Some(overrideMaxRequestSize))
            ),
          )
        )(oldEnv.actualConfig),
    )
    withResource(newEnv) { env =>
      handleStartupLogs(restart(newEnv))
      f(env)
    }
  }

  final private def stop(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    // user-manual-entry-begin: StopDistributedCanton user-manual-entry-begin: StopNonDistributedCanton
    participants.all.synchronizers.disconnect(daName)
    nodes.local.stop()
    // user-manual-entry-end: StopDistributedCanton user-manual-entry-end: StopNonDistributedCanton
  }

  final private def restart(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    // user-manual-entry-begin: RestartCanton
    nodes.local.start()
    participants.all.synchronizers.reconnect_all()
    // user-manual-entry-end: RestartCanton
  }

  // High request size
  private val overrideMaxRequestSize = NonNegativeInt.tryCreate(30_000)
  // Request size chosen so that even TimeProof requests are rejected
  private val lowMaxRequestSize = MaxRequestSize(NonNegativeInt.zero)

  "Canton" should {
    "recover from failure due to too small request size " in { implicit env =>
      import env.*
      participant1.dars.upload(CantonTestsPath)

      // verify the ping is successful
      assertPingSucceeds(participant1, participant1)
      // change maxRequestSize
      for (owner <- synchronizerOwners1) {
        owner.topology.synchronizer_parameters
          .propose_update(
            synchronizerId = daId,
            _.update(maxRequestSize = lowMaxRequestSize.unwrap),
          )
      }
      eventually() {
        forAll(nodes.local) { owner =>
          forAll(owner.topology.synchronizer_parameters.list(daId).map(_.item.maxRequestSize))(
            _ == lowMaxRequestSize
          )
        }
      }

      val commandId = s"submit-async-dummy-${UUID.randomUUID().toString}"
      val matchError =
        s"RequestInvalid\\(Batch size \\(\\d+ bytes\\) is exceeding maximum size \\(MaxRequestSize\\(${lowMaxRequestSize.unwrap}\\) bytes\\) for synchronizer .*\\)"
      val matchLog = s"Failed to submit submission due to Error\\($matchError\\)"

      loggerFactory.assertLogs(
        {
          participant1.ledger_api.javaapi.commands.submit_async(
            Seq(participant1.adminParty),
            new Dummy(participant1.adminParty.toProtoPrimitive).create.commands.asScala.toSeq,
            commandId = commandId,
          )
          eventually() {
            val completion = participant1.ledger_api.completions
              .list(
                partyId = participant1.adminParty,
                atLeastNumCompletions = 1,
                beginOffsetExclusive = 0L,
                filter = _.commandId == commandId,
              )
              .headOption
              .value
            val status = completion.status.value
            val deserializedError = DecodedCantonError.fromGrpcStatus(status).value
            val sendError = deserializedError.context.get("sendError").value
            deserializedError.code.id shouldBe TransactionProcessor.SubmissionErrors.SequencerRequest.id
            sendError should fullyMatch regex matchError
          }
        },
        _.warningMessage should include regex matchLog,
      )

      // restart Canton with overrideMaxRequestSize
      setOverrideMaxRequestSizeWithNewEnv(env, overrideMaxRequestSize) { implicit newEnv =>
        import newEnv.*
        // we verify that the dynamic parameter is still set to the low value
        forAll(synchronizerOwners1) { owner =>
          forAll(owner.topology.synchronizer_parameters.list(daId).map(_.item.maxRequestSize))(
            _ == lowMaxRequestSize
          )
        }
        // we verify that this time the dynamic parameter is ignored
        participant1.ledger_api.javaapi.commands
          .submit(
            Seq(participant1.adminParty),
            new Dummy(participant1.adminParty.toProtoPrimitive).create.commands.asScala.toSeq,
          )
          .discard
      }
    }
  }
}

class MaxRequestSizeCrashReferenceIntegrationTestPostgres extends MaxRequestSizeCrashTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

@UnstableTest // TODO(#24987)
class MaxRequestSizeCrashBftOrderingIntegrationTestPostgres extends MaxRequestSizeCrashTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
