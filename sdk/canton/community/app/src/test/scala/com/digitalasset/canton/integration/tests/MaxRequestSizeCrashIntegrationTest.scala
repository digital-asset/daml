// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.ParticipantReference
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
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.MalformedRequest
import com.digitalasset.canton.util.ResourceUtil.withResource
import monocle.macros.syntax.lens.*

import java.util.UUID
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

sealed abstract class MaxRequestSizeCrashIntegrationTest
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
    // user-manual-entry-begin: StopCanton
    participants.all.synchronizers.disconnect(daName)
    nodes.local.stop()
    // user-manual-entry-end: StopCanton
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
  private val lowMaxRequestSize = NonNegativeInt.zero

  "Canton" should {
    "recover from failure due to too small request size " in { implicit env =>
      import env.*
      participant1.dars.upload(CantonTestsPath)

      // verify the ping is successful
      participant1.health.ping(participant1)

      // change maxRequestSize
      synchronizerOwners1.foreach(
        _.topology.synchronizer_parameters
          .propose_update(
            synchronizerId = daId,
            _.update(maxRequestSize = lowMaxRequestSize.unwrap),
          )
      )

      eventually() {
        forAll(nodes.all) {
          _.topology.synchronizer_parameters
            .latest(daId)
            .maxRequestSize
            .value shouldBe lowMaxRequestSize.unwrap
        }
      }

      val matchError =
        s"MaxViewSizeExceeded\\(view size = .*, max request size configured = .*\\)."

      def submitCommand(p: ParticipantReference) = {
        val commandId = s"submit-async-dummy-${UUID.randomUUID().toString}"

        val commandF = Future {
          p.ledger_api.javaapi.commands.submit(
            Seq(p.adminParty),
            new Dummy(p.adminParty.toProtoPrimitive).create.commands.asScala.toSeq,
            commandId = commandId,
          )
        }

        (commandId, commandF)
      }

      loggerFactory.assertLogs(
        {
          val (commandId, _) = submitCommand(env.participant1)

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
            val reason = deserializedError.context.get("reason").value
            deserializedError.code.id shouldBe MalformedRequest.id
            reason should include regex matchError
          }
        },
        _.errorMessage should include("INVALID_ARGUMENT/MALFORMED_REQUEST"),
      )

      /*
       restart Canton with overrideMaxRequestSize, so that max request size can be increased
       */
      setOverrideMaxRequestSizeWithNewEnv(env, overrideMaxRequestSize) { implicit newEnv =>
        import newEnv.*
        // we verify that the dynamic parameter is still set to the low value

        forAll(nodes.all)(
          _.topology.synchronizer_parameters.latest(daId).maxRequestSize == lowMaxRequestSize
        )

        val newMaxRequestSize = NonNegativeInt.tryCreate(60_000)
        newMaxRequestSize should not be lowMaxRequestSize

        synchronizerOwners1.foreach(
          _.topology.synchronizer_parameters
            .propose_update(synchronizerId = daId, _.update(maxRequestSize = newMaxRequestSize))
        )

        eventually() {
          forAll(nodes.all) { member =>
            member.topology.synchronizer_parameters
              .latest(daId)
              .maxRequestSize shouldBe newMaxRequestSize
          }
        }

        stop(newEnv)
      }

      restart

      // Use the old env (without the override), submission should work
      val (_, submissionF) = submitCommand(participant1)
      submissionF.futureValue.discard
    }
  }
}

class MaxRequestSizeCrashReferenceIntegrationIntegrationTestPostgres
    extends MaxRequestSizeCrashIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

class MaxRequestSizeCrashBftOrderingIntegrationIntegrationTestPostgres
    extends MaxRequestSizeCrashIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
