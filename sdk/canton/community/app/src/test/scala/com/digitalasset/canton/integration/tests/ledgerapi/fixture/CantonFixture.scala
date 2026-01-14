// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.fixture

import com.digitalasset.canton.config
import com.digitalasset.canton.config.{AuthServiceConfig, PositiveDurationSeconds}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.integration.tests.ledgerapi.auth.SandboxRequiringAuthorizationFuns
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetup,
  IsolatedEnvironments,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.lifecycle.LifeCycle.CloseableChannel
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import io.grpc.ManagedChannel
import monocle.macros.syntax.lens.*
import org.scalatest.BeforeAndAfterEach

import java.io.File
import java.net.InetAddress
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.DurationInt

trait CantonFixture extends CantonFixtureAbstract with SharedEnvironment {
  override def afterAll(): Unit =
    try {
      // when an unbounded stream is fed a forceful shutdown is needed
      channels.values.foreach(_.channel.shutdownNow())
      channels.values.foreach(_.close())
    } finally super.afterAll()

}

trait CantonFixtureIsolated
    extends CantonFixtureAbstract
    with IsolatedEnvironments
    with BeforeAndAfterEach {
  override protected def afterEach() = channels.values.foreach(_.close())
}

trait CantonFixtureAbstract
    extends CommunityIntegrationTest
    with SandboxRequiringAuthorizationFuns {
  this: EnvironmentSetup =>

  protected def darFile = new File(CantonTestsPath)

  private def getLAPIClientConfig(participant: LocalParticipantReference) =
    participant.config.ledgerApi.clientConfig

  protected def baseEnvironmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P1_S1M1

  override def environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition
      .addConfigTransforms(
        ConfigTransforms.updateParticipantConfig("participant1") {
          _.focus(_.ledgerApi.authServices).replace(
            Seq(
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = jwtSecret,
                  targetAudience = None,
                  targetScope = None,
                )
            )
          )
        },
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.ledgerApi.userManagementService.cacheExpiryAfterWriteInSeconds)
            .replace(UserManagementCacheExpiryInSeconds)
        ),
        ConfigTransforms.updateParticipantConfig("participant1")(
          ConfigTransforms.useTestingTimeService
        ),
        // Setting the token lifetime to 2 hours makes the tests less flaky even under duress,
        // as we have tests that use tokens on both sides of the lifetime boundary.
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.ledgerApi.maxTokenLifetime)
            .replace(config.NonNegativeDuration(2.hours))
        ),
        ConfigTransforms.useStaticTime,
        // to enable tests related to pruning
        ConfigTransforms.updateMaxDeduplicationDurations(java.time.Duration.ofSeconds(0)),
      )
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        // to enable tests related to pruning
        env.runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .propose_update(
              synchronizer.synchronizerId,
              _.update(reconciliationInterval = PositiveDurationSeconds.ofSeconds(1)),
            )
        )

        createChannel(participant1)

        participant1.dars.upload(CantonTestsPath, synchronizerId = daId)

        additionalEnvironmentSetup(env)
      }

  protected def additionalEnvironmentSetup(testConsoleEnvironment: TestConsoleEnvironment): Unit =
    ()

  protected val channels = TrieMap[String, CloseableChannel]()

  def channel: ManagedChannel = getChannel("participant1")

  protected def createChannel(
      participant: LocalParticipantReference
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    val cc = getLAPIClientConfig(participant)
    val channel = ClientChannelBuilder.createChannelBuilderToTrustedServer(cc).build()
    val closeableChannel = new CloseableChannel(
      channel,
      logger,
      s"CantonFixture",
    )
    channels(participant.name) = closeableChannel
  }

  def getChannel(participantName: String): ManagedChannel =
    channels.get(participantName) match {
      case None => fail(s"no channel is available for $participantName")
      case Some(channel) => channel.channel
    }

  protected val ExpectedAudience = "ExpectedTargetAudience"
  protected val ExpectedScope = "ExpectedTargetScope/With-Dash/And_Underscore"
  protected val UserManagementCacheExpiryInSeconds = 1

  protected def getPartyId(partyHint: String)(implicit
      env: TestConsoleEnvironment
  ): String =
    env.participant1.parties.find(partyHint).toProtoPrimitive

  protected def serverHost: String = InetAddress.getLoopbackAddress.getHostName
}
