// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.data.EitherT
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authenticity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SequencerTestHelper,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.SequencerServiceStub
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.SubmissionRequestRefused
import com.digitalasset.canton.topology.{Member, ParticipantId, PhysicalSynchronizerId}
import io.grpc.*
import io.grpc.Status.Code
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.time.Duration as JDuration
import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
trait AuthenticationTokenIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite {

  lazy private val securityAsset: SecurityTest =
    SecurityTest(property = Authenticity, asset = "synchronizer public api client")

  private var daChannel: ManagedChannel = _
  private var sequencerServiceStub: SequencerServiceStub = _
  private val confirmationRequestsMaxRate = NonNegativeInt.tryCreate(10000)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.publicApi.maxTokenExpirationInterval)
            .replace(config.NonNegativeFiniteDuration.ofHours(1))
        ),
        // TODO(i26481): Enable new connection pool (issues with staticTime)
        ConfigTransforms.disableConnectionPool,
      )
      .withSetup { implicit env =>
        import env.*

        synchronizerOwners1.foreach {
          _.topology.synchronizer_parameters.propose_update(
            synchronizerId = daId,
            _.update(confirmationRequestsMaxRate = confirmationRequestsMaxRate),
          )
        }
        daChannel = SequencerTestHelper.createChannel(
          sequencer1,
          loggerFactory,
          executionContext,
        )
        sequencerServiceStub = new SequencerServiceStub(daChannel)
      }

  override def afterAll(): Unit = {
    SequencerTestHelper.closeChannel(daChannel, logger, getClass.getSimpleName)
    super.afterAll()
  }

  "A member" when {
    "the authentication token is correct" can {
      "use the synchronizer" taggedAs securityAsset.setHappyCase(
        "use a correct authentication token"
      ) in { implicit env =>
        import env.*

        Seq(participant1, participant2).foreach {
          _.synchronizers.connect_local(sequencer1, alias = daName)
        }

        assertPingSucceeds(participant1, participant1)
        assertPingSucceeds(participant2, participant1)

      // TODO(i2794) change this test back to run a concurrent bong while moving the sim-clock
      // 1. start the bongs,
      // 2. run one ping (to ensure that the bongs have started)
      // 3. move the sim-clock
      // 4. wait for the bongs to conclude
      }
    }

    "the authentication token has expired" should {
      "renew the token" taggedAs securityAsset.setHappyCase(
        "renew an expired authentication token"
      ) in { implicit env =>
        import env.*

        // advancing the clock will cause the sequencer subscription authentication token to expire
        environment.simClock.foreach(_.advance(JDuration.ofHours(2)))

        // try pinging again
        assertPingSucceeds(participant1, participant2)
      }
    }

    "the authentication token is incorrect" should {
      def checkAuthentication(memberId: Member, crypto: SynchronizerCrypto, logout: () => Unit)(
          implicit env: FixtureParam
      ): Assertion = {
        import env.*

        val token = requestToken(daId, memberId, crypto).futureValueUS.value

        // First, do a positive test
        assertRefused(
          sendSubmissionUsingToken(daId, memberId, token)
        )

        // Test missing call credentials
        assertUnauthenticated(
          SequencerTestHelper.sendSubmissionRequest(sequencerServiceStub, memberId)
        )

        // Test incorrect token
        val incorrectToken = AuthenticationToken.generate(new SymbolicPureCrypto)
        assertUnauthenticated(sendSubmissionUsingToken(daId, memberId, incorrectToken))

        // Test reusing token after logout
        logout()
        assertUnauthenticated(sendSubmissionUsingToken(daId, memberId, token))
      }

      "reject the request (participant case)" taggedAs securityAsset
        .setAttack(
          Attack(
            actor = "Network participant that can reach the public api",
            threat = "Impersonate a participant",
            mitigation = "Reject requests without a known authentication token",
          )
        ) in { implicit env =>
        import env.*

        checkAuthentication(
          participant1.id,
          SynchronizerCrypto(participant1.crypto, staticSynchronizerParameters1),
          logout = () => participant1.synchronizers.logout(daName),
        )
      }

      "reject the request (mediator case)" taggedAs securityAsset
        .setAttack(
          Attack(
            actor = "Network participant that can reach the public api",
            threat = "Impersonate a mediator",
            mitigation = "Reject requests without a known authentication token",
          )
        ) in { implicit env =>
        import env.*

        checkAuthentication(
          mediator1.id,
          SynchronizerCrypto(mediator1.crypto, staticSynchronizerParameters1),
          logout = () => mediator1.sequencer_connection.logout(),
        )
      }
    }

    "not enabled on the synchronizer" must {
      "not be able to obtain an authentication token" taggedAs securityAsset
        .setAttack(
          Attack(
            actor = "Network participant that can reach the public api",
            threat = "Try to obtain an authentication token",
            mitigation = "Refuse to issue authentication tokens for non-permissioned nodes",
          )
        ) in { implicit env =>
        import env.*

        val nonpermissionedId = ParticipantId.tryFromProtoPrimitive("PAR::roeschti::gericht")

        inside(
          requestToken(
            daId,
            nonpermissionedId,
            SynchronizerCrypto(participant1.crypto, staticSynchronizerParameters1),
          ).value.futureValueUS
        ) { case Left(status) =>
          status.getCode shouldBe Code.PERMISSION_DENIED
        }
      }
    }

    "the authentication token is correct" should {
      "not be able to impersonate a different node" taggedAs securityAsset
        .setAttack(
          Attack(
            actor = "An authenticated public api client",
            threat = "Impersonate a different public api client",
            mitigation =
              "Reject requests if the sender is not the owner of the authentication token",
          )
        ) in { implicit env =>
        import env.*

        val token = requestToken(
          daId,
          participant1,
          SynchronizerCrypto(participant1.crypto, staticSynchronizerParameters1),
        ).value.futureValueUS.value

        assertRefused(
          sendSubmissionUsingToken(daId, participant1, token)
        )

        assertUnauthenticated(sendSubmissionUsingToken(daId, participant2, token))
      }
    }

    "a logout has been performed" should {
      "reconnect automatically to the sequencer" in { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)

        assertPingSucceeds(participant1, participant1)

        participant1.synchronizers.logout(daName)
        mediator1.sequencer_connection.logout()

        assertPingSucceeds(participant1, participant1)
      }
    }
  }

  private def requestToken(
      synchronizerId: PhysicalSynchronizerId,
      memberId: Member,
      crypto: SynchronizerCrypto,
  )(implicit ec: ExecutionContext): EitherT[FutureUnlessShutdown, Status, AuthenticationToken] =
    SequencerTestHelper
      .requestToken(
        daChannel,
        synchronizerId,
        memberId,
        crypto,
        testedProtocolVersion,
        loggerFactory,
      )

  private def sendSubmissionUsingToken(
      synchronizerId: PhysicalSynchronizerId,
      memberId: Member,
      token: AuthenticationToken,
  )(implicit ec: ExecutionContext): Future[Unit] =
    SequencerTestHelper
      .sendSubmissionRequest(
        sequencerServiceStub.withCallCredentials(
          SequencerTestHelper.mkCallCredentials(synchronizerId, memberId, token)
        ),
        memberId,
      )

  private def assertUnauthenticated(outcome: Future[?]): Assertion =
    inside(outcome.failed.futureValue) { case sre: StatusRuntimeException =>
      sre.getStatus.getCode shouldBe Code.UNAUTHENTICATED
    }

  private def assertRefused(outcome: Future[?]): Assertion =
    inside(outcome.failed.futureValue) { case sre: StatusRuntimeException =>
      sre.getStatus.getCode shouldBe SubmissionRequestRefused.category.grpcCode.value
    }
}

class AuthenticationTokenIntegrationTestInMemory extends AuthenticationTokenIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class AuthenticationTokenIntegrationTestPostgres extends AuthenticationTokenIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

}
