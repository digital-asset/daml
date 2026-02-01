// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.data.EitherT
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authenticity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.admin.api.client.data.*
import com.digitalasset.canton.admin.api.client.data.OnboardingRestriction.*
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SequencerTestHelper,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.SequencerServiceStub
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.SubmissionRequestRefused
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, TopologyChangeOp}
import com.digitalasset.canton.topology.{
  MediatorId,
  Member,
  ParticipantId,
  PhysicalSynchronizerId,
  SequencerId,
}
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
    EnvironmentDefinition.P2_S2M2
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.publicApi.maxTokenExpirationInterval)
            .replace(config.NonNegativeFiniteDuration.ofHours(1))
        ),
        // This is to prevent issues with the BFT orderer in tests with a simClock, as described in the Flaky Test Guide
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.timeTracker.observationLatency).replace(config.NonNegativeFiniteDuration.Zero)
        ),
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

  "sequencers" should {
    "be able to authenticate and talk to each other" in { implicit env =>
      import env.*

      val synchronizerId = daId
      val sender = sequencer2
      val receiver = sequencer1

      //  Wait until receiver sees sender's keys
      eventually() {
        receiver.topology.owner_to_key_mappings
          .list(
            filterKeyOwnerUid = sender.id.uid.toProtoPrimitive
          ) should not be empty
      }

      val senderCrypto = SynchronizerCrypto(sender.crypto, staticSynchronizerParameters1)
      val tokenForSender = requestToken(synchronizerId, sender.id, senderCrypto).futureValueUS.value

      assertRefused(sendSubmissionUsingToken(synchronizerId, sender.id, tokenForSender))
    }
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

        val simClock = environment.simClock.value

        // advancing the clock will cause the sequencer subscription authentication token to expire
        simClock.advance(JDuration.ofHours(2))

        // make sure the sequencer is synchronized with the sim clock
        sequencer1.underlying.value.sequencer.timeTracker
          .awaitTick(simClock.now)
          .foreach(_.futureValue)

        // fetch the synchronizer time; this replicates a CN test that identified an issue
        // (see https://github.com/DACH-NY/canton-network-internal/issues/2671 for details)
        eventually() {
          val synchronizerTime = participant1.testing.fetch_synchronizer_time(daId)
          synchronizerTime shouldBe >(CantonTimestamp.ofEpochSecond(2 * 3600))
        }

        // try pinging again
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          // will automatically retry until it works or times out
          assertPingSucceeds(participant1, participant2),
          LogEntry.assertLogSeq( // what we expect to see in the logs while the Ping is retrying
            mustContainWithClue = Seq.empty,
            mayContain = Seq(
              // Since we advanced the sim clock, the connection did not have a chance to renew the authentication token before it expired.
              // Depending on the concurrency, if a resubscription is attempted before a new token has been obtained, it will fail and restart
              // the connection to obtain a new token, which will result in a temporary "no connection available" for submissions.
              _.warningMessage should include(
                "No connection available"
              )
            ),
          ),
        )
      }
    }

    "the authentication token is incorrect" should {
      // checks that a member can obtain a token, can send requests with it and gets back the right error types in the responses
      // checks that invalid tokens do not work
      // also checks that the token no longer works after logout
      def checkAuthentication(memberId: Member, crypto: SynchronizerCrypto, logout: () => Unit)(
          implicit env: FixtureParam
      ): Assertion = {
        import env.*

        val token = requestToken(daId, memberId, crypto).futureValueUS.value

        // First, do a positive test with the correct token (but invalid payload)
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

        // authentication accepted
        assertRefused(
          sendSubmissionUsingToken(daId, participant1, token)
        )
        // authentication rejected when p1 uses own token for p2
        assertUnauthenticated(sendSubmissionUsingToken(daId, participant2, token))
      }
    }

    "a logout has been performed" should {
      "reconnect automatically to the sequencer" in { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        assertPingSucceeds(participant1, participant1)
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            participant1.synchronizers.logout(daName)
            mediator1.sequencer_connection.logout()

            assertPingSucceeds(participant1, participant1)
          },
          LogEntry.assertLogSeq(
            mustContainWithClue = Seq.empty,
            mayContain = Seq(
              // The `logout` command will cause the sequencer connection to restart. If a submission happens during that time, it may
              // receive a temporary "no connection available" error.
              // The ping will eventually work because pings have an implicit retry mechanism that works even with sim clock.
              _.warningMessage should include("No connection available")
            ),
          ),
        )
      }
    }

    "deactivated participant" should {
      "not be able to use the synchronizer with future loginAfter, even with a previously valid token" in {
        implicit env =>
          import env.*

          // Active member gets a valid token
          val crypto = SynchronizerCrypto(participant1.crypto, staticSynchronizerParameters1)
          val validToken = requestToken(daId, participant1, crypto).futureValueUS.value
          val farInTheFuture = env.environment.clock.now.add(java.time.Duration.ofDays(10000))

          // Test that the existing token is accepted by the sequencer
          assertRefused(
            sendSubmissionUsingToken(daId, participant1, validToken)
          )
          sequencer1.topology.synchronisation.await_idle()

          // Deactivate the member by shifting loginAfter to a time far in the future
          synchronizerOwners1.foreach { owner =>
            owner.topology.participant_synchronizer_permissions.propose(
              synchronizerId = daId,
              participantId = participant1.id,
              permission = ParticipantPermission.Observation,
              loginAfter = Some(farInTheFuture),
              change = TopologyChangeOp.Replace,
            )
          }
          // Wait for queues to clear while suppressor is active
          sequencer1.topology.synchronisation.await_idle()

          // read the topology store to confirm the new loginAfter
          eventually() {
            val perm = sequencer1.topology.participant_synchronizer_permissions
              .find(daId, participant1.id)
            perm.flatMap(_.item.loginAfter) shouldBe Some(farInTheFuture)
          }

          // Test the existing token
          // the Sequencer should now reject it because the member is disabled
          assertUnauthenticated(
            sendSubmissionUsingToken(daId, participant1, validToken)
          )

          // Test if the participant can get a new token. The request should be denied
          val newAttempt = requestToken(daId, participant1, crypto).value.futureValueUS
          inside(newAttempt) { case Left(status) =>
            status.getCode shouldBe Code.PERMISSION_DENIED
          }
      }

      "not be able to use the synchronizer when removed, even with a previously valid token" in {
        implicit env =>
          import env.*

          // Activate synchronizer restrictions (allowlist only) and enable p1 on the synchronizer
          // setup: Enable restrictions and grant permission
          sequencer1.topology.synchronizer_parameters.propose_update(
            daId,
            _.update(onboardingRestriction = OnboardingRestriction.RestrictedOpen),
            mustFullyAuthorize = true,
          )
          sequencer1.topology.participant_synchronizer_permissions.propose(
            daId,
            participant1,
            ParticipantPermission.Submission,
            loginAfter = None, // restore loginAfter to None
            mustFullyAuthorize = true,
          )

          // Active member gets a valid token
          val crypto = SynchronizerCrypto(participant1.crypto, staticSynchronizerParameters1)
          val validToken = requestToken(daId, participant1, crypto).futureValueUS.value

          // Test that the existing token is accepted by the sequencer
          assertRefused(
            sendSubmissionUsingToken(daId, participant1, validToken)
          )

          // revoke the participant permission and verify refusal
          // wrap the whole block after revoke to suppress background "unable to find participantSynchronizerPermissions..." warnings
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            {
              sequencer1.topology.participant_synchronizer_permissions.revoke(
                synchronizerId = daId,
                participantId = participant1.id,
              )

              sequencer1.topology.synchronisation.await_idle()

              // check to make sure the state is reached and no permissions exist
              eventually() {
                sequencer1.topology.participant_synchronizer_permissions
                  .find(daId, participant1.id) shouldBe empty
              }

              // Test the existing token
              // the Sequencer should now reject it because the member is disabled
              assertUnauthenticated(
                sendSubmissionUsingToken(daId, participant1, validToken)
              )

              // Test if the participant can get a new token
              val newAttempt = requestToken(daId, participant1, crypto).value.futureValueUS

              inside(newAttempt) { case Left(status) =>
                status.getCode shouldBe Code.PERMISSION_DENIED
              }

              // restore UnrestrictedOpen permissions in the test environment
              sequencer1.topology.synchronizer_parameters.propose_update(
                daId,
                _.update(onboardingRestriction = UnrestrictedOpen),
                mustFullyAuthorize = true,
              )
              sequencer1.topology.synchronisation.await_idle()

              // reactivate the participant
              sequencer1.topology.participant_synchronizer_permissions.propose(
                daId,
                participant1,
                ParticipantPermission.Submission,
                loginAfter = None,
                mustFullyAuthorize = true,
              )
              sequencer1.topology.synchronisation.await_idle()
            },
            LogEntry.assertLogSeq(
              mustContainWithClue = Seq.empty,
              mayContain = Seq(
                _.warningMessage should include("Unable to find ParticipantSynchronizerPermission")
              ),
            ),
          )
      }
    }

    // test for the working of case 6 in MemberAuthenticationService.observed
    "deactivated sequencer" should {
      "be disconnected when removed from the group" in { implicit env =>
        import env.*
        val seq2Id = sequencer2.id // Target Sequencer 2 for removal
        val crypto = SynchronizerCrypto(sequencer2.crypto, staticSynchronizerParameters1)
        val crypto1 = SynchronizerCrypto(sequencer1.crypto, staticSynchronizerParameters1)
        testMemberRemoval(
          daId = daId,
          leaverIdentity = seq2Id,
          leaverCrypto = crypto,
          stayingMember = sequencer1.id,
          stayingCrypto = crypto1,
          proposer = sequencer1,
        )
      }
    }

    // test for the working of case 7 in MemberAuthenticationService.observed
    "deactivated mediator" should {
      "be disconnected when removed from its mediator group" in { implicit env =>
        import env.*
        val med2Id = mediator2.id // Target Mediator 2 for removal
        val crypto = SynchronizerCrypto(mediator2.crypto, staticSynchronizerParameters1)
        val crypto1 = SynchronizerCrypto(mediator1.crypto, staticSynchronizerParameters1)
        testMemberRemoval(
          daId = daId,
          leaverIdentity = med2Id,
          leaverCrypto = crypto,
          stayingMember = mediator1.id,
          stayingCrypto = crypto1,
          proposer = sequencer1,
        )
      }
    }

    // test for the working of case 5 in MemberAuthenticationService.observed
    "deactivated mediator group" should {
      "purge tokens when a group is removed" in { implicit env =>
        import env.*
        val judge = sequencer1
        val med1Id = mediator1.id
        val med2Id = mediator2.id
        val crypto1 = SynchronizerCrypto(mediator1.crypto, staticSynchronizerParameters1)
        val crypto2 = SynchronizerCrypto(mediator2.crypto, staticSynchronizerParameters1)

        // assert there is exactly one mediator group and capture it
        val initialGroup = judge.topology.mediators
          .list(
            filterSynchronizer = daId.logical.filterString
          )
          .loneElement

        // if mediator2 is in that group, evict it
        if (initialGroup.item.active.contains(med2Id)) {
          val remainingActive = initialGroup.item.active.filter(_ != med2Id)

          judge.topology.mediators.propose(
            synchronizerId = daId.logical,
            group = initialGroup.item.group,
            threshold = initialGroup.item.threshold,
            active = remainingActive,
            mustFullyAuthorize = true,
          )
          judge.topology.synchronisation.await_idle()
        }
        // Setup complete: at this point it is guaranteed that mediator2 is not active in any group

        // Add Mediator 2 (no group assigned) to a new group (Group 1)
        judge.topology.mediators.propose(
          synchronizerId = daId.logical,
          group = NonNegativeInt.tryCreate(1),
          threshold = PositiveInt.one,
          active = Seq(med2Id),
          mustFullyAuthorize = true,
        )
        judge.topology.synchronisation.await_idle()

        // obtain tokens for both mediators
        val token1 = requestToken(daId, med1Id, crypto1).futureValueUS.value
        val token2 = requestToken(daId, med2Id, crypto2).futureValueUS.value

        //  Remove Group 1 entirely to trigger case 5 on observed
        judge.topology.mediators.remove_group(
          synchronizerId = daId.logical,
          group = com.digitalasset.canton.config.RequireTypes.NonNegativeInt.tryCreate(1),
          store = Some(daId),
        )

        judge.topology.synchronisation.await_idle()

        // Verify Mediator 2 is purged (Case 5 logic)
        eventually() {
          assertUnauthenticated(sendSubmissionUsingToken(daId, med2Id, token2))

          // Verify that Mediator 1 is still able to submit
          assertRefused(sendSubmissionUsingToken(daId, med1Id, token1))
        }
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

  // helper for removal of individual sequencers and mediators from their groups
  // do not use for participants
  // parameters include one of the members remaining in the group and their crypto object
  private def testMemberRemoval[T <: Member](
      daId: PhysicalSynchronizerId,
      leaverIdentity: T,
      leaverCrypto: SynchronizerCrypto,
      stayingMember: T,
      stayingCrypto: SynchronizerCrypto,
      proposer: LocalSequencerReference,
  )(implicit ec: ExecutionContext): Unit = {
    val token = requestToken(daId, leaverIdentity, leaverCrypto).futureValueUS.value

    val stayingToken = requestToken(daId, stayingMember, stayingCrypto).futureValueUS.value

    // Verify that the token works initially
    assertRefused(sendSubmissionUsingToken(daId, leaverIdentity, token))

    leaverIdentity match {
      case _: MediatorId =>
        val activeMediator = stayingMember.asInstanceOf[MediatorId]
        //  Trigger Case 7 in observed by updating the group with a Replace transaction for mediators
        proposer.topology.mediators.propose(
          synchronizerId = daId.logical,
          threshold = PositiveInt.one,
          active = Seq(activeMediator), // mediator2 is the "leaver"
          group = NonNegativeInt.zero,
          signedBy = None,
          store = Some(daId),
          mustFullyAuthorize = true,
        )
      case _: SequencerId =>
        //  Trigger Case 6 in observed by updating the group with a Replace transaction
        val activeSequencer = stayingMember.asInstanceOf[SequencerId]
        proposer.topology.sequencers.propose(
          synchronizerId = daId.logical,
          threshold = PositiveInt.one,
          active = Seq(activeSequencer), // sequencer2 is the "leaver"
          signedBy = None,
          store = Some(daId),
          mustFullyAuthorize = true,
        )

      case _: ParticipantId =>
        // resolve "match may not be exhaustive" error
        fail(s"testMemberRemoval does not support ParticipantId.")
    }

    // Wait for the topology update to be processed
    proposer.topology.synchronisation.await_idle()

    eventually() {
      // Verify that the leaver's token is now purged by sending the existing token
      assertUnauthenticated(sendSubmissionUsingToken(daId, leaverIdentity, token))

      // Verify that the remaining member is still able to submit
      assertRefused(sendSubmissionUsingToken(daId, stayingMember, stayingToken))
    }

    // Verify leaver cannot obtain a new token
    val reAuthAttempt = requestToken(daId, leaverIdentity, leaverCrypto).value.futureValueUS
    inside(reAuthAttempt) { case Left(status) =>
      status.getCode shouldBe io.grpc.Status.Code.PERMISSION_DENIED
    }
    proposer.topology.synchronisation.await_idle()

  }
}

class AuthenticationTokenIntegrationTestInMemory extends AuthenticationTokenIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class AuthenticationTokenIntegrationTestPostgres extends AuthenticationTokenIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
