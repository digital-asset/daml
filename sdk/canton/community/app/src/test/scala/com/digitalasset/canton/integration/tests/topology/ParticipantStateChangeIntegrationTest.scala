// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.FuncTest
import com.daml.test.evidence.tag.Security.SecurityTest.Property
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.{CommandFailure, LocalInstanceReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.NoSynchronizerOnWhichAllSubmittersCanSubmit
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceInconsistentConnectivity,
  SyncServiceSynchronizerDisabledUs,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.MemberAccessDisabled
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*

@SuppressWarnings(Array("org.wartremover.warts.Null"))
trait ParticipantStateChangeIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite
    with AccessTestScenario {

  protected val synchronizerAuthenticity: SecurityTest =
    SecurityTest(Property.Authenticity, "Synchronizer node")
  protected val synchronizerAvailability: SecurityTest =
    SecurityTest(Property.Availability, "Synchronizer node")

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1

  private def rollSigningKey(
      node: LocalInstanceReference
  ): Unit = {

    def keys() = node.topology.owner_to_key_mappings
      .list(
        filterKeyOwnerUid = node.id.member.filterString,
        filterKeyOwnerType = Some(node.id.member.code),
      )
      .flatMap(_.item.keys.forgetNE)
      .toSet
    val before = keys()
    node.keys.secret.rotate_node_keys()
    clue(s"${node.name} keys are distinct after rolling") {
      eventually() {
        forAll(before) { k =>
          keys() should not contain (k)
        }
      }
    }
  }

  private def changeParticipantPermission(
      participantId: ParticipantId,
      permission: ParticipantPermission,
      loginAfter: Option[Long],
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    clue(s"changing $participantId to $permission with loginAfter $loginAfter") {
      sequencer1.topology.participant_synchronizer_permissions
        .propose(
          sequencer1.synchronizer_id,
          participantId,
          permission,
          mustFullyAuthorize = true,
          loginAfter = loginAfter.map(env.environment.clock.now.plusSeconds(_)),
        )
    }
  }

  def waitForChangeToBecomeEffective(
      permission: ParticipantPermission
  )(implicit env: TestConsoleEnvironment) = {
    import env.*
    Seq(participant1, participant2).foreach { p =>
      clue(s"${p.id} validates that $participant1 has $permission") {
        eventually() {
          p.topology.participant_synchronizer_permissions
            .find(sequencer1.synchronizer_id, participant1)
            .value
            .item
            .permission shouldBe permission
        }
      }

    }
  }

  "roll synchronizer keys while a participant is connected" taggedAs synchronizerAuthenticity
    .setAttack(
      Attack(
        actor = "Active attacker on the network",
        threat = "Impersonate the synchronizer node with compromised keys",
        mitigation = "Roll signing keys of synchronizer entities",
      )
    ) in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, daName)

    assertPingSucceeds(participant1, participant1)

    // roll sequencer key
    clue("we can roll the sequencer key") {
      rollSigningKey(sequencer1)
      assertPingSucceeds(participant1, participant1)
    }

    clue("we can roll the mediator key") {
      rollSigningKey(mediator1)
      assertPingSucceeds(participant1, participant1)

    }

  }

  "A participant can freshly register at a synchronizer that has rolled its signing keys" taggedAs_ {
    synchronizerAuthenticity.setHappyCase(_)
  } in { implicit env =>
    import env.*

    // connect participant2
    participant2.synchronizers.connect_local(sequencer1, daName)
    // test that p2 <-> p1 ping works
    assertPingSucceeds(participant2, participant1)
  }

  "A disabled participant must get kicked out" taggedAs synchronizerAvailability.setAttack(
    Attack(
      actor = "A malicious participant or application",
      threat = "Overload the synchronizer",
      mitigation = "Disable the participant",
    )
  ) in { implicit env =>
    import env.*

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        // deactivate participant1 on the synchronizer.
        // the sequencer should automatically disconnect participant1
        changeParticipantPermission(participant1.id, ParticipantPermission.Submission, Some(300))
        eventually() {
          participant1.synchronizers.is_connected(sequencer1.synchronizer_id) shouldBe false
        }
      },
      forEvery(_) {
        _.message should (include(MemberAccessDisabled(participant1.id).reason) or
          include(SyncServiceSynchronizerDisabledUs.id) or
          include("Aborted fetching token due to my node shutdown") or
          include("Token refresh aborted due to shutdown"))
      },
    )
  }

  "After rolling signing keys of synchronizer entities several times, a previously disabled participant can be enabled and can reconnect to the synchronizer" taggedAs_ {
    synchronizerAvailability.setHappyCase(_)
  } in { implicit env =>
    import env.*

    // roll key a few times, toggle the participant state and flush the system
    // this will create the situation where the essential state changed multiple times with a few snapshots being
    // queued while the participant was offline
    (0 to 2).foreach { idx =>
      logger.info(s"rollSigningKey iteration #$idx")
      rollSigningKey(sequencer1)
      rollSigningKey(mediator1)
      rollSigningKey(sequencer1)
      rollSigningKey(mediator1)
    }

    clue("re-enable participant") {
      changeParticipantPermission(participant1.id, ParticipantPermission.Submission, None)

      participant1.synchronizers.reconnect_all()
      waitForChangeToBecomeEffective(ParticipantPermission.Submission)
    }

    // reconnect participant1
    clue("ping after reconnecting") {
      assertPingSucceeds(participant1, participant1)
    }
  }

  "A participant must refuse to connect to a synchronizer if the requested synchronizer id differs from the actual synchronizer id" taggedAs FuncTest(
    topics = Seq("topology management"),
    features = Seq("participant connects to a synchronizer"),
  ) in { implicit env =>
    import env.*

    // try to start participant3 with an invalid synchronizer id
    val conConfig = SynchronizerConnectionConfig.grpc(
      SequencerAlias.Default,
      daName,
      s"http://localhost:${sequencer1.config.publicApi.port}",
      manualConnect = true,
      synchronizerId = Some(
        SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("notcorrect::fingerprint")).toPhysical
      ),
    )

    // fail because synchronizer id is wrong
    assertThrowsAndLogsCommandFailures(
      participant3.synchronizers.connect_by_config(conConfig),
      _.commandFailureMessage should include(
        SyncServiceInconsistentConnectivity.id
      ),
    )

    val synchronizerId = sequencer1.synchronizer_id
    participant3.synchronizers.connect_by_config(
      conConfig.copy(synchronizerId = Some(synchronizerId))
    )

  }

  "A participant can connect to a synchronizer if the requested synchronizer id matches the actual synchronizer id" taggedAs
    FuncTest(
      topics = Seq("topology management"),
      features = Seq("participant connects to a synchronizer"),
    ) in { implicit env =>
      import env.*

      participant3.synchronizers.reconnect(daName)
      eventually() {
        participant3.synchronizers.is_connected(sequencer1.synchronizer_id) shouldBe true
      }
      assertPingSucceeds(participant3, participant2)
    }

  // confirming participant can not submit
  "A participant with confirmation or observation rights can not submit or confirm respectively" taggedAs FuncTest(
    topics = Seq("topology management"),
    features = Seq("different participant permissions enforced by synchronizer"),
  ) in { implicit env =>
    import env.*

    Seq(participant1, participant2).foreach(_.dars.upload(CantonExamplesPath))

    val alice = participant1.parties.enable(
      "Alice",
      synchronizeParticipants = Seq(participant2),
    )
    val bob = participant2.parties.enable(
      "Bob",
      synchronizeParticipants = Seq(participant1),
    )

    // everything works before we test
    assertPingSucceeds(participant1, participant2)
    def submit() =
      participant1.ledger_api.javaapi.commands.submit_flat(
        Seq(alice),
        Seq(
          IouSyntax
            .testIou(alice, bob)
            .create
            .commands
            .loneElement
        ),
      )
    // p1 can submit while having submission permissions
    val tx1 = submit()
    val cid = JavaDecodeUtil.decodeAllCreated(Iou.COMPANION)(tx1).loneElement

    changeParticipantPermission(participant1.id, ParticipantPermission.Observation, None)
    waitForChangeToBecomeEffective(ParticipantPermission.Observation)

    // p1 can not submit anymore
    assertThrowsAndLogsCommandFailures(
      submit(),
      _.shouldBeCantonErrorCode(NoSynchronizerOnWhichAllSubmittersCanSubmit),
    )

    // p2 can not exercise a choice as p1 can not confirm
    def call() = participant2.ledger_api.javaapi.commands.submit_flat(
      Seq(bob),
      Seq(cid.id.exerciseCall().commands().loneElement),
    )
    clue("p2 can not exercise a choice as p1 can not confirm") {
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        call(),
        _.message should include("Request failed"),
      )
    }

    // change to confirmation
    changeParticipantPermission(participant1.id, ParticipantPermission.Confirmation, None)
    waitForChangeToBecomeEffective(ParticipantPermission.Confirmation)

    // p1 can not submit
    assertThrowsAndLogsCommandFailures(
      submit(),
      _.shouldBeCantonErrorCode(NoSynchronizerOnWhichAllSubmittersCanSubmit),
    )

    // p2 can exercise a choice as p1 can confirm
    call().discard

  }
}

class ParticipantStateChangeIntegrationTestPostgres extends ParticipantStateChangeIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
