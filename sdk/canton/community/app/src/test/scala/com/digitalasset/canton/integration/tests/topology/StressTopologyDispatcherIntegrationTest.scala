// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.TopologyAdminCommands
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.SigningKeyUsage.matchesRelevantUsages
import com.digitalasset.canton.crypto.{PublicKey, SigningKeyUsage, SigningPublicKey}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.{
  InvalidAcknowledgementSignature,
  InvalidSubmissionRequestSignature,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{HostingParticipant, ParticipantPermission}
import com.digitalasset.canton.topology.{PartyId, UniqueIdentifier, transaction}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.MonadUtil
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

trait StressTopologyDispatcherIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransform(
        _.focus(_.parameters.timeouts.processing.verifyActive)
          .replace(config.NonNegativeDuration.ofSeconds(120))
      )

  private def addParties(
      env: TestConsoleEnvironment,
      participant: ParticipantReference,
      from: Int,
      to: Int,
  ): Future[Unit] = {
    import env.*
    val res = MonadUtil.parTraverseWithLimit(200)(from to to) { idx =>
      val partyId = PartyId(
        UniqueIdentifier.tryCreate(
          s"party$idx",
          participant.id.uid.namespace,
        )
      )
      // can't use console commands as the await result will exhaust the ECs
      val (_timeout, commandET) = env.grpcAdminCommandRunner.runCommandAsync(
        participant.name,
        TopologyAdminCommands.Write.Propose(
          mapping = transaction.PartyToParticipant.tryCreate(
            partyId,
            1,
            Seq(
              HostingParticipant(
                participant.id,
                ParticipantPermission.Submission,
              )
            ),
          ),
          signedBy = Seq(participant.fingerprint),
          store = TopologyStoreId.Authorized,
          waitToBecomeEffective = None,
        ),
        participant.config.clientAdminApi,
        None,
      )
      commandET.valueOrFail(s"Adding party $partyId failed!")
    }
    res.map(_ => ())
  }

  private def rollSequencerKeys()(implicit env: TestConsoleEnvironment): Unit = {
    import env.sequencer1
    (1 to 3).foreach { it =>
      clue(s"roll sequencer key $it") {
        val owner = sequencer1.id.member
        val current = getCurrentSigningKey(sequencer1, SigningKeyUsage.ProtocolOnly)

        val newKey =
          sequencer1.keys.secret
            .generate_signing_key(s"signing-do-$it", SigningKeyUsage.ProtocolOnly)
        sequencer1.topology.owner_to_key_mappings.rotate_key(
          owner,
          current,
          newKey,
          synchronize = None,
        )

        eventually() {
          getCurrentSigningKey(sequencer1, SigningKeyUsage.ProtocolOnly) shouldBe newKey
        }
      }
    }
  }

  private def getCurrentSigningKey(
      node: InstanceReference,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit env: TestConsoleEnvironment): PublicKey = {
    env.sequencer1.id.discard
    node.topology.owner_to_key_mappings
      .list(
        store = env.daId,
        filterKeyOwnerType = Some(node.id.member.code),
        filterKeyOwnerUid = node.id.member.filterString,
      )
      .loneElement
      .item
      .keys
      .find {
        case SigningPublicKey(_, _, _, keyUsage, _) => matchesRelevantUsages(keyUsage, usage)
        case _ => false // it's an encryption key
      }
      .valueOrFail(s"No signing keys found for $node")
  }

  "dispatcher" when {
    val numOfPartiesFirstIteration = 1000
    val numOfPartiesSecondIteration = 1000

    "participant joins with many existing parties" should {
      "allow joining the synchronizer" in { implicit env =>
        import env.*
        clue("add lots of parties before joining the synchronizer") {
          Await
            .result(addParties(env, participant1, 1, numOfPartiesFirstIteration), 10.minutes)
            .discard // be generous
        }
        clue("join the synchronizer with lots of parties") {
          participant1.synchronizers.connect_local(sequencer1, daName)
          eventually() {
            participant1.health.maybe_ping(participant1) should not be empty
          }
        }
      }

      "can add parties while rolling sequencer keys" in { implicit env =>
        import env.*
        val addPartyF = addParties(
          env,
          participant1,
          numOfPartiesFirstIteration + 1,
          numOfPartiesFirstIteration + numOfPartiesSecondIteration,
        )
        Threading.sleep(100) // let's start the party submissions
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            rollSequencerKeys()
            Await.result(addPartyF, 10.minutes).discard // be generous
          },
          LogEntry.assertLogSeq(
            mustContainWithClue = Seq.empty,
            mayContain = Seq(
              _.shouldBeCantonErrorCode(InvalidAcknowledgementSignature),
              _.shouldBeCantonErrorCode(InvalidSubmissionRequestSignature),
              _.errorMessage should include("periodic acknowledgement failed"),
            ),
          ),
        )
      }
    }

    val partiesSoFar = numOfPartiesFirstIteration + numOfPartiesSecondIteration
    val numOfPartiesThirdIteration = 500
    val numberOfAllParties = partiesSoFar + numOfPartiesThirdIteration

    "new participant joins with reasonably large topology state (while parties keep being added to participant1)" in {
      implicit env =>
        import env.*
        val addPartyF = addParties(env, participant1, partiesSoFar + 1, numberOfAllParties)
        Threading.sleep(100) // let's start the connecting to participant2
        participant2.synchronizers.connect_local(sequencer1, daName)
        eventually() {
          participant2.health.maybe_ping(participant1) should not be empty
        }
        Await.result(addPartyF, 10.minutes).discard // be generous
        // at this point all seem to work, but in the back all the parties are getting put into the indexers, let's wait for them to finish to prevent errors
        val allGeneratedPartyNames = (1 to numberOfAllParties).map(idx => s"party$idx").toSet

        def assumeGeneratedParties(participant: LocalParticipantReference): Unit =
          participant.ledger_api.parties
            .list()
            .map(_.party.identifier.toProtoPrimitive)
            .filter(allGeneratedPartyNames) should have size numberOfAllParties.toLong

        def assumeGeneratedEvents(participant: LocalParticipantReference): Unit = {

          val endOffset = participant.ledger_api.state.end()
          participant.ledger_api.updates
            .topology_transactions(
              completeAfter = Int.MaxValue,
              endOffsetInclusive = Some(endOffset),
            )
            .flatMap(_.topologyTransaction.events)
            .flatMap(_.event.participantAuthorizationChanged)
            .map(_.partyId)
            .map(PartyId.tryFromProtoPrimitive)
            .map(_.identifier.str)
            .filter(allGeneratedPartyNames) should have size numberOfAllParties.toLong
        }

        eventually()(assumeGeneratedParties(participant1))
        eventually()(assumeGeneratedEvents(participant1))
        eventually()(assumeGeneratedParties(participant2))
        eventually()(assumeGeneratedEvents(participant2))
    }
  }
}

class StressTopologyDispatcherReferenceIntegrationTestPostgres
    extends StressTopologyDispatcherIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

class StressTopologyDispatcherBftOrderingIntegrationTestPostgres
    extends StressTopologyDispatcherIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
}
