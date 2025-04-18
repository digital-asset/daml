// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.admin.api.client.data.TemplateId.templateIdsFromJava
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, PositiveDurationSeconds}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.crypto.{EncryptionPublicKey, KeyPurpose, SigningKeyUsage}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.plugins.{UseCommunityReferenceBlockSequencer, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as W
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceSynchronizerDisconnect
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription
import com.digitalasset.canton.sequencing.protocol.{
  Deliver,
  DeliverError,
  MediatorGroupRecipient,
  MemberRecipient,
  MessageId,
  SequencerErrors,
}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.SequencedEventStore.{
  LatestUpto,
  PossiblyIgnoredSequencedEvent,
  SequencedEventWithTraceContext,
}
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.InvalidAcknowledgementTimestamp
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllButNamespaceDelegations
import com.digitalasset.canton.topology.transaction.NamespaceDelegation
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags}
import com.digitalasset.canton.util.ShowUtil.*
import org.slf4j.event.Level

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.Try

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
trait IgnoreSequencedEventsIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .withSetup { implicit env =>
        import env.*

        // Use a long reconciliation interval so that acs commitments are exchanged right at the beginning
        // and not during the tests.
        val daSynchronizerOwners = Seq[InstanceReference](sequencer1, mediator1)
        daSynchronizerOwners.foreach(
          _.topology.synchronizer_parameters.propose_update(
            synchronizerId = daId,
            _.update(reconciliationInterval = PositiveDurationSeconds.ofHours(1)),
          )
        )

        participant2.synchronizers.connect_local(sequencer1, daName)
      }

  def loadLastStoredEvent()(implicit
      env: TestConsoleEnvironment
  ): PossiblyIgnoredSequencedEvent[DefaultOpenEnvelope] = {
    import env.*
    participant1.testing.state_inspection
      .findMessage(daName, LatestUpto(CantonTimestamp.MaxValue))
      .value
      .value
  }

  var missingEncryptionKey: EncryptionPublicKey = _

  "A participant" when {
    "it has never connected to a synchronizer" can {
      "neither ignore nor unignore events for the synchronizer" in { implicit env =>
        import env.*

        participant1.synchronizers.register_by_config(
          SynchronizerConnectionConfig(
            daName,
            SequencerConnections.single(sequencer1.sequencerConnection),
            manualConnect = true,
          ),
          performHandshake = false,
        )

        assertThrowsAndLogsCommandFailures(
          participant1.repair
            .ignore_events(daId, SequencerCounter.Genesis, SequencerCounter.Genesis),
          _.commandFailureMessage should include("Could not find synchronizer1"),
        )
        assertThrowsAndLogsCommandFailures(
          participant1.repair
            .unignore_events(daId, SequencerCounter.Genesis, SequencerCounter.Genesis),
          _.commandFailureMessage should include("Could not find synchronizer1"),
        )
      }
    }

    "it is connected to a synchronizer" can {
      "use the synchronizer" in { implicit env =>
        import env.*

        participant1.synchronizers.reconnect(daName)
        participant1.health.ping(participant1)
      }

      "neither ignore nor unignore events" in { implicit env =>
        import env.*

        assertThrowsAndLogsCommandFailures(
          participant1.repair
            .ignore_events(daId, SequencerCounter.Genesis, SequencerCounter.Genesis),
          _.commandFailureMessage should include(
            "Participant is still connected to synchronizer synchronizer1"
          ),
        )
        assertThrowsAndLogsCommandFailures(
          participant1.repair
            .unignore_events(daId, SequencerCounter.Genesis, SequencerCounter.Genesis),
          _.commandFailureMessage should include(
            "Participant is still connected to synchronizer synchronizer1"
          ),
        )
      }
    }

    "it is disconnected from the synchronizer" can {
      "ignore and unignore events, and then ping" in { implicit env =>
        import env.*

        // Ignore the next two events, corresponding to a single create ping request.
        participant1.synchronizers.disconnect(daName)
        val lastCounter = loadLastStoredEvent().counter
        participant1.repair.ignore_events(daId, lastCounter + 1, lastCounter + 2)
        participant1.repair.unignore_events(daId, lastCounter + 1, lastCounter + 2)
        participant1.synchronizers.reconnect(daName)

        participant1.health.ping(participant1)
      }

      "neither ignore nor unignore events before the first sequencer index" in { implicit env =>
        import env.*

        participant1.synchronizers.disconnect(daName)
        val lastCounter = loadLastStoredEvent().counter
        assertThrowsAndLogsCommandFailures(
          participant1.repair.ignore_events(daId, lastCounter, lastCounter),
          _.commandFailureMessage should (
            include("Unable to modify events between") and
              include("Enable \"force\" to modify them nevertheless.")
          ),
        )
        assertThrowsAndLogsCommandFailures(
          participant1.repair.unignore_events(daId, lastCounter, lastCounter),
          _.commandFailureMessage should (
            include("Unable to modify events between") and
              include("Enable \"force\" to modify them nevertheless.")
          ),
        )
        participant1.synchronizers.reconnect(daName)
        participant1.health.ping(participant1)
      }

      "not introduce sequencer counter gaps" in { implicit env =>
        import env.*

        participant1.synchronizers.disconnect(daName)
        val lastCounter = loadLastStoredEvent().counter

        assertThrowsAndLogsCommandFailures(
          participant1.repair.ignore_events(daId, lastCounter + 2, lastCounter + 2, force = true),
          _.commandFailureMessage should include(
            "Unable to perform operation, " +
              s"because that would result in a sequencer counter gap between ${lastCounter + 1} and ${lastCounter + 1}."
          ),
        )

        participant1.repair.ignore_events(daId, lastCounter + 1, lastCounter + 2)
        assertThrowsAndLogsCommandFailures(
          participant1.repair
            .unignore_events(daId, lastCounter + 1, lastCounter + 1, force = true),
          _.commandFailureMessage should include(
            "Unable to perform operation, " +
              s"because that would result in a sequencer counter gap between ${lastCounter + 1} and ${lastCounter + 1}."
          ),
        )
        participant1.repair.unignore_events(daId, lastCounter + 1, lastCounter + 2)

        participant1.synchronizers.reconnect(daName)
        participant1.health.ping(participant1)
      }

      "ignore and unignore events using force" in { implicit env =>
        import env.*

        participant1.synchronizers.disconnect(daName)
        val lastCounter = loadLastStoredEvent().counter
        participant1.repair.ignore_events(
          daId,
          SequencerCounter.Genesis,
          lastCounter,
          force = true,
        )
        participant1.repair.unignore_events(
          daId,
          SequencerCounter.Genesis,
          lastCounter,
          force = true,
        )
        participant1.synchronizers.reconnect(daName)

        participant1.health.ping(participant1)
      }

      // TODO(#25162): Ignoring future events is incompatible with the counter based event ignoring/unignoring APIs,
      //  because the future timestamp are unknown unlike the counters. Need to consider and implement
      //  a new timestamp-based API for the use case of ignoring future events, should it still be necessary.
      "insert an empty ignored event, therefore ignore the next ping and then successfully ping again" ignore {
        implicit env =>
          import env.*

          // Ignore the next two events, corresponding to a single create ping request.
          participant1.synchronizers.disconnect(daName)
          val lastCounter = loadLastStoredEvent().counter
          participant1.repair.ignore_events(daId, lastCounter + 1, lastCounter + 2)
          loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            {
              participant1.synchronizers.reconnect(daName)

              // Create a ping contract with participant1 as observer.
              // Generous timeout to make sure that timely rejections do not kick in.
              val createPing = new W.ping.Ping(
                "",
                participant2.adminParty.toProtoPrimitive,
                participant1.adminParty.toProtoPrimitive,
              ).create.commands.loneElement
              participant2.ledger_api.javaapi.commands.submit(
                Seq(participant2.adminParty),
                Seq(createPing),
                optTimeout = None,
              )

              // Participant2 sees the ping contract from the failed attempt.
              participant2.ledger_api.state.acs.of_party(
                participant2.adminParty,
                filterTemplates = templateIdsFromJava(W.ping.Ping.TEMPLATE_ID),
              ) should have size 1
              // Participant1 does not see the ping contract, because the creating events have been ignored.
              participant1.ledger_api.state.acs.of_party(
                participant1.adminParty,
                filterTemplates = templateIdsFromJava(W.ping.Ping.TEMPLATE_ID),
              ) shouldBe empty

              // Verify that the next ping succeeds.
              clue("running the ping") {
                participant2.health.ping(participant1)
              }

              // Verify that topology management still works
              participant1.parties.enable("alice")
            },
            entries => {
              // The participant may try to acknowledge the ignored events and the sequencer will complain that
              // the acknowledged timestamp has not yet been sequenced. But sometimes the participant is slow
              // and then we don't have any complaints
              if (entries.nonEmpty) {
                entries should have size 3
                entries(0).warningMessage should include(InvalidAcknowledgementTimestamp.id)
                entries(1).errorMessage should include("INVALID_ARGUMENT")
                entries(2).warningMessage should include("Failed to acknowledge clean timestamp")
              } else succeed
            },
          )
      }
    }

    "the last stored sequenced event has been modified" must {
      "refuse to connect to the synchronizer" in { implicit env =>
        import env.*

        participant1.synchronizers.disconnect(daName)

        val lastStoredEvent = loadLastStoredEvent()
        val lastEvent = lastStoredEvent.underlying.value

        // Choose DeliverError as type of tampered event, because we don't expect DeliverErrors to be stored
        // as part of the previous tests.
        val tamperedEvent = DeliverError.create(
          None,
          lastStoredEvent.timestamp,
          daId,
          MessageId.tryCreate("schnitzel"),
          SequencerErrors.SubmissionRequestRefused(""),
          testedProtocolVersion,
          Option.empty[TrafficReceipt],
        )
        val tracedSignedTamperedEvent =
          SequencedEventWithTraceContext(lastEvent.copy(content = tamperedEvent))(traceContext)

        // Replace last event by the tamperedEvent
        val p1Node = participant1.underlying.value
        val p1PersistentState =
          p1Node.sync.syncPersistentStateManager.getByAlias(daName).value
        val p1SequencedEventStore = p1PersistentState.sequencedEventStore
        p1SequencedEventStore.delete(lastStoredEvent.counter).futureValueUS
        p1SequencedEventStore.store(Seq(tracedSignedTamperedEvent)).futureValueUS

        loggerFactory.assertLogs(
          {
            participant1.synchronizers.reconnect(daName)

            // Necessary synchronization because some of the errors are reported asynchronously.
            // Feel free to adjust if the number of errors has changed.
            eventually() {
              loggerFactory.numberOfRecordedEntries should be >= 2
            }
          },
          // The problem happens to be "ForkHappened" due to the order of checks carried out by the sequencer client.
          // Feel free to change, if another property is checked first, e.g., "SignatureInvalid".
          _.shouldBeCantonErrorCode(ResilientSequencerSubscription.ForkHappened),
          _.warningMessage should include("ForkHappened"),
          _.shouldBeCantonErrorCode(SyncServiceSynchronizerDisconnect),
        )

        eventually() {
          participant1.synchronizers.list_connected() shouldBe empty
        }
      }
    }

    "recover after ignoring the problematic event" in { implicit env =>
      import env.*

      val lastCounter = loadLastStoredEvent().counter
      // Need to enable force, because the original event has already been processed.
      participant1.repair.ignore_events(daId, lastCounter, lastCounter, force = true)

      participant1.synchronizers.reconnect(daName)
      participant1.health.ping(participant1)

      // Verify that identity management still works
      participant1.parties.enable("bob")
    }

    "the application handler has failed" must {
      "disconnect from the synchronizer" in { implicit env =>
        import env.*

        val signingKey = clue("Enable p2 to sign topology transactions on behalf of p1") {
          val signingKey =
            participant2.keys.secret
              .generate_signing_key("p2 signing key", SigningKeyUsage.NamespaceOnly)
          participant1.keys.public
            .upload(signingKey.toByteString(testedProtocolVersion), Some("p2 signing key"))
          participant1.topology.namespace_delegations.propose_delegation(
            participant1.namespace,
            signingKey,
            CanSignAllButNamespaceDelegations,
          )
          val delegations = participant1.topology.transactions
            .list(filterAuthorizedKey = Some(participant1.fingerprint))
            .result
            .map(_.transaction)
            .filter(_.mapping.code == NamespaceDelegation.code)
          participant2.topology.transactions.load(
            delegations,
            TopologyStoreId.Authorized,
            ForceFlag.AlienMember,
          )
          utils.retry_until_true(
            participant2.topology.namespace_delegations
              .list(
                store = daId,
                filterNamespace = participant1.namespace.filterString,
                filterTargetKey = Some(signingKey.fingerprint),
              )
              .nonEmpty
          )
          signingKey
        }

        clue("Create encryption key that does not exist on p1") {
          missingEncryptionKey =
            participant2.keys.secret.generate_encryption_key("encryption-key-missing-on-p1")

          // participant2 enables the missing encryption key on behalf of p1
          // and disables all previous encryption keys of p1
          val previousOtk = participant1.topology.owner_to_key_mappings
            .list(
              TopologyStoreId.Authorized,
              filterKeyOwnerUid = participant1.uid.toProtoPrimitive,
            )
            .loneElement
            .item
          val otk = previousOtk.copy(keys =
            NonEmpty(Seq, missingEncryptionKey) ++
              previousOtk.keys.filterNot(_.purpose == KeyPurpose.Encryption)
          )

          loggerFactory.assertLogs(
            {
              participant2.topology.owner_to_key_mappings.propose(
                otk,
                Some(PositiveInt.tryCreate(2)),
                signedBy = Seq(signingKey.fingerprint),
                store = daId,
                force = ForceFlags(ForceFlag.AlienMember),
              )
              // Wait until p1 has processed the topology transaction.
              eventually() {
                participant1.topology.owner_to_key_mappings
                  .list(
                    store = daId,
                    filterKeyOwnerUid = participant1.id.filterString,
                  )
                  .flatMap(_.item.keys)
                  .filter(_.purpose == missingEncryptionKey.purpose)
                  .loneElement
                  .fingerprint shouldBe missingEncryptionKey.fingerprint
              }
            },
            // Participant1 will emit an error, because the key is not present.
            _.errorMessage should include("but this key is not present"),
          )
        }

        @tailrec
        def pokeAndAdvance(pingF: Future[Option[Duration]]): Option[Duration] = if (
          pingF.isCompleted
        ) {
          pingF.futureValue
        } else {
          participant2.testing.fetch_synchronizer_time(daName, 5.seconds)
          env.environment.simClock.foreach(_.advance(java.time.Duration.ofMillis(500)))
          pokeAndAdvance(pingF)
        }

        // Note: The following ping will bring transaction processing to a halt,
        // because it can't decrypt the confirmation request.
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            clue("pinging to halt") {
              pokeAndAdvance(Future {
                participant2.health.maybe_ping(participant1, timeout = 2.seconds)
              })
            }

            // There is a 1% chance that the participant is not yet disconnected,
            // because the poisonous event was the last event received.
            // The participant will only disconnect from a synchronizer if (1) the application handler has thrown an exception
            // and (2) another event arrives to the SequencerClient.
            Try(participant1.synchronizers.disconnect(daName))
            // Restart participant to clean up CantonSyncService
            Try(participant1.stop())
            participant1.start()
            participant1.synchronizers.list_connected() shouldBe empty
          },
          forAtLeast(1, _) {
            _.message should startWith("Asynchronous event processing failed")
          },
        )
      }

      "recover after ignoring the problematic event" in { implicit env =>
        import env.*

        val lastEvent = participant1.testing.state_inspection
          .findMessage(daName, LatestUpto(CantonTimestamp.MaxValue))
          .value
          .value
        val lastEventRecipients =
          lastEvent.underlying.value.content.asInstanceOf[Deliver[_]].batch.allRecipients
        logger.info(show"Recipients of last event: $lastEventRecipients")
        val lastEventIsRequestMessage =
          lastEventRecipients == Set(
            MemberRecipient(participant1.id),
            MemberRecipient(participant2.id),
            MediatorGroupRecipient(MediatorGroupIndex.zero),
          )
        val lastRequestSequencerCounter =
          if (lastEventIsRequestMessage) lastEvent.counter else lastEvent.counter - 1

        // Ignore the problematic messages (confirmation request + result message)
        participant1.repair.ignore_events(
          daId,
          lastRequestSequencerCounter,
          // TODO(#25162): This ignores the future event, which is incompatible with previous timestamps.
          //  The test work probably because the result message is ignored without prior confirmation request.
          //  Need to check if that is good enough and if we don't need to extend event ignoring API
          //  to support ignoring "future" timestamps.
          //  Previously command here was:
          //    lastRequestSequencerCounter + 1,
          lastRequestSequencerCounter,
        )

        // We can already reconnect, but we still get an error about the missing key
        loggerFactory.assertLogs(
          participant1.synchronizers.reconnect(daName),
          _.errorMessage should include("but this key is not present"),
        )

        // now, add the encryption key
        val privateKey = participant2.keys.secret
          .download(missingEncryptionKey.fingerprint, testedProtocolVersion)
        participant1.keys.secret.upload(privateKey, None)

        // and ping should pass
        participant2.health.ping(participant1)
      }
    }
  }
}

class IgnoreSequencedEventsIntegrationTestH2 extends IgnoreSequencedEventsIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}
