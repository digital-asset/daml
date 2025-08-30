// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.OnboardsNewSequencerNode
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpenerError.EnvelopeOpenerDeserializationError
import com.digitalasset.canton.synchronizer.sequencer.store.DbSequencerStore
import com.digitalasset.canton.topology.transaction.{
  DelegationRestriction,
  NamespaceDelegation,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{Namespace, PhysicalSynchronizerId}
import com.digitalasset.canton.util.MaliciousParticipantNode
import org.scalatest.Assertion
import org.slf4j.event.Level

trait SequencerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OnboardsNewSequencerNode {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Manual

  private var synchronizerId: PhysicalSynchronizerId = _
  private var staticParameters: StaticSynchronizerParameters = _
  private var synchronizerOwners: Seq[InstanceReference] = _

  "Basic synchronizer startup" in { implicit env =>
    import env.*

    clue("starting up participants") {
      // for now we need a participant to effect changes to the synchronizer after the initial bootstrap
      participant1.start()
      participant2.start()
    }
    clue("start sequencers") {
      sequencers.local.start()
    }
    clue("start mediator") {
      mediator1.start()
    }

    staticParameters =
      StaticSynchronizerParameters.defaults(sequencer1.config.crypto, testedProtocolVersion)

    synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1)
    synchronizerId = clue("bootstrapping the synchronizer") {
      bootstrap.synchronizer(
        "test-synchronizer",
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
        synchronizerOwners = synchronizerOwners,
        synchronizerThreshold = PositiveInt.two,
        staticParameters,
      )
    }
  }

  "Generate some traffic" in { implicit env =>
    import env.*

    clue("participant1 connects to sequencer1") {
      participant1.synchronizers.connect_local(sequencer1, daName)
    }
    clue("participant2 connects to sequencer1") {
      participant2.synchronizers.connect_local(sequencer1, daName)
    }
    // ping
    participant2.health.ping(participant1.id)

    // enable a party
    participant1.parties.enable(
      "alice",
      synchronizeParticipants = Seq(participant1, participant2),
    )

    // ping again
    participant1.health.ping(participant2.id)
  }

  "Onboard a new sequencer" in { implicit env =>
    import env.*
    onboardNewSequencer(
      synchronizerId,
      newSequencerReference = sequencer2,
      existingSequencerReference = sequencer1,
      synchronizerOwners = synchronizerOwners.toSet,
    )
  }

  "Onboard participant2 to sequencer2 and send a ping" in { implicit env =>
    import env.*

    clue("participant1 connects to sequencer1") {
      participant1.synchronizers.connect_local(sequencer1, daName)
    }
    clue("participant2 connects to sequencer2") {
      participant2.synchronizers.disconnect_local(daName)
      participant2.synchronizers.connect_local(sequencer2, daName)
    }
    participant2.health.ping(participant1.id)

    // Note: we stop the participants here since these are not needed for the following tests
    //  and can cause warning flakes when they try to submit time requests
    participant1.stop()
    participant2.stop()
  }

  "preload events into the sequencer cache" in { implicit env =>
    import env.*
    loggerFactory.assertLogsSeq(
      SuppressionRule.Level(Level.INFO) && SuppressionRule.forLogger[DbSequencerStore]
    )(
      {
        sequencer1.stop()
        sequencer1.start()
        sequencer1.health.wait_for_running()
      },
      entries => {
        forAtLeast(1, entries) {
          _.infoMessage should (include regex s"Preloading the events buffer with a memory limit of .*")
        }
        forAtLeast(1, entries) {
          _.infoMessage should (include regex ("Loaded [1-9]\\d* events between .*? and .*?".r))
        }
      },
    )

  }

  "sequencer should discard only invalid envelopes" in { implicit env =>
    import env.*

    participant3.start()
    participant3.synchronizers.connect_local(sequencer1, daName)

    val syncService = participant3.underlying.value.sync

    // generate a new signing key, with which we'll generate an invalid root certificate
    val rootKey =
      participant3.keys.secret.generate_signing_key(usage = SigningKeyUsage.NamespaceOnly)

    def mkBroadcast(nsd: NamespaceDelegation) =
      TopologyTransactionsBroadcast(
        synchronizerId,
        Seq(
          SignedTopologyTransaction
            .signAndCreate(
              TopologyTransaction(
                TopologyChangeOp.Replace: TopologyChangeOp,
                PositiveInt.one,
                nsd,
                testedProtocolVersion,
              ),
              signingKeys = NonEmpty(Set, rootKey.fingerprint),
              isProposal = false,
              syncService.syncCrypto.crypto.privateCrypto,
              testedProtocolVersion,
            )
            .futureValueUS
            .value
        ),
      )

    val maliciousParticipant = MaliciousParticipantNode(
      participant3,
      synchronizerId,
      testedProtocolVersion,
      timeouts,
      loggerFactory,
    )

    def submitInvalidEnvelopeAndAssertWarnings(
        broadcasts: Seq[TopologyTransactionsBroadcast],
        assertEventually: Option[() => Assertion] = None,
    ): Assertion =
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        {
          // remember the approximate timestamp before submitting the topology transactions
          val approximateTimestampBefore =
            sequencer1.underlying.value.sequencer.syncCrypto.approximateTimestamp

          maliciousParticipant.submitTopologyTransactionBroadcasts(broadcasts).futureValueUS
          eventually() {
            // if the sequencer has processed the Deliver event (even without the invalid envelope),
            // the approximate timestamp must have moved.
            val approximateTimestampAfter =
              sequencer1.underlying.value.sequencer.syncCrypto.approximateTimestamp
            approximateTimestampAfter should be > approximateTimestampBefore

            assertEventually.map(_()).getOrElse(succeed)
          }
        },
        // expect warnings about invalid envelopes from all nodes
        logs => {
          val assertSequencerDeserializationWarning: LogEntry => Assertion =
            _.shouldBeCantonError(
              EnvelopeOpenerDeserializationError,
              _ should include regex (s"InvariantViolation.*${rootKey.fingerprint}".r),
            )
          val assertParticipantDeserializationWarning: LogEntry => Assertion =
            _.shouldBeCantonError(
              SyncServiceAlarm,
              _ should include regex (s"InvariantViolation.*${rootKey.fingerprint}".r),
            )
          val assertMediatorDeserializationWarning: LogEntry => Assertion =
            _.shouldBeCantonError(
              SyncServiceAlarm,
              _ should include regex (s"InvariantViolation.*${rootKey.fingerprint}".r),
            )
          forAtLeast(2, logs)(assertSequencerDeserializationWarning)
          forAtLeast(1, logs)(assertParticipantDeserializationWarning)
          forAtLeast(1, logs)(assertMediatorDeserializationWarning)
        },
      )

    // create a valid root certificate
    val rootCert = NamespaceDelegation.tryCreate(
      Namespace(rootKey.fingerprint),
      rootKey,
      DelegationRestriction.CanSignAllMappings,
    )
    // create an invalid root certificate that will fail when deserialized
    val invalidRootCert = NamespaceDelegation.restrictionUnsafe.replace(
      DelegationRestriction.CanSignAllButNamespaceDelegations
    )(rootCert)

    // check that a batch with a single invalid envelope still gets processed by the sequencer
    // topology processing pipeline, even if it's just an empty Deliver that simply moves the approximate time forward.
    clue("check that a batch with a single invalid envelope still gets processed by the sequencer")(
      submitInvalidEnvelopeAndAssertWarnings(broadcasts = Seq(mkBroadcast(invalidRootCert)))
    )
    // submit the invalid root cert together with a valid root cert.
    // we expect that the invalid envelope gets dropped, and the valid envelope
    // gets processed.
    clue("submit the invalid root cert together with a valid root cert")(
      submitInvalidEnvelopeAndAssertWarnings(
        broadcasts = Seq(invalidRootCert, rootCert).map(mkBroadcast),
        assertEventually = Some(() =>
          participant3.topology.namespace_delegations
            .list(
              synchronizerId,
              filterNamespace = rootKey.fingerprint.toProtoPrimitive,
            ) should not be empty
        ),
      )
    )

  }

}

// TODO(#18401): Re-enable the following tests when SequencerStore creation has been moved to the factory
//class SequencerIntegrationTestDefault extends SequencerIntegrationTest {
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//}

class SequencerIntegrationTestPostgres extends SequencerIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
