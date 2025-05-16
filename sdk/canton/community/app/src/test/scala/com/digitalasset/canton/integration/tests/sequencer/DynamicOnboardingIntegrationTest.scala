// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.*
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.EnvironmentDefinition.buildBaseEnvironmentDefinition
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.util.OnboardsNewSequencerNode
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  EmptyRootHashMessagePayload,
  RootHashMessage,
}
import com.digitalasset.canton.sequencing.client.{SendResult, SequencerClient}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnections,
}
import com.digitalasset.canton.topology.{SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias}
import org.scalatest
import org.slf4j.event.Level

import java.time.Duration
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt

abstract class DynamicOnboardingIntegrationTest(val name: String)
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OnboardsNewSequencerNode
    with ReliabilityTestSuite {

  protected val synchronizerInitializationTimeout: Duration = Duration.ofSeconds(20)
  implicit private val metricsContext: MetricsContext = MetricsContext.Empty

  /** Hook for allowing sequencer integrations to adjust the base test config */
  protected def additionalConfigTransforms: ConfigTransform =
    ConfigTransforms.identity

  override lazy val environmentDefinition: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 3,
      numSequencers = 2,
      numMediators = 1,
    ).withNetworkBootstrap { implicit env =>
      new NetworkBootstrapper(EnvironmentDefinition.S1M1)
    }.addConfigTransforms(ConfigTransforms.setExitOnFatalFailures(false))

  private def modifyConnection(
      participant: ParticipantReference,
      name: SynchronizerAlias,
      connection: SequencerConnection,
  ): Unit = {
    participant.synchronizers.disconnect(name)
    participant.synchronizers.modify(
      name,
      _.copy(sequencerConnections = SequencerConnections.single(connection)),
    )
    participant.synchronizers.reconnect(name)
  }

  private var aggregationSequenced2: CantonTimestamp = _
  private var maxSequencingTimeOfAggregation: CantonTimestamp = _
  private var topologyTimestampTombstone: CantonTimestamp = _
  private var aggregatedBatch: Batch[DefaultOpenEnvelope] = _
  private var aggregationRule1: AggregationRule = _
  private var aggregationRule2: AggregationRule = _

  s"using an environment with 2 $name sequencer nodes and a synchronizer configured with only one of them" should {
    "bootstrap a synchronizer and have ping working" in { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant3.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.health.ping(participant3, timeout = 30.seconds)
    }

    def sequencerClientOf(
        participant: LocalParticipantReference,
        synchronizerId: SynchronizerId,
    ): SequencerClient =
      participant.underlying.value.sync
        .readyConnectedSynchronizerById(synchronizerId)
        .value
        .sequencerClient

    "setup in-flight aggregation" in { implicit env =>
      import env.*

      val p1SequencerClient = sequencerClientOf(participant1, daId)

      // First aggregation will remain in-flight while we switch sequencers
      aggregationRule1 = AggregationRule(
        NonEmpty(Seq, participant1.id, participant3.id),
        PositiveInt.tryCreate(2),
        testedProtocolVersion,
      )
      maxSequencingTimeOfAggregation = env.environment.clock.now.add(
        Duration.ofMinutes(2)
      ) // cannot exceed the DynamicSynchronizerParameters.sequencerAggregateSubmissionTimeout (defaults to 5m)
      topologyTimestampTombstone = env.environment.clock.now

      aggregatedBatch = Batch.of(
        testedProtocolVersion,
        RootHashMessage(
          RootHash(TestHash.digest(1)),
          daId.toPhysical,
          testedProtocolVersion,
          TransactionViewType,
          CantonTimestamp.Epoch,
          EmptyRootHashMessagePayload,
        ) -> Recipients.cc(mediator1.id),
      )

      TraceContext.withNewTraceContext { implicit traceContext =>
        logger.debug("Sending aggregation 1 part 1")
        val send1ResultPromise = Promise[UnlessShutdown[SendResult]]()
        p1SequencerClient
          .sendAsync(
            aggregatedBatch,
            maxSequencingTime = maxSequencingTimeOfAggregation,
            aggregationRule = Some(aggregationRule1),
            callback = send1ResultPromise.success,
          )
          .valueOrFailShutdown("send aggregation 1 part 1")
          .futureValue
        send1ResultPromise.future.futureValue.onShutdown(fail()).discard[SendResult]
      }

      // Second aggregation is delivered before we switch sequencers, but must be deduplicated afterwards
      aggregationRule2 = AggregationRule(
        NonEmpty(Seq, participant1.id, participant3.id),
        PositiveInt.tryCreate(2),
        testedProtocolVersion,
      )
      val p3SequencerClient = sequencerClientOf(participant3, daId)
      TraceContext.withNewTraceContext { implicit traceContext =>
        logger.debug("Sending aggregation 2 part 1")
        val send2ResultPromise = Promise[UnlessShutdown[SendResult]]()
        val send2 = p1SequencerClient
          .sendAsync(
            Batch.empty(testedProtocolVersion),
            maxSequencingTime = maxSequencingTimeOfAggregation,
            messageId = MessageId.tryCreate("aggregation-2-part-1a"),
            aggregationRule = Some(aggregationRule2),
            callback = send2ResultPromise.success,
            topologyTimestamp = None,
          )
          .valueOrFailShutdown("send aggregation 2 part 1a")

        val send3ResultPromise = Promise[UnlessShutdown[SendResult]]()
        val send3 = p3SequencerClient
          .sendAsync(
            Batch.empty(testedProtocolVersion),
            maxSequencingTime = maxSequencingTimeOfAggregation,
            messageId = MessageId.tryCreate("aggregation-2-part-1b"),
            aggregationRule = Some(aggregationRule2),
            callback = send3ResultPromise.success,
            topologyTimestamp = None,
          )
          .valueOrFailShutdown("send aggregation 2 part 1b")

        send2.futureValue
        send3.futureValue
        val send2Result = send2ResultPromise.future.futureValue.onShutdown(fail())
        val send2Timestamp = inside(send2Result) { case SendResult.Success(deliver) =>
          deliver.timestamp
        }
        val send3Result = send3ResultPromise.future.futureValue.onShutdown(fail())
        val send3Timestamp = inside(send3Result) { case SendResult.Success(deliver) =>
          deliver.timestamp
        }
        aggregationSequenced2 = send2Timestamp max send3Timestamp
      }
    }

    "bootstrap command be idempotent and have no effect if called again" in { implicit env =>
      import env.*
      val newSynchronizerId = bootstrap.synchronizer(
        EnvironmentDefinition.S1M1.synchronizerName,
        EnvironmentDefinition.S1M1.sequencers,
        EnvironmentDefinition.S1M1.mediators,
        synchronizerOwners = EnvironmentDefinition.S1M1.synchronizerOwners,
        synchronizerThreshold = EnvironmentDefinition.S1M1.synchronizerThreshold,
        staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
      )
      newSynchronizerId shouldBe daId
      participant1.health.ping(participant1, timeout = 30.seconds)
    }

    "participant should be able to ping using dynamically onboarded sequencer" in { implicit env =>
      // TODO(#22198): We should check if the newly onboarded node can serve requests from its onboarding effective time
      //  (in particular up to an end of the containing block in block sequencers) to verify the interface guarantees.
      import env.*
      sequencer1.health.initialized() shouldBe true
      sequencer2.health.initialized() shouldBe false

      // user-manual-entry-begin: DynamicallyOnboardSequencer
      onboardNewSequencer(
        synchronizerId = daId,
        newSequencer = sequencer2,
        existingSequencer = sequencer1,
        synchronizerOwners = initializedSynchronizers(daName).synchronizerOwners,
      )
      // user-manual-entry-end: DynamicallyOnboardSequencer

      sequencer2.health.initialized() shouldBe true

      // Restart the new sequencer to make sure that the initialization survives a restart
      // TODO(#25004): restart the BFT sequencer too once it's fully crash fault-tolerant (and remove the flag).
      // Currently this fails because we're stopping the sequencer before it finished its initial state transfer process.
      // Ideally we wait for that to be concluded before considering the sequencer initialized and ready to tolerate crashes.
      if (!isBftSequencer) {
        sequencer2.stop()
        sequencer2.start()
      }

      participant2.synchronizers.connect_local(sequencer2, daName)
      participant1.health.ping(participant2, timeout = 30.seconds)

      // restarting the new node after some activity is also an important scenario to check
      sequencer2.stop()
      sequencer2.start()
      participant1.health.ping(participant2, timeout = 30.seconds)
    }

    "participant should be able to switch to new sequencer even if it has received no transactions after the onboarding" taggedAs
      ReliabilityTest(
        Component("Participant", "connected to sequencer"),
        AdverseScenario(
          dependency = "sequencer",
          details = "connected sequencer fails",
        ),
        Remediation(
          remediator = "multiple sequencers",
          action = "participant is manually switched over to use a difference sequencer",
        ),
        outcome = "participant continues to process and submit transactions",
      ) in { implicit env =>
        import env.*

        // This is crucial because the participant will re-request the last event in its sequenced event store
        // and the new sequencer will serve only events from after when the snapshot was taken,
        // which happens before the sequencer onboarding transaction.
        eventually() {
          val sequencerTx = participant3.topology.owner_to_key_mappings.list(
            store = daId,
            filterKeyOwnerUid = sequencer2.id.filterString,
            filterKeyOwnerType = Some(SequencerId.Code),
          )
          // Participant3 has received the onboarding transaction of sequencer2
          sequencerTx should not be empty
        }
        modifyConnection(participant3, daName, sequencer2.sequencerConnection)
        participant3.health.ping(participant1, timeout = 30.seconds)
      }

    "new sequencer correctly aggregates existing in-flight submissions" in { implicit env =>
      import env.*
      // participant3 now talks to the newly onboarded sequencer

      val p3SequencerClient = sequencerClientOf(participant3, daId)
      TraceContext.withNewTraceContext { implicit traceContext =>
        logger.debug("Sending aggregation 1 part 2")
        val send1ResultPromise = Promise[UnlessShutdown[SendResult]]()
        // This should deliver the bogus root hash message to mediator1
        // When the mediator switches below to the other sequencer,
        // we'd see a ledger fork if the sequencers disagreed on the delivery of this event.
        p3SequencerClient
          .sendAsync(
            aggregatedBatch,
            maxSequencingTime = maxSequencingTimeOfAggregation,
            aggregationRule = Some(aggregationRule1),
            callback = send1ResultPromise.success,
          )
          .valueOrFailShutdown("send aggregation 1 part 2")
          .futureValue
        send1ResultPromise.future.futureValue.onShutdown(fail()) shouldBe a[SendResult.Success]
      }

      // Now also try to send the second aggregation that was already delivered
      TraceContext.withNewTraceContext { implicit traceContext =>
        logger.debug("Sending aggregation 2 part 2")
        val send2ResultPromise = Promise[UnlessShutdown[SendResult]]()
        p3SequencerClient
          .sendAsync(
            Batch.empty(testedProtocolVersion),
            maxSequencingTime = maxSequencingTimeOfAggregation,
            aggregationRule = Some(aggregationRule2),
            callback = send2ResultPromise.success,
          )
          .valueOrFailShutdown("send aggregation 2 part 2")
          .futureValue
        val send2Result = send2ResultPromise.future.futureValue.onShutdown(fail())
        inside(send2Result) {
          case SendResult.Error(
                DeliverError(
                  _,
                  _,
                  _,
                  _,
                  SequencerErrors.AggregateSubmissionAlreadySent(message),
                  _,
                )
              ) =>
            message should include(s"was previously delivered at $aggregationSequenced2")
        }
      }
    }

    "new sequencer sends out tombstone for participant subscription on events before its initialization" in {
      implicit env =>
        import env.*
        // participant3 now talks to the newly onboarded sequencer

        val logAssertions: Seq[LogEntry => scalatest.Assertion] =
          Seq {
            // Sequencer logs the tombstone on the read side, together with the event counter, timestamp at which it cannot sign and the member
            (logEntry: LogEntry) =>
              logEntry.loggerName should include(
                "SequencerReader$EventsReader"
              )
              logEntry.warningMessage should (
                include(
                  "This sequencer cannot sign the event with sequencing timestamp"
                ) and
                  include(
                    "for member PAR::participant3"
                  ) and
                  include(
                    "at signing timestamp"
                  )
              )
          } ++ Seq(
            // The sequencer server's "direct subscription" warns it has terminated the server-side subscription.
            // Note that because this is logged after the subscription has been canceled by the server, the
            // sequencer-client-side warning/error below can be logged before this entry. Hence the use of
            // "loggerFactory.assertLogsUnordered" above.
            logEntry => {
              logEntry.loggerName should include("DirectSequencerSubscription")
              logEntry.warningMessage should (include(
                "Subscription handler returned error"
              ) and include(
                "This sequencer cannot sign the event"
              ))
            },
            // The participant's resilient sequencer subscription warns that it is giving up the sequencer-client-side
            // subscription due to the tombstone error.
            logEntry => {
              logEntry.loggerName should include("ResilientSequencerSubscription")
              logEntry.warningMessage should (include(
                "Closing resilient sequencer subscription due to error"
              ) and include("FAILED_PRECONDITION/SEQUENCER_TOMBSTONE_ENCOUNTERED"))
            },
            // The participant's sync service errors that the participant has lost access to the sequencer's
            // corresponding synchronizer.
            logEntry => {
              logEntry.loggerName should include("CantonSyncService")
              logEntry.errorMessage should (include(
                "SYNC_SERVICE_SYNCHRONIZER_DISCONNECTED"
              ) and include(
                "FAILED_PRECONDITION/SEQUENCER_TOMBSTONE_ENCOUNTERED"
              ))
            },
          )

        val p3SequencerClient = sequencerClientOf(participant3, daId)
        TraceContext.withNewTraceContext { implicit traceContext =>
          loggerFactory.assertLogsUnordered(
            {
              logger.debug("Sending submission request with tombstone topology timestamp")
              val send1ResultPromise = Promise[UnlessShutdown[SendResult]]()
              p3SequencerClient
                .sendAsync(
                  Batch.empty(testedProtocolVersion),
                  maxSequencingTime = maxSequencingTimeOfAggregation,
                  callback = send1ResultPromise.success,
                  topologyTimestamp = Some(topologyTimestampTombstone),
                  messageId = MessageId.tryCreate("tombstone-submission-request"),
                )
                .valueOrFailShutdown("tombstone submission request submission")
                .futureValue
              send1ResultPromise.future.futureValue
              eventually() {
                participant3.synchronizers.active(daName) shouldBe false
              }
            },
            logAssertions*
          )
        }
    }

    // TODO(#14573): this documents a bug and is ignored for the normal tests
    "mediator cannot change sequencer due to a connection due to a tombstoned subscription" ignore {
      implicit env =>
        // This situation happens due to the sequencer client requesting the last event from the sequencer when connecting
        // and comparing it with the last event it got before. Due to a tombstone the sequencer counter will not match the
        // last received event and is expected to produce an exception while connecting
        import env.*

        val conn1 = sequencer1.sequencerConnection
        mediator1.sequencer_connection.get() shouldBe Some(SequencerConnections.single(conn1))

        val conn2 = sequencer2.sequencerConnection
        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          {
            mediator1.sequencer_connection.set(SequencerConnections.single(conn2))
            mediator1.sequencer_connection.get() shouldBe Some(SequencerConnections.single(conn2))
          },
          logs => {
            inside(logs) {
              case x if x.exists(_.message.contains("InvalidCounter")) => succeed
            }
            inside(logs) {
              case x
                  if x.exists(
                    _.message.contains("Closing resilient sequencer subscription due to error")
                  ) =>
                succeed
            }
          },
        )
        mediator1.sequencer_connection.set(SequencerConnections.single(conn1))
        mediator1.sequencer_connection.get() shouldBe Some(SequencerConnections.single(conn1))
        // without stop/start mediator remain non-functional as SequencersTransportState is in the shutdown state
        mediator1.stop()
        mediator1.start()

        participant1.health.ping(participant2, timeout = 30.seconds)
    }

    "reconnect participant3 to sequencer 1" in { implicit env =>
      logger.debug("reconnect participant3 to sequencer 1")
      import env.*

      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.DEBUG))(
        modifyConnection(participant3, daName, sequencer1.sequencerConnection),
        logs => {
          inside(logs) {
            case x
                if x.exists(r =>
                  r.loggerName.contains("participant=participant3") && r.message.contains(
                    "Deliver("
                  ) && r.message.contains("message id = Some(tombstone-submission-request)")
                ) =>
              succeed
          }
        },
      )

      participant3.health.ping(participant1, timeout = 30.seconds)
    }

    // TODO(#16245): Reincarnate this, currently doesn't work due to sequencer connection aliases and BFT sequencer transport change not being supported
    "mediator can change sequencer connection" ignore { implicit env =>
      import env.*

      // needed to prevent the mediator from getting a tombstone on re-connection reading the last message
      participant1.health.ping(participant3, timeout = 30.seconds)

      val conn1 = sequencer1.sequencerConnection
      mediator1.sequencer_connection.get() shouldBe Some(SequencerConnections.single(conn1))

      val conn2 = sequencer2.sequencerConnection
      mediator1.sequencer_connection.set(SequencerConnections.single(conn2))
      mediator1.sequencer_connection.get() shouldBe Some(SequencerConnections.single(conn2))
      participant1.health.ping(participant2, timeout = 30.seconds)

      mediator1.sequencer_connection.modify_connections(
        _.addEndpoints(SequencerAlias.Default, conn1).value
      )

      mediator1.sequencer_connection.get() shouldBe Some(
        SequencerConnections.single(SequencerConnection.merge(Seq(conn2, conn1)).value)
      )
      participant1.health.ping(participant2, timeout = 30.seconds)
    }

    "participant 3 can eventually change the connection to sequencer 2" in { implicit env =>
      import env.*

      modifyConnection(participant3, daName, sequencer2.sequencerConnection)
      participant3.health.ping(participant1, timeout = 30.seconds)
    }

    "cannot change to an invalid connection" in { implicit env =>
      import env.*

      val invalidConnection =
        GrpcSequencerConnection(
          NonEmpty(Seq, Endpoint("fake-host", Port.tryCreate(100))),
          transportSecurity = false,
          None,
          SequencerAlias.Default,
          None,
        )

      val conn = mediator1.sequencer_connection.get()

      loggerFactory.suppressWarningsAndErrors {
        // trying to set the connection to an invalid one will fail the sequencer handshake step
        a[CommandFailure] should be thrownBy mediator1.sequencer_connection.set(
          SequencerConnections.single(invalidConnection)
        )
      }

      // the previous connection details should not have been changed in this case
      mediator1.sequencer_connection.get() shouldBe conn

      // everything still works
      participant1.health.ping(participant2, timeout = 30.seconds)
    }

  }
}
