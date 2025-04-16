// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.reference

import cats.data.EitherT
import com.daml.metrics.api.{HistogramInventory, MetricName}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveDouble, PositiveInt}
import com.digitalasset.canton.config.{
  BatchAggregatorConfig,
  ProcessingTimeout,
  SessionSigningKeysConfig,
}
import com.digitalasset.canton.crypto.{HashPurpose, Signature, SynchronizerCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.integration.tests.sequencer.reference.ReferenceSequencerWithTrafficControlApiTestBase.EnterpriseRateLimitManagerTest
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.{
  LedgerApiServerHistograms,
  MetricsUtils,
  OpenTelemetryOnDemandMetricsReader,
}
import com.digitalasset.canton.protocol.SynchronizerParameters
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.protocol.RecipientsTest.*
import com.digitalasset.canton.sequencing.traffic.{
  EventCostCalculator,
  TrafficPurchased,
  TrafficReceipt,
}
import com.digitalasset.canton.store.db.DbTest
import com.digitalasset.canton.synchronizer.metrics.{SequencerHistograms, SequencerMetrics}
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeParameters
import com.digitalasset.canton.synchronizer.sequencer.store.DbSequencerStoreTest
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
  SequencerTrafficConfig,
  TimestampSelector,
}
import com.digitalasset.canton.synchronizer.sequencer.{Sequencer, SequencerApiTestUtils}
import com.digitalasset.canton.synchronizer.sequencing.traffic.TrafficPurchasedManager.TrafficPurchasedManagerError
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.{
  TrafficConsumedStore,
  TrafficPurchasedStore,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.{
  DefaultTrafficConsumedManagerFactory,
  EnterpriseSequencerRateLimitManager,
  TrafficConsumedManagerFactory,
  TrafficPurchasedManager,
}
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  FailOnShutdown,
  MockedNodeParameters,
  ProtocolVersionChecksFixtureAsyncWordSpec,
  SequencerCounter,
}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalatest.{Assertion, FutureOutcome}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.annotation.unused
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

abstract class ReferenceSequencerWithTrafficControlApiTestBase
    extends SequencerApiTestUtils
    with DbTest
    with ProtocolVersionChecksFixtureAsyncWordSpec
    with FailOnShutdown
    with MetricsUtils {

  override def cleanDb(storage: DbStorage)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    DbSequencerStoreTest.cleanSequencerTables(storage)

  private val eventCostCalculator = new EventCostCalculator(loggerFactory)
  private def eventCostFunction(
      @unused recipients: Recipients,
      config: TrafficControlParameters,
  ): Batch[ClosedEnvelope] => Option[SequencingSubmissionCost] = { batch =>
    Some(
      SequencingSubmissionCost(
        eventCostCalculator
          .computeEventCost(
            batch,
            config.readVsWriteScalingFactor,
            Map.empty,
            testedProtocolVersion,
            config.baseEventCost,
          )
          .eventCost,
        testedProtocolVersion,
      )
    )
  }

  class Env extends AutoCloseable with NamedLogging {
    val config: TrafficControlParameters = TrafficControlParameters(
      maxBaseTrafficAmount = NonNegativeLong.zero, // No base rate, we get only extra traffic
      readVsWriteScalingFactor = PositiveInt.tryCreate(1),
      maxBaseTrafficAccumulationDuration = canton.time.PositiveFiniteDuration.tryOfMinutes(1),
    )

    protected val loggerFactory: NamedLoggerFactory =
      ReferenceSequencerWithTrafficControlApiTestBase.this.loggerFactory

    implicit val actorSystem: ActorSystem =
      PekkoUtil.createActorSystem(loggerFactory.threadName)(parallelExecutionContext)

    val clock = new SimClock(loggerFactory = loggerFactory)

    val currentBalances =
      TrieMap.empty[Member, Either[TrafficPurchasedManagerError, NonNegativeLong]]
    val trafficConsumedStore = TrafficConsumedStore(storage, timeouts, loggerFactory)

    val topology: TestingTopology =
      TestingTopology()
        .withSimpleParticipants(p11, p12, p13, p14, p15)

    implicit val onDemandMetricsReader: OpenTelemetryOnDemandMetricsReader =
      new OpenTelemetryOnDemandMetricsReader()

    def sign(
        request: SubmissionRequest,
        signingTimestamp: Option[CantonTimestamp] = None,
    ): SignedContent[SubmissionRequest] = {
      val cryptoSnapshot =
        topology
          .build(loggerFactory)
          .forOwnerAndSynchronizer(request.sender)
          .currentSnapshotApproximation
      SignedContent
        .create(
          cryptoSnapshot.pureCrypto,
          cryptoSnapshot,
          request,
          signingTimestamp.orElse(Some(cryptoSnapshot.ipsSnapshot.timestamp)),
          HashPurpose.SubmissionRequestSignature,
          testedProtocolVersion,
        )
        .futureValueUS
        .value
    }

    override def close(): Unit =
      LifeCycle.toCloseableActorSystem(actorSystem, logger, timeouts).close()
  }

  protected def createBlockSequencerFactory(
      clock: Clock,
      topologyFactory: TestingIdentityFactory,
      storage: DbStorage,
      params: SequencerNodeParameters,
      rateLimitManager: SequencerRateLimitManager,
      sequencerMetrics: SequencerMetrics,
  )(implicit mat: Materializer): BlockSequencerFactory

  override type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()
    complete {
      super.withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  private def withSequencerAndRLM(
      config: TrafficControlParameters,
      clock: Clock,
      costCalculator: EventCostCalculator = new EventCostCalculator(loggerFactory),
      currentTopologySnapshotTimestamp: CantonTimestamp = CantonTimestamp.Epoch,
      availableUpToInclusive: CantonTimestamp = CantonTimestamp.MaxValue,
      sequencerTrafficConfig: SequencerTrafficConfig = SequencerTrafficConfig(),
  )(
      f: (Sequencer, EnterpriseRateLimitManagerTest) => FutureUnlessShutdown[Unit]
  )(implicit mat: Materializer, env: Env): FutureUnlessShutdown[Assertion] = {
    val (sequencer, rlm) = createSequencer(
      config,
      clock,
      costCalculator,
      currentTopologySnapshotTimestamp,
      availableUpToInclusive,
      sequencerTrafficConfig,
    )
    f(sequencer, rlm).transformWith {
      case Failure(exception) =>
        sequencer.close()
        fail(exception)
      case Success(_) =>
        sequencer.close()
        FutureUnlessShutdown.pure(succeed)
    }
  }

  private val trafficParametersChangeTs = CantonTimestamp.Epoch.plusSeconds(2)

  def createSequencer(
      config: TrafficControlParameters,
      clock: Clock,
      costCalculator: EventCostCalculator = new EventCostCalculator(loggerFactory),
      currentTopologySnapshotTimestamp: CantonTimestamp = CantonTimestamp.Epoch,
      availableUpToInclusive: CantonTimestamp = CantonTimestamp.MaxValue,
      sequencerTrafficConfig: SequencerTrafficConfig = SequencerTrafficConfig(),
  )(implicit
      mat: Materializer,
      env: Env,
  ): (Sequencer, EnterpriseRateLimitManagerTest) = {
    val parameters = List(
      SynchronizerParameters.WithValidity(
        validFrom = CantonTimestamp.Epoch.immediatePredecessor,
        validUntil = Some(trafficParametersChangeTs),
        parameter = DefaultTestIdentities.defaultDynamicSynchronizerParameters
          .tryUpdate(trafficControlParameters = Some(config)),
      ),
      SynchronizerParameters.WithValidity(
        validFrom = trafficParametersChangeTs,
        validUntil = None,
        parameter = DefaultTestIdentities.defaultDynamicSynchronizerParameters
          .tryUpdate(trafficControlParameters =
            Some(
              config.copy(readVsWriteScalingFactor =
                config.readVsWriteScalingFactor * PositiveInt.tryCreate(100000)
              )
            )
          ),
      ),
    )
    val topologyFactoryWithSynchronizerParameters = env.topology
      .copy(synchronizerParameters = parameters)
      .build(loggerFactory)
    val params = SequencerNodeParameters(
      general = MockedNodeParameters.cantonNodeParameters(
        ProcessingTimeout()
      ),
      protocol = CantonNodeParameters.Protocol.Impl(
        sessionSigningKeys = SessionSigningKeysConfig.disabled,
        alphaVersionSupport = false,
        betaVersionSupport = false,
        dontWarnOnDeprecatedPV = false,
      ),
      maxConfirmationRequestsBurstFactor = PositiveDouble.tryCreate(1.0),
    )
    // Important to create the histograms before the factory, because creating the factory will
    // register them once and for all and we can't add more afterwards
    val histogramInventory = new HistogramInventory()
    val sequencerHistograms = new SequencerHistograms(MetricName.Daml)(histogramInventory)
    val _ = new LedgerApiServerHistograms(MetricName.Daml)(histogramInventory)
    val factory = metricsFactory(histogramInventory)
    val sequencerMetrics = new SequencerMetrics(
      sequencerHistograms,
      factory,
    )
    val trafficPurchasedManager = new TrafficPurchasedManager(
      TrafficPurchasedStore(
        storage,
        timeouts,
        loggerFactory,
        BatchAggregatorConfig.defaultsForTesting,
      ),
      sequencerTrafficConfig,
      futureSupervisor,
      SequencerMetrics.noop("reference-sequencer-traffic-control"),
      timeouts,
      this.loggerFactory,
    ) {
      override def getTrafficPurchasedAt(
          member: Member,
          desired: CantonTimestamp,
          lastSeenO: Option[CantonTimestamp] = None,
          warnIfApproximate: Boolean = true,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, TrafficPurchasedManagerError, Option[TrafficPurchased]] =
        EitherT.fromEither(
          env.currentBalances
            .get(member) match {
            case Some(Right(b)) =>
              Right(Some(TrafficPurchased(member, PositiveInt.one, b, desired)))
            case Some(Left(error)) => Left(error)
            case None => Right(None)
          }
        )

      override def maxTsO: Option[CantonTimestamp] = None

      override def getLatestKnownBalance(
          member: Member
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[TrafficPurchased]] =
        FutureUnlessShutdown.pure {
          env.currentBalances
            .getOrElse(member, Right(NonNegativeLong.zero))
            .map(b => Some(TrafficPurchased(member, PositiveInt.one, b, clock.now)))
            .getOrElse(None)
        }
    }
    val rateLimitManager = new EnterpriseRateLimitManagerTest(
      trafficPurchasedManager,
      env.trafficConsumedStore,
      loggerFactory,
      timeouts,
      sequencerMetrics,
      topologyFactoryWithSynchronizerParameters.forOwnerAndSynchronizer(
        owner = mediatorId,
        synchronizerId,
        currentSnapshotApproximationTimestamp = currentTopologySnapshotTimestamp,
        availableUpToInclusive = availableUpToInclusive,
      ),
      testedProtocolVersion,
      sequencerTrafficConfig,
      eventCostCalculator = costCalculator,
    )
    val sequencer = createDriver(
      clock,
      topologyFactoryWithSynchronizerParameters,
      storage,
      params,
      rateLimitManager,
      sequencerMetrics,
      currentTopologySnapshotTimestamp,
      availableUpToInclusive = availableUpToInclusive,
    )
    registerAllTopologyMembers(
      topologyFactoryWithSynchronizerParameters.topologySnapshot(),
      sequencer,
    )
    sequencer -> rateLimitManager
  }

  def synchronizerId: SynchronizerId = DefaultTestIdentities.synchronizerId

  def mediatorId: MediatorId = DefaultTestIdentities.mediatorId

  def sequencerId: SequencerId = DefaultTestIdentities.sequencerId

  private def createDriver(
      clock: Clock,
      topologyFactory: TestingIdentityFactory,
      storage: DbStorage,
      params: SequencerNodeParameters,
      rateLimitManager: SequencerRateLimitManager,
      sequencerMetrics: SequencerMetrics,
      currentTopologySnapshotTimestamp: CantonTimestamp,
      availableUpToInclusive: CantonTimestamp,
  )(implicit mat: Materializer): Sequencer =
    createBlockSequencerFactory(
      clock,
      topologyFactory,
      storage,
      params,
      rateLimitManager,
      sequencerMetrics,
    )
      .create(
        synchronizerId,
        SequencerId(synchronizerId.uid),
        clock,
        clock,
        topologyFactory.forOwnerAndSynchronizer(
          owner = sequencerId,
          synchronizerId,
          currentSnapshotApproximationTimestamp = currentTopologySnapshotTimestamp,
          availableUpToInclusive = availableUpToInclusive,
        ),
        FutureSupervisor.Noop,
        SequencerTrafficConfig(),
        runtimeReady = FutureUnlessShutdown.unit,
      )
      .futureValueUS

  private def getStateFor(
      member: Member,
      sequencer: Sequencer,
  ): FutureUnlessShutdown[Option[TrafficState]] =
    sequencer
      .trafficStatus(Seq(member), TimestampSelector.LastUpdatePerMember)
      .map(_.trafficStates.get(member))

  "rate limiting sequencer" should {
    "return the status of members" in { implicit env =>
      import env.*

      withSequencerAndRLM(config, clock, currentTopologySnapshotTimestamp = clock.now) {
        case (sequencer, rlm) =>
          val messageContent = "hello"
          val sender: MediatorId = mediatorId
          val recipients = Recipients.cc(p11)

          val request = createSendRequest(
            sender,
            messageContent,
            recipients,
            sequencingSubmissionCost = eventCostFunction(recipients, config),
          )
          env.currentBalances.put(sender, Right(NonNegativeLong.tryCreate(1000L))).discard

          for {
            _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFail("Send async")
            _ <- readForMembers(List(sender), sequencer)
            allMemberStates <- sequencer
              .trafficStatus(Seq.empty, TimestampSelector.LastUpdatePerMember)

            filteredMemberStates <- sequencer
              .trafficStatus(Seq(p11), TimestampSelector.LastUpdatePerMember)

          } yield {
            val allMediatorsAndParticipants =
              env.topology.participants.keys.toSeq ++ env.topology.mediators
            allMemberStates.trafficStates.keys should contain theSameElementsAs allMediatorsAndParticipants

            filteredMemberStates.trafficStates.keys should contain theSameElementsAs List(
              p11
            )
          }
      }
    }

    "compute state based on currently valid balance" in { implicit env =>
      import env.*

      withSequencerAndRLM(config, clock) { case (sequencer, rlm) =>
        val messageContent = "hello"
        val sender: MediatorId = mediatorId
        val recipients = Recipients.cc(p11)

        val request = createSendRequest(
          sender,
          messageContent,
          recipients,
          sequencingSubmissionCost = eventCostFunction(recipients, config),
        )
        env.currentBalances.put(sender, Right(NonNegativeLong.tryCreate(1000L))).discard
        for {
          _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFail("Send async")
          messages1 <- readForMembers(List(sender), sequencer)
          traffic1Sender <- getStateFor(sender, sequencer)
          _ = traffic1Sender.value.extraTrafficPurchased.value shouldBe 1000L
          _ = traffic1Sender.value.extraTrafficConsumed.value should be > 0L
        } yield {
          val (_, event) = messages1.loneElement
          inside(event.signedEvent.content) { case deliver: Deliver[_] =>
            assertLongValue(
              "daml.sequencer.traffic-control.event-delivered-cost",
              deliver.trafficReceipt.value.consumedCost.value,
            )
            assertLongValue(
              "daml.sequencer.traffic-control.extra-traffic-consumed",
              deliver.trafficReceipt.value.consumedCost.value,
            )
            assertLongValue("daml.sequencer.traffic-control.base-traffic-remainder", 0L)
            assertLongValue(
              "daml.sequencer.traffic-control.last-traffic-update",
              deliver.timestamp.getEpochSecond,
            )
            assertMemberIsInContext(
              "daml.sequencer.traffic-control.extra-traffic-consumed",
              sender,
            )
            assertMemberIsInContext(
              "daml.sequencer.traffic-control.base-traffic-remainder",
              sender,
            )
            assertMemberIsInContext("daml.sequencer.traffic-control.last-traffic-update", sender)
            assertNoValue("daml.sequencer.traffic-control.wasted-sequencing")
            assertNoValue("daml.sequencer.traffic-control.wasted-traffic")
          }
        }
      }
    }

    "support traffic purchased at Long.MAX" in { implicit env =>
      import env.*

      // Add some base traffic too to force available traffic (base traffic + purchased traffic)
      // to be > Long.MAX
      withSequencerAndRLM(
        config.copy(maxBaseTrafficAmount = NonNegativeLong.tryCreate(100L)),
        clock,
      ) { case (sequencer, rlm) =>
        val messageContent = "hello"
        val sender: MediatorId = mediatorId
        val recipients = Recipients.cc(p11)

        val request = createSendRequest(
          sender,
          messageContent,
          recipients,
          sequencingSubmissionCost = eventCostFunction(recipients, config),
        )
        env.currentBalances.put(sender, Right(NonNegativeLong.tryCreate(Long.MaxValue))).discard

        for {
          _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFail("Send async")
        } yield ()
      }
    }

    "support concurrent topology changes within the tolerance window" in { implicit env =>
      import env.*

      withSequencerAndRLM(
        config.copy(maxBaseTrafficAmount = NonNegativeLong.tryCreate(100L)),
        clock,
        // Place the current topology TS just after the change in config, such that the cost computed by the sequencer
        // is different than the one provided by the sender
        currentTopologySnapshotTimestamp = trafficParametersChangeTs.immediateSuccessor,
        // Bound the head snapshot to a reasonable timestamp, otherwise the testing topology returns CantonTimestamp.Max
        // which puts us way too far in the future outside the tolerance window, which we don't want for this test
        availableUpToInclusive = trafficParametersChangeTs.plusMillis(10),
      ) { case (sequencer, rlm) =>
        val messageContent = "hello"
        val sender: MediatorId = mediatorId
        val recipients = Recipients.cc(p11)

        val request = createSendRequest(
          sender,
          messageContent,
          recipients,
          sequencingSubmissionCost = eventCostFunction(recipients, config),
        )
        // Sign with a timestamp just before the parameters change Ts
        val signed = sign(request, Some(trafficParametersChangeTs.immediatePredecessor))
        env.currentBalances.put(sender, Right(NonNegativeLong.tryCreate(Long.MaxValue))).discard

        for {
          _ <- sequencer.sendAsyncSigned(signed).valueOrFail("Send async")
        } yield ()
      }
    }

    "refuse submission if not enough traffic" in { implicit env =>
      import env.*

      withSequencerAndRLM(config, clock) { case (sequencer, rlm) =>
        val messageContent = "hello"
        val sender: MediatorId = mediatorId
        val recipients = Recipients.cc(p11)

        val request = createSendRequest(
          sender,
          messageContent,
          recipients,
          sequencingSubmissionCost = eventCostFunction(recipients, config),
        )

        sequencer.sendAsyncSigned(sign(request)).value.map {
          case Right(_) =>
            fail("Expected failure")
          case Left(err) =>
            err.cause should include("AboveTrafficLimit")
        }
      }
    }

    "debit traffic credit from sender but not recipient" in { implicit env =>
      import env.*

      withSequencerAndRLM(config, clock) { case (sequencer, rlm) =>
        val messageContent = "hello"
        val messageContent2 = "hello2"
        val sender: MediatorId = mediatorId
        val recipients = Recipients.cc(p11)
        val p11ExtraTrafficLimit = NonNegativeLong.tryCreate(2000)

        val request1 = createSendRequest(
          sender,
          messageContent,
          recipients,
          sequencingSubmissionCost = eventCostFunction(recipients, config),
        )
        val request2 = createSendRequest(
          sender,
          messageContent2,
          recipients,
          sequencingSubmissionCost = eventCostFunction(recipients, config),
        )

        for {
          // Check sender has no status
          senderLive1 <- getStateFor(sender, sequencer)
          _ = senderLive1 shouldBe Some(TrafficState.empty(CantonTimestamp.MinValue))
          _ = env.currentBalances.put(sender, Right(NonNegativeLong.tryCreate(1000L))).discard
          _ = env.currentBalances.put(p11, Right(p11ExtraTrafficLimit)).discard
          _ <- sequencer.sendAsyncSigned(sign(request1)).valueOrFail("Send async")
          messages1 <- readForMembers(List(sender, p11), sequencer)
          // Sender traffic consumed should have increased
          senderLive2 <- getStateFor(sender, sequencer)
          _ = senderLive2.value.extraTrafficConsumed.value > 0L shouldBe true
          // Send another message
          _ <- sequencer.sendAsyncSigned(sign(request2)).valueOrFail("Send async")
          messages2 <- readForMembers(
            List(sender, p11),
            sequencer,
            firstSequencerCounter = SequencerCounter.Genesis + 1,
          )
          senderLive3 <- getStateFor(sender, sequencer)
          _ =
            senderLive3.value.extraTrafficConsumed.value > senderLive2.value.extraTrafficConsumed.value shouldBe true
          recipientState <- getStateFor(p11, sequencer)
          _ =
            recipientState.value.extraTrafficConsumed.value shouldBe 0
        } yield {
          checkMessages(
            Seq(
              // Receipt to sender for message1
              EventDetails(
                previousTimestamp = None,
                to = sender,
                messageId = Some(request1.messageId),
                trafficReceipt = Some(
                  TrafficReceipt(
                    consumedCost = NonNegativeLong.tryCreate(messageContent.length.toLong),
                    extraTrafficConsumed = NonNegativeLong.tryCreate(messageContent.length.toLong),
                    baseTrafficRemainder = NonNegativeLong.zero,
                  )
                ),
              ),
              // Event to p11 recipient
              EventDetails(
                previousTimestamp = None,
                to = p11,
                messageId = None,
                trafficReceipt = Option.empty[TrafficReceipt],
                EnvelopeDetails(messageContent, Recipients.cc(p11)),
              ),
            ),
            messages1,
          )

          checkMessages(
            Seq(
              // Receipt to sender for message2
              EventDetails(
                previousTimestamp = messages1.headOption.map(_._2.timestamp),
                to = sender,
                messageId = Some(request2.messageId),
                trafficReceipt = Some(
                  TrafficReceipt(
                    consumedCost = NonNegativeLong.tryCreate(messageContent2.length.toLong),
                    extraTrafficConsumed = NonNegativeLong.tryCreate(
                      messageContent.length.toLong + messageContent2.length.toLong
                    ),
                    baseTrafficRemainder = NonNegativeLong.zero,
                  )
                ),
              ),
              // Event to p11 recipient
              EventDetails(
                previousTimestamp = messages1.lastOption.map(_._2.timestamp),
                to = p11,
                messageId = None,
                trafficReceipt = Option.empty[TrafficReceipt],
                EnvelopeDetails(messageContent2, Recipients.cc(p11)),
              ),
            ),
            messages2,
          )
        }
      }
    }

    "use resolved groups to compute cost" in { implicit env =>
      import env.*

      val customCostCalculator = new EventCostCalculator(loggerFactory) {
        // Set a fixed payload size, so we can reliably compute what the cost should be based on the number of recipients
        override protected def payloadSize(envelope: ClosedEnvelope): Int = 100
      }

      val trafficConfig = config.copy(readVsWriteScalingFactor = PositiveInt.tryCreate(200))

      val eventCostFunction: Batch[ClosedEnvelope] => Option[SequencingSubmissionCost] = { batch =>
        Some(
          SequencingSubmissionCost(
            customCostCalculator
              .computeEventCost(
                batch,
                trafficConfig.readVsWriteScalingFactor,
                Map(
                  AllMembersOfSynchronizer -> Set(p11, p12, p13, p14, p15, sequencerId, mediatorId)
                ),
                testedProtocolVersion,
                trafficConfig.baseEventCost,
              )
              .eventCost,
            testedProtocolVersion,
          )
        )
      }

      withSequencerAndRLM(
        trafficConfig,
        clock,
        customCostCalculator,
      ) { case (sequencer, _rlm) =>
        val messageContent = "hello"
        val sender: MediatorId = mediatorId
        val recipients = Recipients.cc(AllMembersOfSynchronizer)
        val request1 = createSendRequest(
          sender,
          messageContent,
          recipients,
          sequencingSubmissionCost = eventCostFunction,
        )

        for {
          // Check sender has no status
          senderLive1 <- getStateFor(sender, sequencer)
          _ = senderLive1 shouldBe Some(TrafficState.empty(CantonTimestamp.MinValue))
          _ = env.currentBalances.put(sender, Right(NonNegativeLong.tryCreate(120L))).discard
          _ <- sequencer.sendAsyncSigned(sign(request1)).valueOrFail("Send async")
          messages1 <- readForMembers(List(sender, p11), sequencer)
          // Sender traffic remainder should have decreased
          traffic1Sender <- getStateFor(sender, sequencer)
          /*
              Cost should be:
               100 + 100 * 7 * 200 / 10000 = 114

              7 because there are 7 members on the synchronizer:
              PAR::participant12::default
              SEQ::sequencer::default
              PAR::participant15::default
              PAR::participant13::default
              PAR::participant11::default
              MED::mediator::default
              PAR::participant14::default
           */
          _ = traffic1Sender.value.extraTrafficRemainder shouldBe 6 // 120 - 114
        } yield ()
      }
    }

    "update traffic state for group recipients correctly" in { implicit env =>
      import env.*

      withSequencerAndRLM(config, clock) { case (sequencer, rlm) =>
        val messageContent = "hello"
        val sender: MediatorId = mediatorId
        val recipients = Recipients.groups(
          NonEmpty.mk(
            Seq,
            NonEmpty.mk(Set, p11, p12),
          )
        )
        val senderExtraTrafficLimit = NonNegativeLong.tryCreate(1000)
        val p11ExtraTrafficLimit = NonNegativeLong.tryCreate(2000)
        val p12ExtraTrafficLimit = NonNegativeLong.tryCreate(3000)

        val request1 = createSendRequest(
          sender,
          messageContent,
          recipients,
          sequencingSubmissionCost = eventCostFunction(recipients, config),
        )

        def expectedTrafficStateP11 = TrafficState(
          extraTrafficPurchased = p11ExtraTrafficLimit,
          extraTrafficConsumed = NonNegativeLong.zero,
          baseTrafficRemainder = NonNegativeLong.zero,
          lastConsumedCost = NonNegativeLong.zero,
          CantonTimestamp.MinValue,
          Some(PositiveInt.one),
        )

        def expectedTrafficStateP12 =
          expectedTrafficStateP11.copy(extraTrafficPurchased = p12ExtraTrafficLimit)

        // Top up everyone let's go!
        env.currentBalances.put(sender, Right(NonNegativeLong.tryCreate(1000L))).discard
        env.currentBalances.put(p11, Right(p11ExtraTrafficLimit)).discard
        env.currentBalances.put(p12, Right(p12ExtraTrafficLimit)).discard
        for {
          _ <- sequencer.sendAsyncSigned(sign(request1)).valueOrFail("Send async")
          messages1 <- readForMembers(List(sender, p11, p12), sequencer)
          // Sender traffic should have decreased
          traffic1Sender <- getStateFor(sender, sequencer)
          _ =
            traffic1Sender.value.availableTraffic shouldBe senderExtraTrafficLimit.value - messageContent.length.toLong

          p11GetTraffic <- getStateFor(p11, sequencer)
          _ = {
            val expected = expectedTrafficStateP11
            // in timestamp because reference and bft sequencer generate a different one, and we only really car about the traffic values
            p11GetTraffic.value.copy(timestamp = CantonTimestamp.MinValue) shouldBe expected
          }
          p12GetTraffic <- getStateFor(p12, sequencer)
          _ =
            p12GetTraffic.value.copy(timestamp =
              CantonTimestamp.MinValue
            ) shouldBe expectedTrafficStateP12
        } yield {
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = sender,
                messageId = Some(request1.messageId),
                trafficReceipt = Some(
                  TrafficReceipt(
                    consumedCost = NonNegativeLong.tryCreate(messageContent.length.toLong),
                    extraTrafficConsumed = NonNegativeLong.tryCreate(messageContent.length.toLong),
                    baseTrafficRemainder = NonNegativeLong.zero,
                  )
                ),
              ),
              EventDetails(
                previousTimestamp = None,
                to = p11,
                messageId = None,
                trafficReceipt = None,
                EnvelopeDetails(messageContent, recipients),
              ),
              EventDetails(
                previousTimestamp = None,
                to = p12,
                messageId = None,
                trafficReceipt = None,
                EnvelopeDetails(messageContent, recipients),
              ),
            ),
            messages1,
          )
        }
      }
    }

    "not deliver event if traffic is insufficient after sequencing" in { implicit env =>
      import env.*

      withSequencerAndRLM(config, clock) { case (sequencer, rlm) =>
        val messageContent = "hello"
        val sender: MediatorId = mediatorId
        val recipients = Recipients.cc(p11)

        val request = createSendRequest(
          sender,
          messageContent,
          recipients,
          sequencingSubmissionCost = eventCostFunction(recipients, config),
        )
        env.currentBalances.put(sender, Right(NonNegativeLong.zero)).discard
        rlm.disableWriteSideEnforcement()

        for {
          _ <- sequencer.sendAsyncSigned(sign(request)).value
          messages <- readForMembers(Seq(sender), sequencer)
          senderTraffic <- getStateFor(sender, sequencer)
        } yield {
          checkRejection(
            messages,
            sender,
            request.messageId,
            Some(
              TrafficReceipt(
                NonNegativeLong.zero,
                NonNegativeLong.zero,
                NonNegativeLong.zero,
              )
            ),
          ) { case error =>
            val (_, event) = messages.loneElement
            error.message shouldBe s"""SEQUENCER_NOT_ENOUGH_TRAFFIC_CREDIT(9,0): AboveTrafficLimit(
                                     |  member = MED::mediator::default,
                                     |  trafficCost = 5,
                                     |  trafficState = TrafficState(extraTrafficLimit = 0, extraTrafficConsumed = 0, baseTrafficRemainder = 0, lastConsumedCost = 0, timestamp = ${event.timestamp}, serial = 1, availableTraffic = 0)
                                     |)""".stripMargin
          }
          // 259 == raw byte size of the signed submission request
          eventually() {
            assertLongValue("daml.sequencer.traffic-control.wasted-sequencing", 259L)
          }
          assertMemberIsInContext("daml.sequencer.traffic-control.wasted-sequencing", sender)
          assertInContext(
            "daml.sequencer.traffic-control.wasted-sequencing",
            "sequencer",
            sequencerId.toProtoPrimitive,
          )
          assertInContext(
            "daml.sequencer.traffic-control.wasted-sequencing",
            "reason",
            "SEQUENCER_NOT_ENOUGH_TRAFFIC_CREDIT",
          )
          senderTraffic.value.extraTrafficConsumed.value shouldBe 0L
          senderTraffic.value.extraTrafficRemainder shouldBe 0L
        }
      }
    }

    "not deadlock if topology event is not delivered due to traffic error" in { implicit env =>
      import env.*

      // In this test we simulate the scenario where a topology event passes write side enforcement
      // but is denied on the read side, and therefore not delivered. We want to make sure that this does not
      // deadlock the sequencer by it asking its topology client to observe the timestamp of that event that will never
      // arrive.
      // To do that we send 2 messages:
      // - one that gets rejected on the read side with OUTDATED_EVENT_COST
      // - a second one after that that gets processed normally, and we assert that the second is delivered as expected

      // We bound availableUpToInclusive such that the topology client cannot serve events above
      // clock.now. This ensures that any timestamp more recent requested by the sequencer along with a "last sequencer event timestamp" would
      // need to wait for that timestamp to be observed, which is what we want to assert does NOT happen in this test,
      // precisely because "last sequencer event timestamp" should not be updated if the event is not delivered to the sequencer
      withSequencerAndRLM(config, clock, availableUpToInclusive = clock.now) {
        case (sequencer, rlm) =>
          val messageContent = "hello"
          val sender: MediatorId = mediatorId
          // All members of synchronizer to simulate topology event
          val recipients = Recipients.cc(AllMembersOfSynchronizer)

          val request = createSendRequest(
            sender,
            messageContent,
            recipients,
            sequencingSubmissionCost = eventCostFunction(recipients, config),
          )
          env.currentBalances.put(sender, Right(NonNegativeLong.tryCreate(100))).discard

          // Disable write side enforcement and fail on the read side
          rlm.disableWriteSideEnforcement()
          rlm.overrideReadValidationResponse(
            EitherT.leftT(
              SequencerRateLimitError.OutdatedEventCost(
                // The error content is irrelevant here we just to see how the sequencer handles this type of error
                sender,
                None,
                clock.now,
                NonNegativeLong.zero,
                clock.now,
                Some(
                  TrafficReceipt(NonNegativeLong.zero, NonNegativeLong.zero, NonNegativeLong.zero)
                ),
              )
            )
          )

          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            for {
              _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFail("Send async")
              messages <- readForMembers(Seq(sender), sequencer)
              // Reset to normal behavior and send another message
              _ = rlm.resetReadValidationResponse()
              _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFail("Send async")
              messages2 <- readForMembers(
                Seq(sender),
                sequencer,
                firstSequencerCounter = SequencerCounter(1),
              )
            } yield {
              // First message should be rejected with and OutdatedEventCost error
              checkRejection(
                messages,
                sender,
                request.messageId,
                Some(
                  TrafficReceipt(
                    NonNegativeLong.zero,
                    NonNegativeLong.zero,
                    NonNegativeLong.zero,
                  )
                ),
              )(_.message should include("OUTDATED_TRAFFIC_COST"))
              // Make sure the second message makes it through and is received
              checkMessages(
                Seq(
                  EventDetails(
                    previousTimestamp = messages.headOption.map(_._2.timestamp),
                    to = sender,
                    messageId = Some(request.messageId),
                    trafficReceipt = Some(
                      TrafficReceipt(
                        consumedCost = NonNegativeLong.tryCreate(messageContent.length.toLong),
                        extraTrafficConsumed =
                          NonNegativeLong.tryCreate(messageContent.length.toLong),
                        baseTrafficRemainder = NonNegativeLong.zero,
                      )
                    ),
                    EnvelopeDetails(
                      content = messageContent,
                      recipients = recipients,
                      signatures = Seq.empty,
                    ),
                  )
                ),
                messages2,
              )
            },
            LogEntry.assertLogSeq(
              Seq.empty,
              Seq(
                // This message is logged at info level when processing the sequencer counter genesis
                // (because there's no "last sequencer event" to use). However in case, the second event is not
                // the sequencer counter genesis, but we still do not have a "last sequencer event", precisely because
                // of what this test is exercising, which is asserting that we do not update "last sequencer event" if
                // the corresponding event was not delivered to the sequencer. Therefore this is logged at warning level here.
                _.warningMessage should include("Using approximate topology snapshot")
              ),
            ),
          )
      }
    }

    "not sequence event if traffic is insufficient" in { implicit env =>
      import env.*

      withSequencerAndRLM(config, clock) { case (sequencer, rlm) =>
        val messageContent = "hello"
        val sender: MediatorId = mediatorId
        val recipients = Recipients.cc(p11)

        val request = createSendRequest(
          sender,
          messageContent,
          recipients,
          sequencingSubmissionCost = eventCostFunction(recipients, config),
        )
        env.currentBalances.put(sender, Right(NonNegativeLong.one)).discard // Not enough traffic
        for {
          res <- sequencer.sendAsyncSigned(sign(request)).value
          _ = res.isLeft shouldBe true
          _ = res.left.toOption.map(
            _.cause.contains("Submission was rejected because not traffic is available")
          ) shouldBe Some(true)
          senderTraffic <- getStateFor(sender, sequencer)
        } yield {
          senderTraffic.value.extraTrafficConsumed.value shouldBe 0L
          senderTraffic.value.extraTrafficRemainder shouldBe 1L
        }
      }
    }

    "deduct traffic if message is not delivered because of failed validation" in { implicit env =>
      import env.*

      withSequencerAndRLM(config, clock) { case (sequencer, rlm) =>
        val messageContent = "hello"
        val sender: MediatorId = mediatorId
        val recipients = Recipients.cc(p11)

        val request = createSendRequest(
          sender,
          messageContent,
          recipients,
          sequencingSubmissionCost = eventCostFunction(recipients, config),
          topologyTimestamp = Some(CantonTimestamp.MaxValue), // Invalid timestamp
        )
        val purchaseAmount = 1000L
        env.currentBalances.put(sender, Right(NonNegativeLong.tryCreate(purchaseAmount))).discard
        for {
          _ <- sequencer.sendAsyncSigned(sign(request)).value
          messages <- readForMembers(Seq(sender), sequencer)
          senderTraffic <- getStateFor(sender, sequencer)
        } yield {
          eventually() {
            assertLongValue("daml.sequencer.traffic-control.wasted-traffic", 5L) // Cost of "hello"
          }
          assertMemberIsInContext("daml.sequencer.traffic-control.wasted-traffic", sender)
          checkRejection(
            messages,
            sender,
            request.messageId,
            Some(
              TrafficReceipt(
                consumedCost = NonNegativeLong.tryCreate(messageContent.length.toLong),
                extraTrafficConsumed = NonNegativeLong.tryCreate(messageContent.length.toLong),
                baseTrafficRemainder = NonNegativeLong.zero,
              )
            ),
          ) { case _ =>
            succeed
          }
          senderTraffic.value.extraTrafficConsumed.value shouldBe messageContent.length.toLong
          senderTraffic.value.extraTrafficRemainder shouldBe purchaseAmount - messageContent.length.toLong
        }
      }
    }

    "not deduct traffic for sequencers" in { implicit env =>
      import env.*

      withSequencerAndRLM(config, clock) { case (sequencer, rlm) =>
        val messageContent = "hello"
        val sender: SequencerId = sequencerId
        val recipients = Recipients.cc(sender, p11)
        val request = createSendRequest(
          sender,
          messageContent,
          recipients,
          sequencingSubmissionCost = eventCostFunction(recipients, config),
        )
        env.currentBalances.put(sender, Right(NonNegativeLong.zero)).discard
        for {
          _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFail("Send async")
          messages <- readForMembers(List(sender, p11), sequencer)
          senderTraffic <- getStateFor(sender, sequencer)
        } yield {
          senderTraffic shouldBe empty
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = sender,
                messageId = Some(request.messageId),
                trafficReceipt =
                  Option.empty[TrafficReceipt], // Sequencers are not subject to traffic control, so even in their deliver receipt there's not traffic receipt
                EnvelopeDetails(messageContent, recipients),
              ),
              EventDetails(
                previousTimestamp = None,
                to = p11,
                messageId = None,
                trafficReceipt = None,
                EnvelopeDetails(messageContent, recipients),
              ),
            ),
            messages,
          )
        }
      }
    }
  }
}

object ReferenceSequencerWithTrafficControlApiTestBase {
  // Test class allowing to override some behaviors of the rate limit manager to test the reaction of the sequencer
  private[sequencer] class EnterpriseRateLimitManagerTest(
      trafficPurchasedManager: TrafficPurchasedManager,
      trafficConsumedStore: TrafficConsumedStore,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      metrics: SequencerMetrics,
      synchronizerSyncCryptoApi: SynchronizerCryptoClient,
      protocolVersion: ProtocolVersion,
      trafficConfig: SequencerTrafficConfig,
      sequencerMemberRateLimiterFactory: TrafficConsumedManagerFactory =
        DefaultTrafficConsumedManagerFactory,
      eventCostCalculator: EventCostCalculator,
  )(implicit executionContext: ExecutionContext)
      extends EnterpriseSequencerRateLimitManager(
        trafficPurchasedManager,
        trafficConsumedStore,
        loggerFactory,
        timeouts,
        metrics,
        synchronizerSyncCryptoApi,
        protocolVersion,
        trafficConfig,
        sequencerMemberRateLimiterFactory,
        eventCostCalculator,
      ) {
    private val isWriteSideEnforcementDisabled = new AtomicBoolean(false)
    private val readValidationResponse = new AtomicReference[
      Option[EitherT[FutureUnlessShutdown, SequencerRateLimitError, Option[TrafficReceipt]]]
    ](None)
    def disableWriteSideEnforcement(): Unit = isWriteSideEnforcementDisabled.set(true)
    def overrideReadValidationResponse(
        value: EitherT[FutureUnlessShutdown, SequencerRateLimitError, Option[TrafficReceipt]]
    ) = readValidationResponse.set(Some(value))
    def resetReadValidationResponse(): Unit = readValidationResponse.set(None)

    override def validateRequestAtSubmissionTime(
        request: SubmissionRequest,
        submissionTimestamp: Option[CantonTimestamp],
        lastSequencedTimestamp: CantonTimestamp,
        lastSequencerEventTimestamp: Option[CantonTimestamp],
    )(implicit
        tc: TraceContext,
        closeContext: CloseContext,
    ): EitherT[FutureUnlessShutdown, SequencerRateLimitError, Unit] =
      if (isWriteSideEnforcementDisabled.get()) {
        EitherT.pure[FutureUnlessShutdown, SequencerRateLimitError](())
      } else {
        super.validateRequestAtSubmissionTime(
          request,
          submissionTimestamp,
          lastSequencedTimestamp,
          lastSequencerEventTimestamp,
        )
      }
    override def validateRequestAndConsumeTraffic(
        request: SubmissionRequest,
        sequencingTime: CantonTimestamp,
        submissionTimestamp: Option[CantonTimestamp],
        latestSequencerEventTimestamp: Option[CantonTimestamp],
        warnIfApproximate: Boolean,
        sequencerSignature: Signature,
    )(implicit
        tc: TraceContext,
        closeContext: CloseContext,
    ): EitherT[FutureUnlessShutdown, SequencerRateLimitError, Option[TrafficReceipt]] =
      readValidationResponse
        .get()
        .getOrElse(
          super.validateRequestAndConsumeTraffic(
            request,
            sequencingTime,
            submissionTimestamp,
            latestSequencerEventTimestamp,
            warnIfApproximate,
            sequencerSignature,
          )
        )
  }
}
