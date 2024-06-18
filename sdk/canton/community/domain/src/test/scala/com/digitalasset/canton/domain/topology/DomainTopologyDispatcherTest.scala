// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble}
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.DomainSnapshotSyncCryptoApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.DomainNodeParameters
import com.digitalasset.canton.domain.topology.DomainTopologySender.{
  TopologyDispatchingDegradation,
  TopologyDispatchingInternalError,
}
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.protocol.messages.DomainTopologyTransactionMessage
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.{
  RequestInvalid,
  RequestRefused,
}
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SendResult,
  SequencerClient,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.memory.InMemorySequencerCounterTrackerStore
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  DomainId,
  Member,
  TestingOwnerWithKeys,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.DelayUtil
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  MockedNodeParameters,
  SequencerCounter,
}
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class DomainTopologyDispatcherTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with MockClock { self =>

  import com.digitalasset.canton.topology.DefaultTestIdentities.*

  private def domainNodeParameters(
      processingTimeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing,
      cachingConfigs: CachingConfigs = CachingConfigs.testing,
  ) = DomainNodeParameters(
    general = MockedNodeParameters.cantonNodeParameters(
      processingTimeouts,
      cachingConfigs,
    ),
    protocol = CantonNodeParameters.Protocol.Impl(
      devVersionSupport = false,
      previewVersionSupport = false,
      dontWarnOnDeprecatedPV = false,
      initialProtocolVersion = testedProtocolVersion,
    ),
    maxBurstFactor = PositiveDouble.tryCreate(1.0),
  )

  case class TopoMsg(
      current: Seq[SignedTopologyTransaction[TopologyChangeOp]],
      recipients: Set[Member],
      observation: Long,
  ) {
    def compare(expected: SignedTopologyTransaction[TopologyChangeOp]*): Assertion = {
      check(current, expected)
    }
  }

  private def check(
      actual: Seq[SignedTopologyTransaction[TopologyChangeOp]],
      expected: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  ): Assertion = {
    actual
      .map(_.transaction.element.mapping)
      .toList shouldBe expected
      .map(_.transaction.element.mapping)
      .toList
  }

  private def toValidated(
      tx: SignedTopologyTransaction[TopologyChangeOp]
  ): ValidatedTopologyTransaction =
    ValidatedTopologyTransaction(tx, None)

  final class Fixture
      extends TestingOwnerWithKeys(domainManager, loggerFactory, parallelExecutionContext)
      with FlagCloseable {

    val ts0 = CantonTimestamp.Epoch
    val ts1 = ts0.plusSeconds(1)
    val ts2 = ts1.plusSeconds(1)

    val sourceStore = new InMemoryTopologyStore(
      TopologyStoreId.AuthorizedStore,
      loggerFactory,
      timeouts,
      futureSupervisor,
    )
    val targetStore =
      new InMemoryTopologyStore(
        TopologyStoreId.DomainStore(domainId),
        loggerFactory,
        timeouts,
        futureSupervisor,
      )

    val manager = mock[DomainTopologyManager]
    when(manager.store).thenReturn(sourceStore)

    val topologyClient = mock[DomainTopologyClientWithInit]

    val clock = mockClock

    val parameters = domainNodeParameters(DefaultProcessingTimeouts.testing, CachingConfigs.testing)

    // the domain topology dispatcher will dispatch messages
    // we use below atomic reference to capture the messages within the test
    val awaiter =
      new AtomicReference[(List[Promise[TopoMsg]], List[TopoMsg])]((List.empty, List.empty))

    // the dispatcher runs asynchronously. therefore if there are multiple changes registered
    // the distribution within a "batch" might vary. therefore in some cases
    // we use "expect(4):Future[Awaiter]" to wait until 4 topology txs have been sent, independent on the number
    // of messages.
    case class Awaiter(observed: Seq[GenericSignedTopologyTransaction], recipients: Set[Member]) {
      def compare(expected: SignedTopologyTransaction[TopologyChangeOp]*): Assertion = {
        check(observed, expected)
      }
    }

    def expect(num: Int): Future[Awaiter] = {
      def go(prev: Awaiter = Awaiter(Seq(), Set())): Future[Awaiter] = {
        nextMessage().flatMap { x =>
          val acc = Awaiter(prev.observed ++ x.current, prev.recipients ++ x.recipients)
          if (acc.observed.sizeIs >= num)
            Future.successful(acc)
          else
            go(acc)
        }
      }
      go()
    }

    def nextMessage(): Future[TopoMsg] = {
      logger.debug(s"Expecting next message")
      val promise = Promise[TopoMsg]()
      val callsite = new Exception("Didn't get a next message")
      DelayUtil.delay("awaiter", 20.seconds, this).foreach { _ =>
        if (!promise.isCompleted) promise.tryFailure(callsite)
      }
      val before = awaiter
        .getAndUpdate {
          case (Nil, _drop :: rest) => (Nil, rest)
          case (queue, Nil) => (queue :+ promise, Nil)
          case (_, _) =>
            logger.error("Should not happen")
            fail("Should not happen")
        }
      before match {
        case (_, one :: _) => promise.success(one)
        case (_, _) =>
      }
      promise.future
        .thereafter {
          case Success(cur) => logger.debug(s"Awaiter returned $cur")
          case Failure(ex) => logger.debug("Awaiter failed with an exception", ex)
        }
    }

    val sendDelay = new AtomicReference[Future[Unit]](Future.unit)
    val senderFailure =
      new AtomicReference[Option[EitherT[FutureUnlessShutdown, String, Unit]]](None)

    val sender = new DomainTopologySender() {
      override val maxBatchSize = 100
      override def sendTransactions(
          snapshot: DomainSnapshotSyncCryptoApi,
          transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
          recipients: Set[Member],
          name: String,
          batching: Boolean,
      )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
        this.logger.debug(
          s"Observed ${transactions.map(_.transaction.element.mapping)}"
        )
        val msg = TopoMsg(transactions, recipients, System.nanoTime())
        awaiter.getAndUpdate {
          case (Nil, msgs) => (Nil, msgs :+ msg)
          case (_ :: more, Nil) => (more, Nil)
          case (_, _) =>
            this.logger.error("Should not happen observation")
            fail("Should not happen observation")
        } match {
          case (recv :: _, Nil) => recv.trySuccess(msg)
          case (_, _) => // no receiver yet ready to pickup
        }
        val ret = EitherT.right[String](
          FutureUnlessShutdown.outcomeF(sendDelay.get())
        )
        senderFailure.get().fold(ret)(failure => ret.flatMap(_ => failure))
      }

      override def onClosed(): Unit = {}
      override protected val timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
      override protected val initialHealthState: ComponentHealthState = ComponentHealthState.Ok()
      override val name: String = "domain-topology-sender"
      override protected val loggerFactory: NamedLoggerFactory = self.loggerFactory
    }
    val processor = mock[TopologyTransactionProcessor]

    def mkDispatcher = new DomainTopologyDispatcher(
      domainId,
      testedProtocolVersion,
      sourceStore,
      processor,
      Map(),
      targetStore,
      this.cryptoApi.crypto,
      clock,
      false,
      parameters,
      defaultStaticDomainParameters,
      FutureSupervisor.Noop,
      sender,
      loggerFactory,
      new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts),
    )
    val dispatcher = mkDispatcher

    def init(flusher: Future[Unit] = Future.unit): Future[Unit] = {
      initDispatcher(dispatcher, flusher)
    }

    def initDispatcher(
        dispatcher: DomainTopologyDispatcher,
        flusher: Future[Unit],
    ): Future[Unit] = {
      dispatcher
        .init(FutureUnlessShutdown.outcomeF(flusher))
        .failOnShutdown("dispatcher initialization")
    }

    def submit(
        ts: CantonTimestamp,
        tx: SignedTopologyTransaction[TopologyChangeOp]*
    ): Future[Unit] = submit(dispatcher, ts, tx *)
    def submit(
        d: DomainTopologyDispatcher,
        ts: CantonTimestamp,
        tx: SignedTopologyTransaction[TopologyChangeOp]*
    ): Future[Unit] = {
      logger.debug(
        s"Submitting at $ts ${tx.map(x => (x.operation, x.transaction.element.mapping))}"
      )
      append(sourceStore, ts, tx).map { _ =>
        d.addedSignedTopologyTransaction(ts, tx)
      }
    }

    def append(
        store: TopologyStore[TopologyStoreId],
        ts: CantonTimestamp,
        txs: Seq[SignedTopologyTransaction[TopologyChangeOp]],
    ): Future[Unit] = {
      store.append(SequencedTime(ts), EffectiveTime(ts), txs.map(toValidated))
    }

    def txs = this.TestingTransactions

    def genPs(state: ParticipantPermission, side: RequestSide = RequestSide.From) = mkAdd(
      ParticipantState(
        side,
        domainId,
        participant1,
        state,
        TrustLevel.Ordinary,
      )
    )
    val participantTrustsDomain = genPs(ParticipantPermission.Submission, RequestSide.To)
    val mpsS = genPs(ParticipantPermission.Submission)
    val mpsO = genPs(ParticipantPermission.Observation)
    val mpsC = genPs(ParticipantPermission.Confirmation)
    val mpsD = genPs(ParticipantPermission.Disabled)

    override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
    override protected def logger: TracedLogger = DomainTopologyDispatcherTest.this.logger
  }

  type FixtureParam = Fixture

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Fixture
    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  "domain topology dispatcher" should {

    "dispatch" when {
      "transactions in sequence" in { f =>
        import f.*
        val grabF = expect(3)
        for {
          _ <- f.init()
          _ <- submit(ts0, txs.ns1k1, txs.id1k1)
          _ <- submit(ts1, txs.id2k2)
          res <- grabF
        } yield {
          res.compare(txs.ns1k1, txs.id1k1, txs.id2k2)
        }
      }
      "end batch if we have a domain parameters change or a participant state change" in { f =>
        import f.*
        val grabF = expect(2)
        for {
          _ <- f.init()
          _ <- submit(ts0, txs.ns1k1, txs.ps1, txs.id1k1)
          res <- grabF
          grab2 = expect(3)
          _ <- submit(ts1, txs.okm1, txs.dpc1, txs.ns1k2)
          res2 <- grab2
          res3 <- expect(1)
        } yield {
          res.compare(txs.ns1k1, txs.ps1)
          res2.compare(txs.id1k1, txs.okm1, txs.dpc1)
          res3.compare(txs.ns1k2)
        }
      }
      "delay second dispatch on effective time update" in { f =>
        import f.*
        val tdp = TestDomainParameters.defaultDynamic
        def dpc(factor: Int) =
          mkDmGov(
            DomainParametersChange(
              DomainId(uid),
              tdp.tryUpdate(topologyChangeDelay =
                tdp.topologyChangeDelay * NonNegativeInt.tryCreate(factor)
              ),
            ),
            SigningKeys.key2,
          )
        val dpc1 = dpc(1)
        val dpc2 = dpc(2)
        for {
          _ <- f.init()
          _ <- submit(ts0, dpc2)
          res <- nextMessage()
          _ <- submit(ts1, dpc1)
          res2 <- nextMessage()
          _ <- submit(ts2, txs.ns1k1)
          res3 <- nextMessage()
        } yield {
          res.compare(dpc2)
          res2.compare(dpc1)
          res3.compare(txs.ns1k1)
          val first = res2.observation
          val snd = res3.observation
          val delta = tdp.topologyChangeDelay * NonNegativeInt.tryCreate(2)
          (snd - first) should be > delta.duration.toNanos
        }
      }
    }

    "abort" when {

      def shouldHalt(f: Fixture): Future[Assertion] = {
        import f.*
        loggerFactory.assertLogs(
          for {
            _ <- f.init()
            _ <- submit(ts0, txs.ns1k1)
          } yield { succeed },
          _.errorMessage should include("Halting topology dispatching"),
        )
      }

      "on fatal submission exceptions" in { f =>
        f.senderFailure.set(Some(EitherT.right(FutureUnlessShutdown.failed(new Exception("booh")))))
        shouldHalt(f)
      }
      "on fatal submission failures failures" in { f =>
        f.senderFailure.set(Some(EitherT.leftT("booh")))
        shouldHalt(f)
      }
    }

    "resume" when {
      "restarting idle" in { f =>
        import f.*
        logger.debug("restarting when idle")
        for {
          _ <- f.init()
          _ <- submit(ts0, txs.ns1k1, txs.id1k1)
          _ <- nextMessage()
          d2 = f.mkDispatcher
          _ <- f.initDispatcher(d2, Future.unit)
          _ <- submit(d2, ts1, txs.okm1, txs.ns1k2)
          res <- nextMessage()
        } yield {
          d2.close()
          res.compare(txs.okm1, txs.ns1k2)
        }
      }
      "restarting with somewhat pending txs" in { f =>
        import f.*
        trait TestFlusher {
          def foo: Future[Unit]
        }
        val flusher = mock[TestFlusher]
        when(flusher.foo).thenReturn(Future.unit)
        for {
          _ <- f.append(sourceStore, ts0, Seq(txs.ns1k2))
          _ <- f.append(sourceStore, ts1, Seq(txs.ns1k1, txs.okm1))
          _ <- f.targetStore.updateDispatchingWatermark(ts0)
          _ <- f.append(targetStore, ts1, Seq(txs.ns1k2, txs.ns1k1))
          _ <- f.init(flusher.foo)
          res <- f.nextMessage()
        } yield {
          // ensure we've flushed the system
          verify(flusher, times(1)).foo
          // first tx should not be submitted due to watermark, and second should be filtered out
          res.compare(txs.okm1)
        }
      }
      "racy start" in { f =>
        import f.*
        for {
          // add one into the store
          _ <- f.append(sourceStore, ts0, Seq(txs.ns1k2))
          // now add one into the store and into the queue
          _ <- submit(ts1, txs.ns1k1)
          // now add one just into the queue (so won't be picked up during init when we read from the store)
          _ = f.dispatcher.addedSignedTopologyTransaction(ts2, Seq(txs.okm1))
          _ <- f.init()
          res <- nextMessage()
        } yield {
          res.compare(txs.ns1k2, txs.ns1k1, txs.okm1)
        }
      }
    }

    "bootstrapping participants" when {
      "send snapshot to new participant" in { f =>
        import f.*
        // namespace cert of the test domain
        val nsD = mkAdd(
          NamespaceDelegation(
            DefaultTestIdentities.namespace,
            SigningKeys.key1,
            isRootDelegation = true,
          )
        )
        for {
          _ <- f.init()
          _ <- submit(ts0, nsD, txs.ns1k1, txs.okm1, txs.id2k2, mpsS)
          res1 <- expect(5)
          // trust certificate will cause the dispatcher to bootstrap the participant node
          _ <- submit(ts1, participantTrustsDomain)
          catch1 <- nextMessage()
          rest <- nextMessage()
          _ <- submit(ts2, participantTrustsDomain, txs.okm2)
          res3 <- nextMessage()
        } yield {
          // normal dispatching to domain members
          res1.compare(nsD, txs.ns1k1, txs.okm1, txs.id2k2, mpsS)
          res1.recipients should not contain participant1
          // onboarding tx for participant with just key data in there at the beginning (so slight reordering
          catch1.recipients shouldBe Set(participant1)
          catch1.compare(nsD, txs.okm1, mpsS, txs.ns1k1, txs.id2k2)
          // actual trust cert then sent to all
          rest.recipients should contain(participant1)
          rest.recipients.size shouldBe 3
          rest.compare(participantTrustsDomain)
          res3.recipients should contain(participant1)

        }
      }

      "keep distributing on non-deactivation changes" in { f =>
        import f.*
        for {
          _ <- f.init()
          _ <- submit(ts0, txs.ns1k1, mpsO)
          _ <- nextMessage()
          _ <- submit(ts1, txs.okm1, participantTrustsDomain)
          catch1 <- nextMessage()
          res1 <- nextMessage()
          _ <- submit(ts2, mpsC)
          _ <- submit(ts2.plusMillis(1), revert(mpsO))
          res2 <- expect(2)
          _ <- submit(ts2.plusMillis(2), mpsS)
          res3a <- nextMessage()
          _ <- submit(ts2.plusMillis(3), revert(mpsC))
          res3b <- nextMessage()
          _ <- submit(ts2.plusMillis(4), mpsD)
          res4a <- nextMessage()
          _ <- submit(ts2.plusMillis(5), revert(mpsS))
          res4b <- nextMessage()
          _ <- submit(ts2.plusMillis(6), txs.dpc1)
          res5 <- nextMessage()
        } yield {
          catch1.recipients shouldBe Set(participant1)
          res1.recipients should contain(participant1)
          res2.recipients should contain(participant1)
          res3a.recipients should contain(participant1)
          res3b.recipients should contain(participant1)
          res4a.recipients should contain(participant1)
          res4b.recipients should contain(participant1)
          res5.recipients should contain(participant1)
        }
      }
    }

    "resume distribution to re-activated participants" in { f =>
      import f.*
      val mpsS2 = genPs(ParticipantPermission.Observation)
      val rmpsS = revert(mpsS)
      for {
        _ <- f.init()
        _ <- submit(ts0, txs.ns1k1, mpsS)
        _ <- nextMessage()
        _ <- submit(ts0.plusMillis(1), participantTrustsDomain)
        boot1 <- nextMessage() // catchup
        _ <- nextMessage() // normal distro
        // depending on protocol version, we need Some(Disabled) or None as the participant state
        _ <- submit(ts0.plusMillis(2), revert(mpsS))
        _ <- nextMessage()
        _ <- submit(ts0.plusMillis(3), rmpsS, txs.ns1k1) // should not be seen by p1
        res1 <- expect(2) // get this in two messages
        _ <-
          submit(
            ts0.plusMillis(4),
            txs.id1k1,
          ) // different scenario for v5+
        res2 <- nextMessage()
        // ban gets lifted
        _ <-
          submit(ts0.plusMillis(5), mpsS2)
        catch1 <- nextMessage() // catchup
        res3 <- nextMessage()
        _ <- submit(ts0.plusMillis(6), txs.okm1)
        res4 <- nextMessage()
      } yield {
        boot1.recipients shouldBe Set(participant1)
        res1.recipients should not contain participant1
        res2.recipients should not contain participant1
        catch1.recipients shouldBe Set(participant1)

        // we get the id1k1 first because in the catchup computation, it got moved
        // to the "first batch" as it is in the namespace / uid of the domain
        catch1.compare(rmpsS, txs.id1k1, txs.ns1k1)
        res3.compare(mpsS2)

        res3.recipients should contain(participant1)
        res4.compare(txs.okm1)
        res4.recipients should contain(participant1)
      }
    }

  }

}

trait MockClock {

  this: BaseTest =>

  private[topology] def mockClock: Clock = {
    val clock = mock[Clock]
    when(clock.scheduleAfter(any[CantonTimestamp => Unit], any[java.time.Duration])).thenAnswer {
      (task: CantonTimestamp => Unit, duration: java.time.Duration) =>
        Threading.sleep(duration.toMillis)
        logger.debug("Done waiting")
        task(CantonTimestamp.Epoch)
        FutureUnlessShutdown.unit
    }
  }
}

class DomainTopologySenderTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with MockClock {

  import com.digitalasset.canton.topology.DefaultTestIdentities.*

  case class Response(
      sync: Either[SendAsyncClientError, Unit],
      async: Option[SendResult] = Some(SendResult.Success(mock[Deliver[Envelope[?]]])),
      await: Future[Unit] = Future.unit,
  ) {
    val sendNotification: Promise[Batch[OpenEnvelope[DomainTopologyTransactionMessage]]] = Promise()
  }

  class Fixture
      extends TestingOwnerWithKeys(domainManager, loggerFactory, parallelExecutionContext) {

    val client = mock[SequencerClient]
    val timeTracker = mock[DomainTimeTracker]
    val clock = mockClock

    val responses = new AtomicReference[List[Response]](List.empty)
    val sender = new DomainTopologySender.Impl(
      domainId,
      testedProtocolVersion,
      client,
      timeTracker,
      clock,
      maxBatchSize = 1,
      retryInterval = 100.millis,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    ) {
      override def send(
          batch: CantonTimestamp => EitherT[Future, String, Batch[
            OpenEnvelope[DomainTopologyTransactionMessage]
          ]],
          callback: SendCallback,
      )(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Either[SendAsyncClientError, Unit]] = {
        logger.debug(s"Send invoked with $batch")
        responses.getAndUpdate(_.drop(1)) match {
          case Nil =>
            logger.error("Unexpected send!!!")
            FutureUnlessShutdown.pure(Left(RequestInvalid("unexpected send")))
          case one :: _ =>
            one.sendNotification.success(batch(CantonTimestamp.Epoch).futureValue)
            one.await.foreach { _ =>
              one.async.map(UnlessShutdown.Outcome(_)).foreach(callback)
            }
            FutureUnlessShutdown.pure(one.sync)
        }
      }
    }
    val snapshot = cryptoApi.currentSnapshotApproximation

    def submit(
        recipients: Set[Member],
        transactions: SignedTopologyTransaction[TopologyChangeOp]*
    ) = {
      sender
        .sendTransactions(snapshot, transactions, recipients, "testing", batching = true)
        .value
        .onShutdown(Left("shutdown"))
    }

    def respond(
        response: Response
    ): Future[Batch[OpenEnvelope[DomainTopologyTransactionMessage]]] = {
      responses.updateAndGet(_ :+ response)
      response.sendNotification.future
    }

    val txs = TestingTransactions

  }

  type FixtureParam = Fixture

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Fixture
    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {}
  }

  "domain topology sender" should {
    "split batch" when {
      "getting multiple transactions" in { f =>
        import f.*
        val resp1F = respond(Response(sync = Right(())))
        val resp2F = respond(Response(sync = Right(())))
        val subF = submit(Set(participant1), txs.id1k1, txs.okm1)

        for {
          res <- subF
          one <- resp1F
          two <- resp2F
        } yield {
          assert(res.isRight)
          one.envelopes.flatMap(_.protocolMessage.transactions) shouldBe Seq(txs.id1k1)
          two.envelopes.flatMap(_.protocolMessage.transactions) shouldBe Seq(txs.okm1)
        }
      }
    }

    "abort" when {
      "on fatal submission failures" in { f =>
        import f.*
        val respondF = respond(
          Response(sync = Left(RequestRefused(SendAsyncError.RequestInvalid("booh"))), async = None)
        )
        loggerFactory.assertLogs(
          {
            val submF = submit(Set(participant1), txs.id1k1)
            for {
              res <- submF
              _ <- respondF
            } yield {
              assert(res.isLeft, res)
            }
          },
          _.shouldBeCantonErrorCode(TopologyDispatchingInternalError),
        )
      }
      "on fatal send tracker failures" in { f =>
        import f.*
        val respondF = respond(
          Response(
            sync = Right(()),
            async = Some(
              SendResult.Error(
                DeliverError.create(
                  counter = SequencerCounter(1),
                  timestamp = CantonTimestamp.Epoch,
                  domainId,
                  messageId = MessageId.tryCreate("booh"),
                  sequencerError = SequencerErrors.SubmissionRequestMalformed("booh"),
                  protocolVersion = testedProtocolVersion,
                )
              )
            ),
          )
        )
        loggerFactory.assertLogs(
          submit(Set(participant1), txs.id1k1).flatMap(res =>
            respondF.map { _ =>
              assert(res.isLeft, res)
            }
          ),
          _.shouldBeCantonErrorCode(TopologyDispatchingInternalError),
        )
      }
    }

    "retry" when {

      def checkDegradation(f: Fixture, failure: Response): Future[Assertion] = {
        import f.*
        val resp1F = respond(failure)
        val resp2F = respond(Response(sync = Right(())))
        val stage1F = loggerFactory.assertLogs(
          {
            val submF = submit(Set(participant1), txs.id1k1)
            for {
              res <- resp1F
            } yield (res, submF)
          },
          _.shouldBeCantonErrorCode(TopologyDispatchingDegradation),
        )
        for {
          stage1 <- stage1F
          (res1, submF) = stage1
          res2 <- resp2F
          sub <- submF
        } yield {
          // we retried so we should have the same message content, but the signature might differ
          // as we are re-creating the message with the new max-sequencing-timeout
          // and signature generation is not deterministic
          def comp(m: DomainTopologyTransactionMessage) =
            (m.notSequencedAfter, m.domainId, m.transactions)
          res1.envelopes.map(x => comp(x.protocolMessage)) shouldBe res2.envelopes.map(x =>
            comp(x.protocolMessage)
          )
          assert(sub.isRight)
        }

      }

      "on retryable submission failures" in { f =>
        checkDegradation(
          f,
          Response(sync = Left(RequestRefused(SendAsyncError.Overloaded("boooh"))), async = None),
        )
      }
      "on send tracker timeouts" in { f =>
        checkDegradation(
          f,
          Response(sync = Right(()), async = Some(SendResult.Timeout(CantonTimestamp.Epoch))),
        )
      }
    }
  }

}
