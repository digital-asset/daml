// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import akka.Done
import akka.stream.QueueOfferResult.Enqueued
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{StreamSpec, TestPublisher}
import akka.stream.{BoundedSourceQueue, KillSwitch, KillSwitches}
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances, PrettyPrinting}
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext}
import com.digitalasset.canton.util.OrderedBucketMergeHub.{
  ActiveSourceTerminated,
  NewConfiguration,
  Output,
  OutputElement,
}
import com.digitalasset.canton.{BaseTest, DiscardOps}

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}

class OrderedBucketMergeHubTest extends StreamSpec with BaseTest {
  import AkkaUtilTest.*

  // Override the implicit from AkkaSpec so that we don't get ambiguous implicits
  override val patience: PatienceConfig = defaultPatience

  private implicit val executionContext: ExecutionContext = system.dispatcher

  private implicit val prettyString = PrettyInstances.prettyString

  private type Name = String
  private type Config = Int
  private type Offset = Int
  private type M = String
  private case class Bucket(offset: Int, discriminator: Int) extends PrettyPrinting {
    override def pretty: Pretty[Bucket] = prettyOfClass(
      param("offset", _.offset),
      param("discriminator", _.discriminator),
    )
  }
  private case class Elem(bucket: Bucket, description: String) extends HasTraceContext {
    override val traceContext: TraceContext = TraceContext.withNewTraceContext(Predef.identity)
  }

  private def mkHub(
      ops: OrderedBucketMergeHubOps[Name, Elem, Config, Offset, M]
  ): OrderedBucketMergeHub[Name, Elem, Config, Offset, M] =
    new OrderedBucketMergeHub[Name, Elem, Config, Offset, M](
      ops,
      loggerFactory,
      enableInvariantCheck = true,
    )

  private def mkOps(initial: Offset)(
      mkSource: (Name, Config, Offset, Option[Elem]) => Source[Elem, (KillSwitch, Future[Done], M)]
  ): OrderedBucketMergeHubOps[Name, Elem, Config, Offset, M] =
    OrderedBucketMergeHubOps[Name, Elem, Config, Offset, Bucket, M](initial)(_.bucket, _.offset)(
      mkSource
    )

  private def matFor(name: Name): M = s"$name-mat"

  private def addMaterialized(
      config: OrderedBucketMergeConfig[Name, Config],
      mats: Name*
  ): OrderedBucketMergeConfig[Name, (Config, Option[M])] =
    config.map((name, c) => c -> Option.when(mats.contains(name))(matFor(name)))

  "with a single config and threshould 1" should {
    "be the identity" in assertAllStagesStopped {
      def mkElem(i: Int): Elem = Elem(Bucket(2 * i + 200, 0), s"$i")

      val sourceCompletesPromise = Promise[Done]()

      val ops = mkOps(100) { (name, _config, _offset, _prior) =>
        Source(1 to 10).map(mkElem).watchTermination() { (_, doneF) =>
          sourceCompletesPromise.completeWith(doneF)
          (noOpKillSwitch, doneF, matFor(name))
        }
      }
      val ((configQueue, doneF), emittedF) =
        Source.queue(1).viaMat(mkHub(ops))(Keep.both).toMat(Sink.seq)(Keep.both).run()

      val config = OrderedBucketMergeConfig(PositiveInt.one, NonEmpty(Map, "primary" -> 0))
      configQueue.offer(config) shouldBe Enqueued
      sourceCompletesPromise.future.futureValue
      configQueue.complete()

      emittedF.futureValue shouldBe
        NewConfiguration(addMaterialized(config, "primary"), 100) +:
        (1 to 10).map(i => OutputElement(NonEmpty(Map, "primary" -> mkElem(i)))) :+
        ActiveSourceTerminated("primary", None)
      doneF.futureValue
    }
  }

  "completing the config source" should {
    "terminate the stream and drain the source" in assertAllStagesStopped {
      def mkElem(i: Int): Elem = Elem(Bucket(2 * i + 200, 0), s"$i")

      val pulledElems = new AtomicReference[Seq[Int]](Seq.empty[Int])
      val configQueueCell =
        new SingleUseCell[BoundedSourceQueue[OrderedBucketMergeConfig[Name, Config]]]()
      def observeElem(i: Int): Int = {
        pulledElems.updateAndGet(_ :+ i)
        // Complete the config stream after the second pull!
        if (i == 2) configQueueCell.get.value.complete()
        i
      }
      val killSwitchPulledAt = new AtomicInteger()

      val ops = mkOps(100) { (name, _config, _offset, _prior) =>
        val completableSource = Source.queue[Int](11)
        completableSource.map(observeElem).map(mkElem).watchTermination() {
          (boundedSourceQueue, doneF) =>
            logger.debug("Filling the source")
            (1 to 10).foreach(boundedSourceQueue.offer(_).discard)
            // Complete the source only when the kill switch is pulled.
            // This ensures that the hub should not output an ActiveSourceTermination
            val killSwitch = new KillSwitch {
              override def shutdown(): Unit = {
                val pulledAt = pulledElems.get().lastOption.value
                logger.debug(s"Pulled the kill switch at $pulledAt")
                killSwitchPulledAt.set(pulledAt)
                boundedSourceQueue.complete()
              }
              override def abort(ex: Throwable): Unit = ???
            }
            doneF.onComplete { res =>
              logger.debug(s"Source terminated with result: $res")
            }
            (killSwitch, doneF, matFor(name))
        }
      }
      val ((configQueue, doneF), emittedF) =
        Source.queue(1).viaMat(mkHub(ops))(Keep.both).toMat(Sink.seq)(Keep.both).run()
      configQueueCell.putIfAbsent(configQueue)
      val config = OrderedBucketMergeConfig(PositiveInt.one, NonEmpty(Map, "primary" -> 0))
      configQueue.offer(config) shouldBe Enqueued

      val emitted = emittedF.futureValue
      val pulledAt = killSwitchPulledAt.get()
      pulledAt shouldBe 2
      emitted shouldBe
        NewConfiguration(addMaterialized(config, "primary"), 100) +:
        (1 until pulledAt).map(i => OutputElement(NonEmpty(Map, "primary" -> mkElem(i))))

      doneF.futureValue
      pulledElems.get shouldBe (1 to 10)
    }
  }

  "collect elements until the threshold is reached" in assertAllStagesStopped {
    def mkElem(i: Int, d: Int, s: String): Elem = Elem(Bucket(i, d), s)

    val primary = "primary"
    val secondary = "secondary"
    val tertiary = "tertiary"
    val primarySourceRef = new AtomicReference[TestPublisher.Probe[Elem]]()
    val secondarySourceRef = new AtomicReference[TestPublisher.Probe[Elem]]()
    val tertiarySourceRef = new AtomicReference[TestPublisher.Probe[Elem]]()
    val sources = Map(
      primary -> primarySourceRef,
      secondary -> secondarySourceRef,
      tertiary -> tertiarySourceRef,
    )

    val ops = mkOps(100) { (name, _config, _offset, _prior) =>
      TestSource.probe[Elem].viaMat(KillSwitches.single)(Keep.both).watchTermination() {
        case ((probe, killSwitch), doneF) =>
          sources(name).set(probe)
          (killSwitch, doneF, matFor(name))
      }
    }
    val sinkProbe = TestSink.probe[Output[Name, (Config, Option[M]), Elem, Offset]]

    val ((configQueue, doneF), sink) =
      Source.queue(1).viaMat(mkHub(ops))(Keep.both).toMat(sinkProbe)(Keep.both).run()
    val config = OrderedBucketMergeConfig(
      PositiveInt.tryCreate(2),
      NonEmpty(Map, primary -> 0, secondary -> 0, tertiary -> 1),
    )
    configQueue.offer(config) shouldBe Enqueued

    // Wait until all three sources have been created
    clue("Process NewConfiguration signal") {
      sink.request(1)
      sink.expectNext() shouldBe NewConfiguration(
        addMaterialized(config, primary, secondary, tertiary),
        100,
      )
    }

    val primarySource = primarySourceRef.get()
    val secondarySource = secondarySourceRef.get()
    val tertiarySource = tertiarySourceRef.get()

    // Due to Akka stream's internal batching, there may be more demand than just the requested 1.
    primarySource.expectRequest() should be >= (1L)
    secondarySource.expectRequest() should be >= (1L)
    tertiarySource.expectRequest() should be >= (1L)

    sink.request(10)
    clue("Send diverging elements") {
      primarySource.sendNext(mkElem(102, 1, primary))
      secondarySource.sendNext(mkElem(102, 2, secondary)) // Same offset, but different bucket
      // Make sure that we don't emit anything yet
      sink.expectNoMessage(20.milliseconds)
    }

    clue("Reach the threshold on one of the diverging elements") {
      tertiarySource.sendNext(mkElem(102, 1, tertiary))
      sink.expectNext() shouldBe OutputElement(
        NonEmpty(Map, primary -> mkElem(102, 1, primary), tertiary -> mkElem(102, 1, tertiary))
      )
    }

    clue("Skip an offset") {
      // Now make sure that we can skip offsets if a later quorum is reached
      tertiarySource.sendNext(mkElem(103, 1, tertiary))
      // Secondary source's element should have been dropped out by now
      // so that the next one is processed
      primarySource.sendNext(mkElem(104, 1, primary))
      sink.expectNoMessage(20.milliseconds)

      secondarySource.sendNext(mkElem(104, 1, secondary))

      sink.expectNext() shouldBe OutputElement(
        NonEmpty(Map, primary -> mkElem(104, 1, primary), secondary -> mkElem(104, 1, secondary))
      )
    }

    clue("Ignore late offsets") {
      tertiarySource.sendNext(mkElem(104, 1, "late arrival"))
      tertiarySource.sendNext(mkElem(105, 2, "three"))
      primarySource.sendNext(mkElem(105, 2, "one"))
      sink.expectNext() shouldBe OutputElement(
        NonEmpty(Map, primary -> mkElem(105, 2, "one"), tertiary -> mkElem(105, 2, "three"))
      )
    }

    clue("Output termination only after element") {
      tertiarySource.sendNext(mkElem(106, 1, tertiary))
      tertiarySource.sendComplete()

      sink.expectNoMessage(20.milliseconds)

      primarySource.sendNext(mkElem(106, 1, primary))
      sink.expectNext() shouldBe OutputElement(
        NonEmpty(Map, primary -> mkElem(106, 1, primary), tertiary -> mkElem(106, 1, tertiary))
      )
      sink.expectNext() shouldBe ActiveSourceTerminated(tertiary, None)
    }

    clue("Continue with two sources") {
      secondarySource.sendNext(mkElem(105, 2, "two"))
      secondarySource.sendNext(mkElem(107, 10, secondary))
      primarySource.sendNext(mkElem(107, 10, primary))
      sink.expectNext() shouldBe OutputElement(
        NonEmpty(Map, primary -> mkElem(107, 10, primary), secondary -> mkElem(107, 10, secondary))
      )
    }

    configQueue.complete()
    sink.expectComplete()
    doneF.futureValue
  }

  "buffer only one element per subsource" in assertAllStagesStopped {
    def mkElem(config: Config, i: Int): Elem = Elem(Bucket(i, config), "")

    val observedElems: scala.collection.concurrent.Map[String, Int] = TrieMap.empty[String, Int]

    def observeElem(name: String, i: Int): Int = {
      observedElems.put(name, i).discard[Option[Int]]
      i
    }

    val ops = mkOps(0) { (name, config, _offset, _prior) =>
      Source(1 to 100)
        .map(observeElem(name, _))
        .viaMat(KillSwitches.single)(Keep.right)
        .map(mkElem(config, _))
        .watchTermination()((killSwitch, doneF) => (killSwitch, doneF, matFor(name)))
    }
    val ((configQueue, doneF), sink) =
      Source.queue(1).viaMat(mkHub(ops))(Keep.both).toMat(TestSink.probe)(Keep.both).run()
    val sources = 100

    val config = OrderedBucketMergeConfig(
      PositiveInt.tryCreate(4),
      NonEmptyUtil.fromUnsafe((1 to sources).map(i => s"subsource-$i" -> i).toMap),
    )
    configQueue.offer(config) shouldBe Enqueued

    sink.request(10)
    sink.expectNext() shouldBe NewConfiguration(
      addMaterialized(config, (1 to sources).map(i => s"subsource-$i") *),
      0,
    )

    // All sources should have been pulled now
    eventually() {
      observedElems should have size sources.toLong
    }
    observedElems.foreach { case (name, i) =>
      clue(s"For subsource $name") {
        i shouldBe 1
      }
    }
    // Nothing is emitted because each bucket contains at most three elements

    clue("Stop the stream") {
      configQueue.complete()
      sink.expectComplete()
    }
    doneF.futureValue
    // No further elements are pulled during shutdown (the kill switch sits after the observation)
    observedElems.foreach { case (name, i) =>
      clue(s"For subsource $name") {
        i shouldBe 1
      }
    }
  }

  "complete only after draining the source" in assertAllStagesStopped {
    val promise = Promise[Elem]()

    val ops = mkOps(0) { (name, _config, _offset, _prior) =>
      Source
        .single(())
        .viaMat(KillSwitches.single)(Keep.right)
        .mapAsync(parallelism = 1)(_ => promise.future)
        .watchTermination()((killSwitch, doneF) => (killSwitch, doneF, matFor(name)))
    }
    val ((configQueue, doneF), sink) =
      Source.queue(1).viaMat(mkHub(ops))(Keep.both).toMat(TestSink.probe)(Keep.both).run()

    val config = OrderedBucketMergeConfig(
      PositiveInt.one,
      NonEmpty(Map, "mapAsync-source" -> 1),
    )
    configQueue.offer(config) shouldBe Enqueued

    sink.request(10)
    sink.expectNext() shouldBe NewConfiguration(
      addMaterialized(config, "mapAsync-source"),
      0,
    )

    configQueue.complete()
    sink.expectNoMessage(20.milliseconds)
    doneF.isCompleted shouldBe false

    promise.success(Elem(Bucket(1, 1), ""))
    sink.expectComplete()
    doneF.futureValue
  }

  "complete only after the completion future of the source" in assertAllStagesStopped {
    val promise = Promise[Done]()

    val ops = mkOps(0) { (name, _config, _offset, _prior) =>
      Source.empty.mapMaterializedValue(_ => (noOpKillSwitch, promise.future, matFor(name)))
    }
    val ((configQueue, doneF), sink) =
      Source.queue(1).viaMat(mkHub(ops))(Keep.both).toMat(TestSink.probe)(Keep.both).run()

    val config = OrderedBucketMergeConfig(
      PositiveInt.one,
      NonEmpty(Map, "source-with-custom-completion-future" -> 1),
    )
    configQueue.offer(config) shouldBe Enqueued

    sink.request(10)
    sink.expectNext() shouldBe NewConfiguration(
      addMaterialized(config, "source-with-custom-completion-future"),
      0,
    )
    sink.expectNext() shouldBe ActiveSourceTerminated("source-with-custom-completion-future", None)

    configQueue.complete()
    sink.expectComplete()
    always(20.milliseconds) {
      doneF.isCompleted shouldBe false
    }

    promise.success(Done)
    doneF.futureValue
  }

  "reconfigurations take effect immediately" in assertAllStagesStopped {
    def mkElem(i: Int, d: Int, s: String): Elem = Elem(Bucket(i, d), s)

    val probes = TrieMap.empty[String, TestPublisher.Probe[Elem]]

    val ops = mkOps(0) { (name, config, _offset, _prior) =>
      TestSource.probe[Elem].viaMat(KillSwitches.single)(Keep.both).watchTermination() {
        case ((probe, killSwitch), doneF) =>
          probes.put(s"$name-$config", probe)
          (killSwitch, doneF, matFor(name))
      }
    }
    val sinkProbe = TestSink.probe[Output[Name, (Config, Option[M]), Elem, Offset]]

    val ((configQueue, doneF), sink) =
      Source.queue(1).viaMat(mkHub(ops))(Keep.both).toMat(sinkProbe)(Keep.both).run()
    val config = OrderedBucketMergeConfig(
      PositiveInt.tryCreate(2),
      NonEmpty(
        Map,
        "probe1" -> 0,
        "probe2" -> 0,
        "probe3" -> 0,
        "probe4" -> 0,
        "probe5" -> 0,
      ),
    )
    configQueue.offer(config) shouldBe Enqueued

    // Wait until all five sources have been created
    clue("Process NewConfiguration signal") {
      sink.request(1)
      sink.expectNext() shouldBe NewConfiguration(
        addMaterialized(config, "probe1", "probe2", "probe3", "probe4", "probe5"),
        0,
      )
    }
    sink.request(20)

    probes("probe1-0").sendNext(mkElem(1, 0, "p1"))
    probes("probe1-0").sendNext(mkElem(5, 0, "p1-5"))
    probes("probe2-0").sendNext(mkElem(2, 0, "p2"))
    probes("probe2-0").sendNext(mkElem(4, 0, "p2-4"))
    probes("probe3-0").sendNext(mkElem(3, 0, "p3"))
    probes("probe4-0").sendNext(mkElem(3, 1, "p4"))
    probes("probe5-0").sendNext(mkElem(5, 1, "p5"))
    probes("probe5-0").sendComplete()
    sink.expectNoMessage(20.milliseconds)

    clue("lower the threshold, change probe2 config, remove probe5, add probe6") {
      val config2 = OrderedBucketMergeConfig(
        PositiveInt.one,
        NonEmpty(
          Map,
          "probe1" -> 0,
          "probe2" -> 1,
          "probe3" -> 0,
          "probe4" -> 0,
          "probe6" -> 0,
        ),
      )
      configQueue.offer(config2) shouldBe Enqueued
      sink.expectNext() shouldBe NewConfiguration(addMaterialized(config2, "probe2", "probe6"), 0)

      probes("probe2-0").expectCancellation()

      // Probe1's element is kept
      sink.expectNext() shouldBe
        OutputElement(NonEmpty(Map, "probe1" -> mkElem(1, 0, "p1")))
      // Probe2's elements are dropped
      // Non-deterministically choose between probe3 and probe4
      sink.expectNext() should (
        equal(OutputElement(NonEmpty(Map, "probe3" -> mkElem(3, 0, "p3")))) or
          equal(OutputElement(NonEmpty(Map, "probe4" -> mkElem(3, 1, "p4"))))
      )
      // Probe5 is stopped, so there is no non-determinism here for offset 5
      sink.expectNext() shouldBe
        OutputElement(NonEmpty(Map, "probe1" -> mkElem(5, 0, "p1-5")))
      // We can use the new probes to send elements
      probes("probe2-1").sendNext(mkElem(6, 0, "p2-6"))
      sink.expectNext() shouldBe OutputElement(NonEmpty(Map, "probe2" -> mkElem(6, 0, "p2-6")))
      probes("probe6-0").sendNext(mkElem(7, 0, "p6-7"))
      sink.expectNext() shouldBe OutputElement(NonEmpty(Map, "probe6" -> mkElem(7, 0, "p6-7")))

      // Test that all the probes whose elements have not been emitted are evicted
      // Do this one by one to avoid signalling races
      probes("probe4-0").sendNext(mkElem(8, 0, "p4-8"))
      sink.expectNext() shouldBe OutputElement(NonEmpty(Map, "probe4" -> mkElem(8, 0, "p4-8")))
      probes("probe3-0").sendNext(mkElem(9, 0, "p3-9"))
      sink.expectNext() shouldBe OutputElement(NonEmpty(Map, "probe3" -> mkElem(9, 0, "p3-9")))
    }

    val config3 = clue("raise the threshold again to 3 and fill the buckets") {
      val config3 = OrderedBucketMergeConfig(
        PositiveInt.tryCreate(3),
        NonEmpty(
          Map,
          "probe1" -> 0,
          "probe2" -> 1,
          "probe3" -> 0,
          "probe4" -> 0,
          "probe5" -> 0,
          "probe6" -> 0,
          "probe7" -> 0,
          "probe8" -> 0,
        ),
      )
      configQueue.offer(config3) shouldBe Enqueued
      sink.expectNext() shouldBe NewConfiguration(
        addMaterialized(config3, "probe5", "probe7", "probe8"),
        9,
      )

      probes("probe1-0").sendNext(mkElem(10, 0, "p1-10"))
      probes("probe2-1").sendNext(mkElem(10, 1, "p2-10"))
      probes("probe3-0").sendNext(mkElem(10, 0, "p3-10"))
      probes("probe4-0").sendNext(mkElem(11, 0, "p4-11"))
      probes("probe5-0").sendNext(mkElem(12, 0, "p5-12"))
      probes("probe6-0").sendNext(mkElem(12, 0, "p6-12")).sendComplete()
      probes("probe7-0").sendNext(mkElem(12, 2, "p7-12"))
      probes("probe8-0").sendNext(mkElem(11, 1, "p8-10")).sendComplete()
      sink.expectNoMessage(20.milliseconds)

      config3
    }

    clue("lower the threshold to 2 and make sure that everything is cleaned up") {
      val config4 = config3.copy(threshold = PositiveInt.tryCreate(2))
      configQueue.offer(config4) shouldBe Enqueued
      sink.expectNext() shouldBe NewConfiguration(addMaterialized(config4), 9)

      // Probe2's bucket has not reached the threshold, so there's no non-determinism here
      sink.expectNext() shouldBe OutputElement(
        NonEmpty(Map, "probe1" -> mkElem(10, 0, "p1-10"), "probe3" -> mkElem(10, 0, "p3-10"))
      )
      // We do not really care whether the termination of probe8 comes before element 12
      val next1 = sink.expectNext()
      val next2 = sink.expectNext()
      Set(next1, next2) shouldBe Set(
        OutputElement(
          NonEmpty(Map, "probe5" -> mkElem(12, 0, "p5-12"), "probe6" -> mkElem(12, 0, "p6-12"))
        ),
        ActiveSourceTerminated("probe8", None),
      )
      // However, the termination signal for probe6 must come after the bucket that contains probe6's element
      sink.expectNext() shouldBe ActiveSourceTerminated("probe6", None)

      // Probes 2, 4, and 7 should have their elements evicted.
      // Check that this is the case
      probes("probe7-0").sendComplete()
      sink.expectNext() shouldBe ActiveSourceTerminated("probe7", None)
      probes("probe2-1").sendNext(mkElem(13, 0, "p2-13"))
      probes("probe4-0").sendNext(mkElem(13, 0, "p4-13"))
      sink.expectNext() shouldBe OutputElement(
        NonEmpty(Map, "probe2" -> mkElem(13, 0, "p2-13"), "probe4" -> mkElem(13, 0, "p4-13"))
      )
    }

    configQueue.complete()
    sink.expectComplete()
    doneF.futureValue
  }

  "reconfiguration synchronizes with the completion future" in assertAllStagesStopped {
    val promise = Promise[Done]()

    val ops = mkOps(0) { (name, _config, offset, _prior) =>
      if (name == "slow-doneF-source") {
        Source.single(Elem(Bucket(offset + 1, 0), "")).viaMat(KillSwitches.single) {
          (_, killSwitch) => (killSwitch, promise.future, matFor(name))
        }
      } else
        Source.empty.mapMaterializedValue(_ =>
          (noOpKillSwitch, Future.successful(Done), matFor(name))
        )
    }
    val ((configQueue, doneF), sink) =
      Source.queue(1).viaMat(mkHub(ops))(Keep.both).toMat(TestSink.probe)(Keep.both).run()

    val config = OrderedBucketMergeConfig(
      PositiveInt.tryCreate(2),
      NonEmpty(Map, "slow-doneF-source" -> 1, "another-source" -> 1),
    )
    configQueue.offer(config) shouldBe Enqueued

    sink.request(10)
    sink.expectNext() shouldBe NewConfiguration(
      addMaterialized(config, "slow-doneF-source", "another-source"),
      0,
    )
    sink.expectNext() shouldBe ActiveSourceTerminated("another-source", None)

    val config2 = OrderedBucketMergeConfig(
      PositiveInt.one,
      NonEmpty(Map, "yet-another-source" -> 1),
    )
    configQueue.offer(config2) shouldBe Enqueued
    sink.expectNext() shouldBe NewConfiguration(addMaterialized(config2, "yet-another-source"), 0)
    sink.expectNext() shouldBe ActiveSourceTerminated("yet-another-source", None)

    configQueue.complete()
    sink.expectComplete()
    always(20.milliseconds) {
      doneF.isCompleted shouldBe false
    }

    promise.success(Done)
    doneF.futureValue
  }

  "propagate failures" in assertAllStagesStopped {
    val promise = Promise[Done]()
    val sourceEx = new Exception("Source failed")
    val ops = mkOps(0) { (name, _config, _offset, _prior) =>
      Source
        .single(())
        .map(_ => throw sourceEx)
        .mapMaterializedValue(_ => (noOpKillSwitch, promise.future, matFor(name)))
    }
    val ((configQueue, doneF), sink) =
      Source.queue(1).viaMat(mkHub(ops))(Keep.both).toMat(TestSink.probe)(Keep.both).run()
    val config = OrderedBucketMergeConfig(PositiveInt.one, NonEmpty(Map, "one" -> 1))
    configQueue.offer(config) shouldBe Enqueued

    sink.request(5)
    sink.expectNext() shouldBe NewConfiguration(addMaterialized(config, "one"), 0)
    sink.expectNext() shouldBe ActiveSourceTerminated("one", Some(sourceEx))

    val configEx = new Exception("Config stream failed")
    configQueue.fail(configEx)
    sink.expectError() shouldBe configEx

    always(20.milliseconds) {
      doneF.isCompleted shouldBe false
    }
    promise.success(Done)
    doneF.futureValue
  }

  "propagate cancellations" in assertAllStagesStopped {
    def mkElem(i: Int): Elem = Elem(Bucket(i, 0), s"$i")

    val pulledElems = new AtomicReference[Seq[Int]](Seq.empty[Int])
    def observeElem(i: Int): Int = {
      logger.debug(s"Observing element $i")
      pulledElems.updateAndGet(_ :+ i)
      i
    }

    val ops = mkOps(0) { (name, _config, _offset, _prior) =>
      Source(1 to 10).map(observeElem).map(mkElem).watchTermination() { (_, doneF) =>
        (noOpKillSwitch, doneF, matFor(name))
      }
    }
    val configSourceProbe = TestSource.probe[OrderedBucketMergeConfig[Name, Config]]
    val ((configSource, doneF), sink) =
      configSourceProbe.viaMat(mkHub(ops))(Keep.both).toMat(TestSink.probe)(Keep.both).run()

    val config = OrderedBucketMergeConfig(PositiveInt.one, NonEmpty(Map, "one" -> 1))
    configSource.sendNext(config)

    sink.request(2)
    sink.expectNext() shouldBe NewConfiguration(addMaterialized(config, "one"), 0)
    sink.expectNext() shouldBe OutputElement(NonEmpty(Map, "one" -> mkElem(1)))
    sink.cancel()

    configSource.expectCancellation()
    doneF.futureValue
    // We can't drain the sources upon cancellation :-(
    pulledElems.get() shouldBe (1 to 2)
  }

  "remember the prior element" in assertAllStagesStopped {
    def mkElem(name: Name, i: Int): Elem = Elem(Bucket(i, 0), s"$name-$i")
    val priors = new AtomicReference[Seq[(Name, Option[Elem])]](Seq.empty)

    val ops = mkOps(10) { (name, _config, offset, prior) =>
      priors.updateAndGet(_ :+ (name -> prior)).discard
      Source(offset + 1 to offset + 2)
        .map(mkElem(name, _))
        .concat(Source.never)
        .viaMat(KillSwitches.single)(Keep.right)
        .watchTermination()((killSwitch, doneF) => (killSwitch, doneF, matFor(name)))
    }
    val ((configQueue, doneF), sink) =
      Source.queue(1).viaMat(mkHub(ops))(Keep.both).toMat(TestSink.probe)(Keep.both).run()

    val config1 = OrderedBucketMergeConfig(PositiveInt.one, NonEmpty(Map, "one" -> 1))
    configQueue.offer(config1) shouldBe Enqueued

    sink.request(100)
    sink.expectNext(NewConfiguration(addMaterialized(config1, "one"), 10))
    sink.expectNext(OutputElement(NonEmpty(Map, "one" -> mkElem("one", 11))))
    sink.expectNext(OutputElement(NonEmpty(Map, "one" -> mkElem("one", 12))))

    val config2 = OrderedBucketMergeConfig(PositiveInt.one, NonEmpty(Map, "one" -> 1, "two" -> 1))
    configQueue.offer(config2) shouldBe Enqueued
    sink.expectNext(NewConfiguration(addMaterialized(config2, "two"), 12))
    sink.expectNext(OutputElement(NonEmpty(Map, "two" -> mkElem("two", 13))))
    sink.expectNext(OutputElement(NonEmpty(Map, "two" -> mkElem("two", 14))))

    val config3 =
      OrderedBucketMergeConfig(PositiveInt.tryCreate(2), NonEmpty(Map, "one" -> 2, "two" -> 2))
    configQueue.offer(config3) shouldBe Enqueued
    sink.expectNext(NewConfiguration(addMaterialized(config3, "one", "two"), 14))
    sink.expectNext(
      OutputElement(NonEmpty(Map, "one" -> mkElem("one", 15), "two" -> mkElem("two", 15)))
    )
    sink.expectNext(
      OutputElement(NonEmpty(Map, "one" -> mkElem("one", 16), "two" -> mkElem("two", 16)))
    )

    configQueue.complete()
    doneF.futureValue

    priors.get() should (equal(
      Seq(
        "one" -> None,
        "two" -> Some(mkElem("one", 12)),
        "one" -> Some(mkElem("two", 14)),
        "two" -> Some(mkElem("two", 14)),
      )
      // The order of the last two elements is non-deterministic
    ) or equal(
      Seq(
        "one" -> None,
        "two" -> Some(mkElem("one", 12)),
        "two" -> Some(mkElem("two", 14)),
        "one" -> Some(mkElem("two", 14)),
      )
    ))
  }

  "initially pass the prior element from Ops" in assertAllStagesStopped {
    def mkElem(name: Name, i: Int): Elem = Elem(Bucket(i, 0), s"$name-$i")

    val priors = new AtomicReference[Seq[(Name, Option[Elem])]](Seq.empty)

    val ops = new OrderedBucketMergeHubOps[Name, Elem, Config, Offset, M] {
      override type PriorElement = Elem
      override type Bucket = OrderedBucketMergeHubTest.this.Bucket
      override def prettyBucket: Pretty[Bucket] = implicitly[Pretty[Bucket]]
      override def bucketOf(elem: Elem): Bucket = elem.bucket
      override def orderingOffset: Ordering[Offset] = implicitly[Ordering[Offset]]
      override def offsetOfBucket(bucket: Bucket): Offset = bucket.offset
      override def exclusiveLowerBoundForBegin: Offset = 10
      override def priorElement: Option[Elem] = Some(mkElem("prior", 10))
      override def toPriorElement(output: OutputElement[Name, Elem]): Elem = output.elem.head1._2
      override def traceContextOf(elem: Elem): TraceContext = TraceContext.empty
      override def makeSource(
          name: Name,
          config: Config,
          exclusiveStart: Offset,
          priorElement: Option[Elem],
      ): Source[Elem, (KillSwitch, Future[Done], M)] = {
        priors.getAndUpdate(_ :+ (name -> priorElement)).discard
        Source
          .empty[Elem]
          .viaMat(KillSwitches.single)(Keep.right)
          .watchTermination()((killSwitch, doneF) => (killSwitch, doneF, matFor(name)))
      }
    }

    val ((configQueue, doneF), sink) =
      Source.queue(1).viaMat(mkHub(ops))(Keep.both).toMat(TestSink.probe)(Keep.both).run()

    val config1 =
      OrderedBucketMergeConfig(PositiveInt.one, NonEmpty(Map, "one" -> 1))
    configQueue.offer(config1) shouldBe Enqueued

    sink.request(10)
    sink.expectNext(NewConfiguration(addMaterialized(config1, "one"), 10))
    sink.expectNext(ActiveSourceTerminated("one", None))
    configQueue.complete()
    doneF.futureValue

    priors.get shouldBe Seq("one" -> Some(mkElem("prior", 10)))
  }
}
