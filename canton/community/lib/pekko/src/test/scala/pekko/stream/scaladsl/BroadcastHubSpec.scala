/*
 * Copyright (C) 2015-2021 Lightbend Inc. <https://www.lightbend.com>
 */

// This test has been extracted from Pekko 2.6.18
// https://raw.githubusercontent.com/pekko/pekko/v2.6.18/pekko-stream-tests/src/test/scala/pekko/stream/scaladsl/HubSpec.scala
// Added tests are marked with NEW

package pekko.stream.scaladsl

import canton.community.lib.pekko.src.main.scala.pekko.stream.scaladsl.BroadcastHub

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.ThrottleMode
import org.apache.pekko.stream.testkit.StreamSpec
import org.apache.pekko.stream.testkit.TestPublisher
import org.apache.pekko.stream.testkit.TestSubscriber
import org.apache.pekko.stream.testkit.Utils.TE
import org.apache.pekko.stream.testkit.scaladsl.StreamTestKit._
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.apache.pekko.stream.testkit.scaladsl.TestSource

class BroadcastHubSpec extends StreamSpec {
  implicit val ec: ExecutionContext = system.dispatcher

  "BroadcastHub" must {

    "work in the happy case" in assertAllStagesStopped {
      val source = Source(1 to 10).runWith(BroadcastHub.sink(8))
      source.runWith(Sink.seq).futureValue should ===(1 to 10)
    }

    "send the same elements to consumers attaching around the same time" in assertAllStagesStopped {
      val (firstElem, source) =
        Source.maybe[Int].concat(Source(2 to 10)).toMat(BroadcastHub.sink(8))(Keep.both).run()

      val f1 = source.runWith(Sink.seq)
      val f2 = source.runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(1))
      f1.futureValue should ===(1 to 10)
      f2.futureValue should ===(1 to 10)
    }

    "send the same prefix to consumers attaching around the same time if one cancels earlier" in assertAllStagesStopped {
      val (firstElem, source) =
        Source.maybe[Int].concat(Source(2 to 20)).toMat(BroadcastHub.sink(8))(Keep.both).run()

      val f1 = source.runWith(Sink.seq)
      val f2 = source.take(10).runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(1))
      f1.futureValue should ===(1 to 20)
      f2.futureValue should ===(1 to 10)
    }

    "ensure that subsequent consumers see subsequent elements without gap" in assertAllStagesStopped {
      val source = Source(1 to 20).runWith(BroadcastHub.sink(8))
      source.take(10).runWith(Sink.seq).futureValue should ===(1 to 10)
      source.take(10).runWith(Sink.seq).futureValue should ===(11 to 20)
    }

    "send the same elements to consumers of different speed attaching around the same time" in assertAllStagesStopped {
      val (firstElem, source) =
        Source.maybe[Int].concat(Source(2 to 10)).toMat(BroadcastHub.sink(8))(Keep.both).run()

      val f1 = source.throttle(1, 10.millis, 3, ThrottleMode.shaping).runWith(Sink.seq)
      val f2 = source.runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(1))
      f1.futureValue should ===(1 to 10)
      f2.futureValue should ===(1 to 10)
    }

    "send the same elements to consumers of attaching around the same time if the producer is slow" in assertAllStagesStopped {
      val (firstElem, source) = Source
        .maybe[Int]
        .concat(Source(2 to 10))
        .throttle(1, 10.millis, 3, ThrottleMode.shaping)
        .toMat(BroadcastHub.sink(8))(Keep.both)
        .run()

      val f1 = source.runWith(Sink.seq)
      val f2 = source.runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(1))
      f1.futureValue should ===(1 to 10)
      f2.futureValue should ===(1 to 10)
    }

    "ensure that from two different speed consumers the slower controls the rate" in assertAllStagesStopped {
      val (firstElem, source) =
        Source.maybe[Int].concat(Source(2 to 20)).toMat(BroadcastHub.sink(1))(Keep.both).run()

      val f1 = source.throttle(1, 10.millis, 1, ThrottleMode.shaping).runWith(Sink.seq)
      // Second cannot be overwhelmed since the first one throttles the overall rate, and second allows a higher rate
      val f2 = source.throttle(10, 10.millis, 8, ThrottleMode.enforcing).runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(1))
      f1.futureValue should ===(1 to 20)
      f2.futureValue should ===(1 to 20)

    }

    "send the same elements to consumers attaching around the same time with a buffer size of one" in assertAllStagesStopped {
      val (firstElem, source) =
        Source.maybe[Int].concat(Source(2 to 10)).toMat(BroadcastHub.sink(1))(Keep.both).run()

      val f1 = source.runWith(Sink.seq)
      val f2 = source.runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(1))
      f1.futureValue should ===(1 to 10)
      f2.futureValue should ===(1 to 10)
    }

    "be able to implement a keep-dropping-if-unsubscribed policy with a simple Sink.ignore" in assertAllStagesStopped {
      val killSwitch = KillSwitches.shared("test-switch")
      val source = Source
        .fromIterator(() => Iterator.from(0))
        .via(killSwitch.flow)
        .runWith(BroadcastHub.sink(8))

      // Now the Hub "drops" elements until we attach a new consumer (Source.ignore consumes as fast as possible)
      source.runWith(Sink.ignore)

      // Now we attached a subscriber which will block the Sink.ignore to "take away" and drop elements anymore,
      // turning the BroadcastHub to a normal non-dropping mode
      val downstream = TestSubscriber.probe[Int]()
      source.runWith(Sink.fromSubscriber(downstream))

      downstream.request(1)
      val first = downstream.expectNext()

      for (i <- (first + 1) to (first + 10)) {
        downstream.request(1)
        downstream.expectNext(i)
      }

      downstream.cancel()

      killSwitch.shutdown()
    }

    "properly signal error to consumers" in assertAllStagesStopped {
      val upstream = TestPublisher.probe[Int]()
      val source = Source.fromPublisher(upstream).runWith(BroadcastHub.sink(8))

      val downstream1 = TestSubscriber.probe[Int]()
      val downstream2 = TestSubscriber.probe[Int]()
      source.runWith(Sink.fromSubscriber(downstream1))
      source.runWith(Sink.fromSubscriber(downstream2))

      downstream1.request(4)
      downstream2.request(8)

      // sending the first element is in a race with downstream subscribing
      // give a bit of time for the downstream to complete subscriptions
      Thread.sleep(100)

      (1 to 8).foreach(upstream.sendNext(_))

      downstream1.expectNext(1, 2, 3, 4)
      downstream2.expectNext(1, 2, 3, 4, 5, 6, 7, 8)

      downstream1.expectNoMessage(100.millis)
      downstream2.expectNoMessage(100.millis)

      upstream.sendError(TE("Failed"))

      downstream1.expectError(TE("Failed"))
      downstream2.expectError(TE("Failed"))
    }

    "properly signal completion to consumers arriving after producer finished" in assertAllStagesStopped {
      val source = Source.empty[Int].runWith(BroadcastHub.sink(8))
      // Wait enough so the Hub gets the completion. This is racy, but this is fine because both
      // cases should work in the end
      Thread.sleep(10)

      source.runWith(Sink.seq).futureValue should ===(Nil)
    }

    "remember completion for materialisations after completion" in {

      val (sourceProbe, source) = TestSource.probe[Unit].toMat(BroadcastHub.sink)(Keep.both).run()
      val sinkProbe = source.runWith(TestSink.probe[Unit])

      sourceProbe.sendComplete()

      sinkProbe.request(1)
      sinkProbe.expectComplete()

      // Materialize a second time. There was a race here, where we managed to enqueue our Source registration just
      // immediately before the Hub shut down.
      val sink2Probe = source.runWith(TestSink.probe[Unit])

      sink2Probe.request(1)
      sink2Probe.expectComplete()
    }

    "properly signal error to consumers arriving after producer finished" in assertAllStagesStopped {
      val source = Source.failed(TE("Fail!")).runWith(BroadcastHub.sink(8))
      // Wait enough so the Hub gets the completion. This is racy, but this is fine because both
      // cases should work in the end
      Thread.sleep(10)

      a[TE] shouldBe thrownBy {
        Await.result(source.runWith(Sink.seq), 3.seconds)
      }
    }

    "handle cancelled Sink" in assertAllStagesStopped {
      val in = TestPublisher.probe[Int]()
      val hubSource = Source.fromPublisher(in).runWith(BroadcastHub.sink(4))

      val out = TestSubscriber.probe[Int]()

      hubSource.runWith(Sink.cancelled)
      hubSource.runWith(Sink.fromSubscriber(out))

      out.ensureSubscription()

      out.request(10)
      in.expectRequest()
      in.sendNext(1)
      out.expectNext(1)
      in.sendNext(2)
      out.expectNext(2)
      in.sendNext(3)
      out.expectNext(3)
      in.sendNext(4)
      out.expectNext(4)
      in.sendNext(5)
      out.expectNext(5)

      in.sendComplete()
      out.expectComplete()
    }

    // NEW BEGIN
    "handle unregistration concurrent with registration" in assertAllStagesStopped {

      var sinkProbe1: TestSubscriber.Probe[Int] = null

      def registerConsumerCallback(id: Long): Unit =
        if (id == 1) {
          sinkProbe1.cancel()
          Thread.sleep(10)
        }

      val in = TestPublisher.probe[Int]()
      val hubSource = Source
        .fromPublisher(in)
        .runWith(Sink.fromGraph(new BroadcastHub[Int](2, registerConsumerCallback)))

      // Put one element into the buffer
      in.sendNext(15)

      // add a consumer to receive the first element
      val sinkProbe0 = hubSource.runWith(TestSink.probe[Int])
      sinkProbe0.request(1)
      sinkProbe0.expectNext(15)
      sinkProbe0.cancel()

      // put the next element into the buffer
      in.sendNext(16)

      // Add another consumer and kill it during registration
      sinkProbe1 = hubSource.runWith(TestSink.probe[Int])
      Thread.sleep(100)

      // Make sure that the element 16 isn't lost by reading it with a third consumer
      val sinkProbe2 = hubSource.runWith(TestSink.probe[Int])
      sinkProbe2.request(1)
      // This fails on the original BroadcastHub implementation because element 16 is overwritten by the double unregister call.
      sinkProbe2.expectNext(16)

      in.sendComplete()
      sinkProbe2.cancel()
    }
    // NEW END
  }
}
