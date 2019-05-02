// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.akkastreams

import java.util.Random
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, TimeUnit}

import akka.stream.DelayOverflowStrategy
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.platform.akkastreams.dispatcher.{Dispatcher, SteppingMode}
import com.digitalasset.platform.akkastreams.dispatcher.SteppingMode.{OneAfterAnother, RangeQuery}
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.{Assertion, AsyncWordSpec, BeforeAndAfter, Matchers}

import scala.collection.immutable
import scala.collection.immutable.TreeMap
import scala.concurrent.Future.successful
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any"
  )
)
class DispatcherSpec
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with BeforeAndAfter
    with Matchers
    with FutureTimeouts
    with AsyncTimeLimitedTests {

  // Newtype wrappers to avoid type mistakes
  case class Value(v: Int)

  case class Index(i: Int) {
    def next = Index(i + 1)
  }

  object Index {
    implicit val ordering: Ordering[Index] = new Ordering[Index] {
      override def compare(x: Index, y: Index): Int = Ordering[Int].compare(x.i, y.i)
    }
  }

  /*
  The values are stored indexed by Index.
  The Indices form a linked list, indexed by successorStore.
   */
  val r = new Random()
  private val store = new AtomicReference(TreeMap.empty[Index, Value])
  private val genesis = Index(0)
  private val latest = new AtomicReference(genesis)
  private val publishedHead = new AtomicReference(genesis)

  implicit val ec: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(128))

  after {
    clearUp()
  }

  def clearUp() = {
    store.set(TreeMap.empty)
    latest.set(genesis)
    publishedHead.set(genesis)
  }

  def valueGenerator: Index => Value = i => Value(i.i)

  def gen(
      count: Int,
      publishTo: Option[Dispatcher[Index, Value]] = None,
      meanDelayMs: Int = 0): IndexedSeq[(Index, Value)] = {
    def genManyHelper(i: Index, count: Int): Stream[(Index, Value)] = {
      if (count == 0) {
        Stream.empty
      } else {
        val next = Stream
          .iterate(i)(i => i.next)
          .filter(i => i != latest.get() && store.get().get(i).isEmpty)
          .head
        val v = valueGenerator(i)
        store.updateAndGet(_ + (i -> v))
        latest.set(next)
        publishTo foreach { d =>
          d.signalNewHead(next)
        }
        Thread.sleep(r.nextInt(meanDelayMs + 1).toLong * 2)
        Stream.cons((i, v), genManyHelper(next, count - 1))
      }
    }

    genManyHelper(latest.get(), count).toIndexedSeq.map { case (i, v) => (i, v) }
  }

  def publish(head: Index, dispatcher: Dispatcher[Index, Value], meanDelayMs: Int = 0): Unit = {
    publishedHead.set(head)
    dispatcher.signalNewHead(head)
    Thread.sleep(r.nextInt(meanDelayMs + 1).toLong * 2)
  }

  /**
    * Collect the actual results between start (exclusive) and stop (inclusive) from the given Dispatcher,
    * then cancels the obtained stream.
    */
  private def collect(start: Index, stop: Index, src: Dispatcher[Index, Value], delayMs: Int = 0) =
    if (delayMs > 0) {
      src
        .startingAt(start)
        .delay(Duration(delayMs.toLong, TimeUnit.MILLISECONDS), DelayOverflowStrategy.backpressure)
        .takeWhile(_._1 != stop, inclusive = true)
        .runWith(Sink.collection)
    } else {
      src
        .startingAt(start)
        .takeWhile(_._1 != stop, inclusive = true)
        .runWith(Sink.collection)
    }

  private val oneAfterAnotherSteppingMode =
    OneAfterAnother[Index, Value]((i, _) => i.next, i => successful(store.get()(i)))

  private def slowOneAfterAnotherSteppingMode(delayMs: Int) =
    OneAfterAnother[Index, Value](
      (i, _) => {
        Thread.sleep(r.nextInt(delayMs + 1).toLong * 2)
        i.next
      },
      i =>
        Future {
          Thread.sleep(r.nextInt(delayMs + 1).toLong * 2)
          store.get()(i)
      })

  private val rangeQuerySteppingMode = RangeQuery[Index, Value](
    (startInclusive, endExclusive) => Source(store.get().range(startInclusive, endExclusive))
  )

  private def slowRangeQuerySteppingMode(delayMs: Int) = RangeQuery[Index, Value](
    (startInclusive, endExclusive) =>
      Source(store.get().range(startInclusive, endExclusive))
        .throttle(1, delayMs.milliseconds * 2)
  )

  def vanillaDispatcher(
      begin: Index = genesis,
      end: Index = genesis,
      steppingMode: SteppingMode[Index, Value]): Dispatcher[Index, Value] =
    Dispatcher[Index, Value](steppingMode, begin, end)

  def slowDispatcher(steppingMode: SteppingMode[Index, Value]): Dispatcher[Index, Value] =
    Dispatcher[Index, Value](
      steppingMode,
      genesis,
      genesis
    )

  private def forAllSteppingModes(
      oneAfterAnother: OneAfterAnother[Index, Value] = oneAfterAnotherSteppingMode,
      rangeQuery: RangeQuery[Index, Value] = rangeQuerySteppingMode)(
      f: SteppingMode[Index, Value] => Future[Assertion]): Future[Assertion] =
    for {
      _ <- f(oneAfterAnother)
      _ = clearUp()
      _ <- f(rangeQuery)
    } yield succeed

  "A Dispatcher" should {

    "fail to initialize if end index < begin index" in {
      forAllSteppingModes() { sm =>
        recoverToSucceededIf[IllegalArgumentException](
          Future(vanillaDispatcher(Index(0), Index(-1), sm)))
      }
    }

    "return errors after being started and stopped" in {
      forAllSteppingModes() { sm =>
        val dispatcher = vanillaDispatcher(steppingMode = sm)

        dispatcher.close()

        dispatcher.signalNewHead(Index(1)) // should not throw
        dispatcher
          .startingAt(Index(0))
          .runWith(Sink.ignore)
          .failed
          .map(_ shouldBe a[IllegalStateException])
      }
    }

    "work with one outlet" in {
      forAllSteppingModes() { sm =>
        val dispatcher = vanillaDispatcher(steppingMode = sm)
        val pairs = gen(100)
        val out = collect(genesis, pairs.last._1, dispatcher)
        publish(latest.get(), dispatcher)
        out.map(_ shouldEqual pairs)
      }
    }

    "complete when the dispatcher completes" in {
      forAllSteppingModes() { sm =>
        val dispatcher = vanillaDispatcher(steppingMode = sm)
        val pairs50 = gen(50)
        val pairs100 = gen(50)
        val i49 = pairs50.last._1

        publish(i49.next, dispatcher)
        // latest.get() will never be published
        val out = collect(i49.next, latest.get(), dispatcher)
        publish(latest.get(), dispatcher)

        dispatcher.close()

        out.map(_ shouldEqual pairs100)
      }
    }

    "work with mid-stream subscriptions" in {
      forAllSteppingModes() { sm =>
        val dispatcher = vanillaDispatcher(steppingMode = sm)

        val pairs50 = gen(50)
        val pairs100 = gen(50)
        val i49 = pairs50.last._1

        publish(i49.next, dispatcher)
        val out = collect(i49.next, pairs100.last._1, dispatcher)
        publish(latest.get(), dispatcher)

        out.map(_ shouldEqual pairs100)
      }
    }

    "work with mid-stream cancellation" in {
      forAllSteppingModes() { sm =>
        val dispatcher = vanillaDispatcher(steppingMode = sm)

        val pairs50 = gen(50)
        val i50 = pairs50.last._1
        // the below cancels the stream after reaching element 50
        val out = collect(genesis, i50, dispatcher)
        gen(50, publishTo = Some(dispatcher))

        out.map(_ shouldEqual pairs50)
      }
    }

    "work with many outlets at different start/end indices" in {
      forAllSteppingModes() { sm =>
        val dispatcher = vanillaDispatcher(steppingMode = sm)

        val pairs25 = gen(25)
        val pairs50 = gen(25)
        val pairs75 = gen(25)
        val pairs100 = gen(25)
        val i25 = pairs25.last._1
        val i50 = pairs50.last._1
        val i75 = pairs75.last._1

        val outF = collect(genesis, i50, dispatcher)
        publish(i25.next, dispatcher)
        val out25F = collect(i25.next, i75, dispatcher)
        publish(i50.next, dispatcher)
        val out50F = collect(i50.next, latest.get(), dispatcher)
        publish(i75.next, dispatcher)
        val out75F = collect(i75.next, latest.get(), dispatcher)
        publish(latest.get(), dispatcher)

        dispatcher.close()

        validate4Sections(pairs25, pairs50, pairs75, pairs100, outF, out25F, out50F, out75F)
      }
    }

    "work with slow producers and consumers" in {
      forAllSteppingModes(slowOneAfterAnotherSteppingMode(10), slowRangeQuerySteppingMode(10)) {
        sm =>
          val dispatcher = slowDispatcher(sm)

          val pairs25 = gen(25)
          val pairs50 = gen(25)
          val pairs75 = gen(25)
          val pairs100 = gen(25)
          val i25 = pairs25.last._1
          val i50 = pairs50.last._1
          val i75 = pairs75.last._1

          val outF = collect(genesis, i50, dispatcher, delayMs = 10)
          publish(i25.next, dispatcher)
          val out25F = collect(i25.next, i75, dispatcher, delayMs = 10)
          publish(i50.next, dispatcher)
          val out50F = collect(i50.next, latest.get(), dispatcher, delayMs = 10)
          publish(i75.next, dispatcher)
          val out75F = collect(i75.next, latest.get(), dispatcher, delayMs = 10)
          publish(latest.get(), dispatcher)

          dispatcher.close()

          validate4Sections(pairs25, pairs50, pairs75, pairs100, outF, out25F, out50F, out75F)
      }
    }

    "handle subscriptions for future elements by waiting for the ledger end to reach them" in {
      forAllSteppingModes() { sm =>
        val dispatcher = vanillaDispatcher(steppingMode = sm)

        val startIndex = 10
        val pairs25 = gen(25).drop(startIndex)
        val i25 = pairs25.last._1

        val resultsF = collect(Index(startIndex), i25, dispatcher)
        publish(i25.next, dispatcher)
        for {
          results <- resultsF
        } yield {
          dispatcher.close()
          results shouldEqual pairs25
        }
      }
    }

    "stall subscriptions for future elements until the ledger end reaches the start index" in {
      val dispatcher = vanillaDispatcher(steppingMode = oneAfterAnotherSteppingMode)

      val startIndex = 10
      val pairs25 = gen(25).drop(startIndex)
      val i25 = pairs25.last._1

      expectTimeout(collect(Index(startIndex), i25, dispatcher), 1.second).andThen {
        case _ => dispatcher.close()
      }
    }

    "tolerate non-monotonic Head updates" in {
      val dispatcher = vanillaDispatcher(steppingMode = oneAfterAnotherSteppingMode)
      val pairs = gen(100)
      val out = collect(genesis, pairs.last._1, dispatcher)
      val updateCount = 10
      val random = new Random()
      1.to(updateCount).foreach(_ => dispatcher.signalNewHead(Index(random.nextInt(100))))
      dispatcher.signalNewHead(Index(100))
      out.map(_ shouldEqual pairs).andThen {
        case _ => dispatcher.close()
      }
    }
  }

  private def validate4Sections(
      pairs25: IndexedSeq[(Index, Value)],
      pairs50: IndexedSeq[(Index, Value)],
      pairs75: IndexedSeq[(Index, Value)],
      pairs100: IndexedSeq[(Index, Value)],
      outF: Future[immutable.IndexedSeq[(Index, Value)]],
      out25F: Future[immutable.IndexedSeq[(Index, Value)]],
      out50F: Future[immutable.IndexedSeq[(Index, Value)]],
      out75F: Future[immutable.IndexedSeq[(Index, Value)]]) = {
    for {
      out <- outF
      out25 <- out25F
      out50 <- out50F
      out75 <- out75F
    } yield {
      out shouldEqual pairs25 ++ pairs50
      out25 shouldEqual pairs50 ++ pairs75
      out50 shouldEqual pairs75 ++ pairs100
      out75 shouldEqual pairs100
    }
  }

  override def timeLimit: Span = 30.seconds
}
