// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pekkostreams.dispatcher

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.pekkostreams.FutureTimeouts
import com.digitalasset.canton.pekkostreams.dispatcher.DispatcherImpl.Incrementable
import com.digitalasset.canton.pekkostreams.dispatcher.SubSource.RangeSource
import com.digitalasset.canton.util.Thereafter.syntax.*
import org.apache.pekko.stream.DelayOverflowStrategy
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScaledTimeSpans}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar.*
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfter}

import java.util.Random
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.immutable
import scala.collection.immutable.TreeMap
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Failure, Success}

class DispatcherSpec
    extends AsyncWordSpec
    with PekkoBeforeAndAfterAll
    with BeforeAndAfter
    with Matchers
    with FutureTimeouts
    with ScaledTimeSpans
    with AsyncTimeLimitedTests
    with BaseTest {

  // Newtype wrappers to avoid type mistakes
  case class Value(v: Int)

  case class Index(i: Int) extends Ordered[Index] with Incrementable[Index] {
    def increment: Index = Index(i + 1)
    def compare(that: Index): Int = i.compare(that.i)
  }

  /*
  The values are stored indexed by Index.
  The Indices form a linked list, indexed by successorStore.
   */
  val r = new Random()
  private val store = new AtomicReference(TreeMap.empty[Index, Value])
  private val firstIndex = Index(1)
  private val nextIndex = new AtomicReference(firstIndex)

  implicit val ec: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(128))

  after {
    clearUp()
  }

  def clearUp() = {
    store.set(TreeMap.empty)
    nextIndex.set(firstIndex)
  }

  def valueGenerator: Index => Value = i => Value(i.i)

  def gen(
      count: Int,
      publishTo: Option[Dispatcher[Index]] = None,
      meanDelayMs: Int = 0,
  ): IndexedSeq[(Index, Value)] = {
    def genManyHelper(i: Index, count: Int): LazyList[(Index, Value)] =
      if (count == 0) {
        LazyList.empty
      } else {
        val next = LazyList
          .iterate(i)(i => i.increment)
          .filter(i => i != nextIndex.get() && !store.get().contains(i))
          .head
        val v = valueGenerator(i)
        store.updateAndGet(_ + (i -> v))
        nextIndex.set(next)
        publishTo foreach { d =>
          d.signalNewHead(i)
        }
        blocking(Threading.sleep(r.nextInt(meanDelayMs + 1).toLong * 2))
        LazyList.cons((i, v), genManyHelper(next, count - 1))
      }

    genManyHelper(nextIndex.get(), count).toIndexedSeq.map { case (i, v) => (i, v) }
  }

  def publish(head: Index, dispatcher: Dispatcher[Index], meanDelayMs: Int = 0): Unit = {
    dispatcher.signalNewHead(head)
    blocking(
      Threading.sleep(r.nextInt(meanDelayMs + 1).toLong * 2)
    )
  }

  /** Collect the actual results between start (exclusive) and stop (inclusive) from the given
    * Dispatcher, then cancels the obtained stream.
    */
  private def collect(
      start: Option[Index],
      stop: Index,
      src: Dispatcher[Index],
      subSrc: SubSource[Index, Value],
      delayMs: Int = 0,
  ): Future[immutable.IndexedSeq[(Index, Value)]] =
    if (delayMs > 0) {
      src
        .startingAt(startExclusive = start, subSource = subSrc, endInclusive = Some(stop))
        .delay(Duration(delayMs.toLong, TimeUnit.MILLISECONDS), DelayOverflowStrategy.backpressure)
        .runWith(Sink.collection)
    } else {
      src
        .startingAt(start, subSrc, Some(stop))
        .runWith(Sink.collection)
    }

  private val rangeQuerySteppingMode = RangeSource[Index, Value]((startInclusive, endInclusive) =>
    Source(
      store
        .get()
        .rangeFrom(startInclusive)
        .rangeTo(endInclusive)
    )
  )

  private def slowRangeQuerySteppingMode(delayMs: Int) =
    RangeSource[Index, Value]((startInclusive, endInclusive) =>
      Source(
        store
          .get()
          .rangeFrom(startInclusive)
          .rangeTo(endInclusive)
      )
        .throttle(1, delayMs.milliseconds * 2)
    )

  def newDispatcher(
      begin: Index = firstIndex,
      end: Option[Index] = None,
  ): Dispatcher[Index] =
    Dispatcher[Index](name = "test", firstIndex = begin, headAtInitialization = end)

  private def forAllSteppingModes(
      rangeQuery: RangeSource[Index, Value] = rangeQuerySteppingMode
  )(f: SubSource[Index, Value] => Future[Assertion]): Future[Assertion] =
    for {
      _ <- f(rangeQuery)
    } yield succeed

  "A Dispatcher" should {

    "fail to initialize if end index < begin index" in {
      forAllSteppingModes() { _ =>
        recoverToSucceededIf[IllegalArgumentException](
          Future(newDispatcher(firstIndex, Some(Index(-1))))
        )
      }
    }

    "return errors after being started and stopped" in {
      forAllSteppingModes() { subSrc =>
        val dispatcher = newDispatcher()

        dispatcher.shutdown().discard

        dispatcher.signalNewHead(Index(1)) // should not throw
        dispatcher
          .startingAt(startExclusive = None, subSource = subSrc)
          .runWith(Sink.ignore)
          .failed
          .map(_ shouldBe a[IllegalStateException])
      }
    }

    "work with one outlet" in {
      forAllSteppingModes() { subSrc =>
        val dispatcher = newDispatcher()
        val pairs = gen(100)
        val out = collect(None, pairs.last._1, dispatcher, subSrc)
        publish(pairs.last._1, dispatcher)
        out.map(_ shouldEqual pairs)
      }
    }

    "complete when the dispatcher completes" in {
      forAllSteppingModes() { subSrc =>
        val dispatcher = newDispatcher()
        val pairs50 = gen(50)
        val pairs100 = gen(50)
        val i50 = pairs50.last._1
        val i100 = pairs100.last._1

        publish(i50, dispatcher)
        val out = collect(Some(i50), i100, dispatcher, subSrc)
        publish(i100, dispatcher)

        dispatcher.shutdown().discard

        out.map(_ shouldEqual pairs100)
      }
    }

    "fail when the dispatcher fails" in {
      forAllSteppingModes() { subSrc =>
        val dispatcher = newDispatcher()
        val pairs50 = gen(50)
        val i50 = pairs50.last._1
        val pairs100 = gen(50)
        val i100 = pairs100.last._1

        publish(i50, dispatcher)
        val out = collect(None, i100, dispatcher, subSrc)

        val expectedException = new RuntimeException("some exception")

        val newException = () => expectedException

        for {
          _ <- dispatcher.cancel(newException)
          _ = publish(i100, dispatcher)

          _ <- out.transform {
            case Failure(`expectedException`) => Success(())
            case Failure(other) =>
              fail(s"Expected stream failed with $expectedException but got $other")
            case Success(_) => fail("Expected stream failed")
          }
        } yield succeed
      }
    }

    "work with mid-stream subscriptions" in {
      forAllSteppingModes() { subSrc =>
        val dispatcher = newDispatcher()

        val pairs50 = gen(50)
        val pairs100 = gen(50)
        val i50 = pairs50.last._1
        val i100 = pairs100.last._1

        publish(i50, dispatcher)
        val out = collect(Some(i50), i100, dispatcher, subSrc)
        publish(i100, dispatcher)

        out.map(_ shouldEqual pairs100)
      }
    }

    "work with mid-stream cancellation" in {
      forAllSteppingModes() { subSrc =>
        val dispatcher = newDispatcher()

        val pairs50 = gen(50)
        val i50 = pairs50.last._1
        // the below cancels the stream after reaching element 50
        val out = collect(None, i50, dispatcher, subSrc)
        gen(50, publishTo = Some(dispatcher))

        out.map(_ shouldEqual pairs50)
      }
    }

    "work with many outlets at different start/end indices" in {
      forAllSteppingModes() { subSrc =>
        val dispatcher = newDispatcher()

        val pairs25 = gen(25)
        val pairs50 = gen(25)
        val pairs75 = gen(25)
        val pairs100 = gen(25)
        val i25 = pairs25.last._1
        val i50 = pairs50.last._1
        val i75 = pairs75.last._1
        val i100 = pairs100.last._1

        val outF = collect(None, i50, dispatcher, subSrc)
        publish(i25, dispatcher)
        val out25F = collect(Some(i25), i75, dispatcher, subSrc)
        publish(i50, dispatcher)
        val out50F = collect(Some(i50), i100, dispatcher, subSrc)
        publish(i75, dispatcher)
        val out75F = collect(Some(i75), i100, dispatcher, subSrc)
        publish(i100, dispatcher)

        dispatcher.shutdown().discard

        validate4Sections(pairs25, pairs50, pairs75, pairs100, outF, out25F, out50F, out75F)
      }
    }

    "work with slow producers and consumers" in {
      forAllSteppingModes(slowRangeQuerySteppingMode(10)) { subSrc =>
        val dispatcher = newDispatcher()

        val pairs25 = gen(25)
        val pairs50 = gen(25)
        val pairs75 = gen(25)
        val pairs100 = gen(25)
        val i25 = pairs25.last._1
        val i50 = pairs50.last._1
        val i75 = pairs75.last._1
        val i100 = pairs100.last._1

        val outF = collect(None, i50, dispatcher, subSrc, delayMs = 10)
        publish(i25, dispatcher)
        val out25F = collect(Some(i25), i75, dispatcher, subSrc, delayMs = 10)
        publish(i50, dispatcher)
        val out50F = collect(Some(i50), i100, dispatcher, subSrc, delayMs = 10)
        publish(i75, dispatcher)
        val out75F = collect(Some(i75), i100, dispatcher, subSrc, delayMs = 10)
        publish(i100, dispatcher)

        dispatcher.shutdown().discard

        validate4Sections(pairs25, pairs50, pairs75, pairs100, outF, out25F, out50F, out75F)
      }
    }

    "handle subscriptions for future elements by waiting for the ledger end to reach them" in {
      forAllSteppingModes() { subSrc =>
        val dispatcher = newDispatcher()

        val startIndex = 10
        val pairs25 = gen(25).drop(startIndex)
        val i25 = pairs25.last._1

        val resultsF = collect(Some(Index(startIndex)), i25, dispatcher, subSrc)
        publish(i25, dispatcher)
        for {
          results <- resultsF
        } yield {
          dispatcher.shutdown().discard
          results shouldEqual pairs25
        }
      }
    }

    "stall subscriptions for future elements until the ledger end reaches the start index" in {
      val dispatcher = newDispatcher()

      val startIndex = 10
      val pairs25 = gen(25).drop(startIndex)
      val i25 = pairs25.last._1

      expectTimeout(
        collect(Some(Index(startIndex)), i25, dispatcher, rangeQuerySteppingMode),
        1.second,
      ).thereafterF { _ =>
        dispatcher.shutdown()
      }
    }

    "tolerate non-monotonic Head updates" in {
      val dispatcher = newDispatcher()
      val pairs = gen(100)
      val out = collect(None, pairs.last._1, dispatcher, rangeQuerySteppingMode)
      val updateCount = 10
      val random = new Random()
      1.to(updateCount).foreach(_ => dispatcher.signalNewHead(Index(random.nextInt(100))))
      dispatcher.signalNewHead(Index(100))
      out.map(_ shouldEqual pairs).thereafterF { _ =>
        dispatcher.shutdown()
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
      out75F: Future[immutable.IndexedSeq[(Index, Value)]],
  ) =
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

  override def timeLimit: Span = scaled(30.seconds)
}
