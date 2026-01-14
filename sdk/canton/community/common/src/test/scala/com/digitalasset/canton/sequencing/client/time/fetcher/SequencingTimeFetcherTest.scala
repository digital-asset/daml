// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.time.fetcher

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.client.time.fetcher.SequencingTimeFetcher.*
import com.digitalasset.canton.sequencing.client.time.fetcher.SequencingTimeReadings.TimeReading
import com.digitalasset.canton.sequencing.client.time.fetcher.TimeSourcesAccessor.TimeSources
import com.digitalasset.canton.time.{
  Clock,
  NonNegativeFiniteDuration,
  PositiveFiniteDuration,
  SimClock,
}
import com.digitalasset.canton.topology.{SequencerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.Assertions.fail
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.immutable.Queue

class SequencingTimeFetcherTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  import SequencingTimeFetcherTest.*

  "Asking the current sequencing time info" should {
    "get the time readings and produce a result" in {
      val timeReadingsMock = mock[SequencingTimeReadings]
      when(timeReadingsMock.getTimeReadings(any[Option[NonNegativeFiniteDuration]]))
        .thenReturn(Map.empty)
      when(
        timeReadingsMock.validTimeInterval(any[Vector[CantonTimestamp]], any[PositiveInt])(
          any[TraceContext]
        )
      )
        .thenReturn(None)
      val timeSourcesAccessorMock = mock[TimeSourcesAccessor]
      when(timeSourcesAccessorMock.timeReadings).thenReturn(timeReadingsMock)
      val fetcher = newTimeFetcher(timeSourcesAccessorMock)

      fetcher.currentSequencingTimeInfo(None) shouldBe
        aSequencingTimeInfo
      verify(timeReadingsMock, times(1)).getTimeReadings(eqTo(None))
      verify(timeReadingsMock, times(1))
        .validTimeInterval(eqTo(Vector.empty), eqTo(PositiveInt.tryCreate(2)))(any[TraceContext])
      succeed
    }
  }

  "Checking whether the synchronizer has reached a sequencing time" when {

    "there are enough positives" should {
      "return true" in {
        val t = CantonTimestamp.MaxValue
        val timeReadingsMock = mock[SequencingTimeReadings]
        when(timeReadingsMock.getTimeReadings(any[Option[NonNegativeFiniteDuration]])).thenReturn(
          Map(
            sequencerIds(0) -> TimeReading(Some(t), ts(0)),
            sequencerIds(1) -> TimeReading(Some(t), ts(0)),
          )
        )
        val timeSourcesAccessorMock = mock[TimeSourcesAccessor]
        when(timeSourcesAccessorMock.timeReadings).thenReturn(timeReadingsMock)
        val fetcher = newTimeFetcher(timeSourcesAccessorMock)

        fetcher.hasReached(t, aTimeout).map { result =>
          verify(timeReadingsMock, times(1)).getTimeReadings(eqTo(None))
          result shouldBe true
        }
      }.failOnShutdown
    }

    "there aren't enough positives" when {

      "there aren't enough time sources" should {
        "return false immediately" in {
          val timeReadingsMock = mock[SequencingTimeReadings]
          when(timeReadingsMock.getTimeReadings(any[Option[NonNegativeFiniteDuration]])).thenReturn(
            Map(sequencerIds(0) -> TimeReading(Some(CantonTimestamp.MaxValue), ts(0)))
          )
          val timeSourcesAccessorMock = mock[TimeSourcesAccessor]
          when(timeSourcesAccessorMock.timeReadings).thenReturn(timeReadingsMock)
          val timeSourcesPool =
            spy(
              new TestTimeSourcesPool(
                sources = Seq.empty,
                trustThreshold = PositiveInt.tryCreate(2),
              )
            )

          val fetcher = newTimeFetcher(timeSourcesAccessorMock, timeSourcesPool)

          fetcher.hasReached(CantonTimestamp.MaxValue, aTimeout).map { result =>
            verify(timeSourcesAccessorMock, times(1)).timeReadings
            verify(timeReadingsMock, times(1)).getTimeReadings(eqTo(None))
            verify(timeSourcesPool, times(1))
              .timeSources(eqTo(PositiveInt.tryCreate(1)), eqTo(Set(sequencerIds(0))))(
                any[TraceContext]
              )
            result shouldBe false
          }
        }.failOnShutdown
      }

      "there are enough time sources" when {

        "querying the time sources and receiving enough positives" should {
          "return true" in {
            val timeReadingsMock = mock[SequencingTimeReadings]
            when(timeReadingsMock.getTimeReadings(any[Option[NonNegativeFiniteDuration]]))
              .thenReturn(
                Map(sequencerIds(0) -> TimeReading(Some(CantonTimestamp.MaxValue), ts(0))),
                Map(
                  sequencerIds(0) -> TimeReading(Some(CantonTimestamp.MaxValue), ts(0)),
                  sequencerIds(1) -> TimeReading(Some(CantonTimestamp.MaxValue), ts(0)),
                ),
              )
            val timeSourcesAccessorMock = mock[TimeSourcesAccessor]
            when(timeSourcesAccessorMock.timeReadings).thenReturn(timeReadingsMock)
            when(
              timeSourcesAccessorMock.queryTimeSources(
                any[TimeSources],
                any[PositiveFiniteDuration],
                any[Boolean],
              )(any[TraceContext])
            ).thenReturn(
              FutureUnlessShutdown.pure(
                Map(
                  sequencerIds(1) -> Some(CantonTimestamp.MaxValue)
                )
              )
            )
            val timeSourcesPool =
              spy(
                new TestTimeSourcesPool(
                  sources = Seq.fill(2)(
                    TestTimeSource(
                      FutureUnlessShutdown.pure(Some(CantonTimestamp.MaxValue))
                    )
                  ),
                  trustThreshold = PositiveInt.tryCreate(2),
                )
              )

            val fetcher = newTimeFetcher(timeSourcesAccessorMock, timeSourcesPool)

            fetcher
              .hasReached(
                CantonTimestamp.MaxValue,
                aTimeout,
                maxTimeReadingsAge = Some(NonNegativeFiniteDuration.Zero),
              )
              .map { result =>
                verify(timeReadingsMock, times(1)).getTimeReadings(
                  eqTo(Some(NonNegativeFiniteDuration.Zero))
                )
                verify(timeReadingsMock, times(1)).getTimeReadings(eqTo(None))
                verify(timeSourcesAccessorMock, times(1)).queryTimeSources(
                  argThat[TimeSources](_.keySet == Set(sequencerIds(1))),
                  eqTo(aTimeout),
                  eqTo(false),
                )(any[TraceContext])
                verify(timeSourcesPool, times(1))
                  .timeSources(eqTo(PositiveInt.tryCreate(1)), eqTo(Set(sequencerIds(0))))(
                    any[TraceContext]
                  )
                result shouldBe true
              }
          }.failOnShutdown
        }

        "querying the time sources and not receiving enough positives" when {

          "there is no time left" should {
            "return false" in {
              val timeReadingsMock = mock[SequencingTimeReadings]
              when(timeReadingsMock.getTimeReadings(any[Option[NonNegativeFiniteDuration]]))
                .thenReturn(
                  Map(sequencerIds(0) -> TimeReading(Some(CantonTimestamp.MaxValue), ts(0))),
                  Map(
                    sequencerIds(0) -> TimeReading(Some(CantonTimestamp.MaxValue), ts(0)),
                    sequencerIds(1) -> TimeReading(None, ts(0)),
                  ),
                )
              val timeSourcesPool =
                spy(
                  new TestTimeSourcesPool(
                    sources = Seq.fill(2)(
                      TestTimeSource(
                        FutureUnlessShutdown.pure(Some(CantonTimestamp.MaxValue))
                      )
                    ),
                    trustThreshold = PositiveInt.tryCreate(2),
                  )
                )
              val simClock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
              val timeSourcesAccessorSpiedFake =
                spy(
                  new TestTimeSourcesAccessor(
                    timeReadingsMock,
                    onQuery = () => simClock.advance(aTimeout.duration),
                  )
                )

              val fetcher = newTimeFetcher(timeSourcesAccessorSpiedFake, timeSourcesPool, simClock)

              fetcher.hasReached(CantonTimestamp.MaxValue, aTimeout).map { result =>
                verify(timeReadingsMock, times(2)).getTimeReadings(eqTo(None))
                verify(timeSourcesAccessorSpiedFake, times(1)).queryTimeSources(
                  argThat[TimeSources](
                    _.keySet == Set(sequencerIds(1))
                  ),
                  eqTo(aTimeout),
                  eqTo(false),
                )(any[TraceContext])
                verify(timeSourcesPool, times(1))
                  .timeSources(eqTo(PositiveInt.tryCreate(1)), eqTo(Set(sequencerIds(0))))(
                    any[TraceContext]
                  )
                result shouldBe false
              }
            }.failOnShutdown
          }

          "there is time left" should {
            "recurse" in {
              val timeReadingsMock = mock[SequencingTimeReadings]
              when(timeReadingsMock.getTimeReadings(any[Option[NonNegativeFiniteDuration]]))
                .thenReturn(
                  Map(sequencerIds(0) -> TimeReading(Some(CantonTimestamp.MaxValue), ts(0))),
                  Map(
                    sequencerIds(0) -> TimeReading(Some(CantonTimestamp.MaxValue), ts(0)),
                    sequencerIds(1) -> TimeReading(None, ts(0)),
                  ),
                )
              val timeSourcesPool =
                spy(
                  new TestTimeSourcesPool(
                    sources = Seq.fill(3)(
                      TestTimeSource(
                        FutureUnlessShutdown.pure(Some(CantonTimestamp.MaxValue))
                      )
                    ),
                    trustThreshold = PositiveInt.tryCreate(2),
                  )
                )
              val simClock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
              val timeSourcesAccessorSpiedFake =
                spy(
                  new TestTimeSourcesAccessor(
                    timeReadingsMock,
                    onQuery = () => simClock.advance(aTimeout.duration.dividedBy(2L)),
                  )
                )

              val fetcher = newTimeFetcher(timeSourcesAccessorSpiedFake, timeSourcesPool, simClock)

              fetcher
                .hasReached(
                  CantonTimestamp.MaxValue,
                  aTimeout,
                  aMaxTimeReadingsAge,
                )
                .map { result =>
                  verify(timeReadingsMock, times(1)).getTimeReadings(eqTo(aMaxTimeReadingsAge))
                  verify(timeReadingsMock, times(3)).getTimeReadings(eqTo(None))
                  verify(timeSourcesAccessorSpiedFake, times(1)).queryTimeSources(
                    argThat[TimeSources](
                      _.keySet == Set(sequencerIds(1))
                    ),
                    eqTo(aTimeout),
                    eqTo(false),
                  )(any[TraceContext])
                  verify(timeSourcesAccessorSpiedFake, times(1)).queryTimeSources(
                    argThat[TimeSources](
                      _.keySet == Set(sequencerIds(2))
                    ),
                    eqTo(PositiveFiniteDuration.tryCreate(aTimeout.duration.dividedBy(2L))),
                    eqTo(false),
                  )(any[TraceContext])
                  verify(timeSourcesPool, times(1))
                    .timeSources(eqTo(PositiveInt.tryCreate(1)), eqTo(Set(sequencerIds(0))))(
                      any[TraceContext]
                    )
                  verify(timeSourcesPool, times(1))
                    .timeSources(
                      eqTo(PositiveInt.tryCreate(1)),
                      eqTo(sequencerIds.slice(0, 2).toSet),
                    )(
                      any[TraceContext]
                    )
                  result shouldBe false
                }
            }.failOnShutdown
          }
        }
      }
    }
  }

  private def newTimeFetcher(
      timeSourcesAccessor: TimeSourcesAccessor,
      timeSourcesPool: TimeSourcesPool = new TestTimeSourcesPool(),
      localClock: Clock = wallClock,
  ) =
    new SequencingTimeFetcher(
      timeSourcesPool,
      timeSourcesAccessor,
      localClock,
      loggerFactory,
    )
}

object SequencingTimeFetcherTest {

  private[fetcher] class TestTimeSourcesPool(
      sources: Seq[TestTimeSource] = Seq.empty,
      trustThreshold: PositiveInt = PositiveInt.tryCreate(2),
  ) extends SequencingTimeFetcher.TimeSourcesPool {

    override def readTrustThreshold(): PositiveInt =
      trustThreshold

    override def timeSources(count: PositiveInt, exclusions: Set[SequencerId])(implicit
        traceContext: TraceContext
    ): Seq[
      (SequencerId, PositiveFiniteDuration => FutureUnlessShutdown[Option[CantonTimestamp]])
    ] =
      sources.zipWithIndex
        .map { case (source, i) =>
          sequencerIds(i) -> ((timeout: PositiveFiniteDuration) => source.fetchTime(timeout))
        }
        .filterNot { case (sequencerId, _) => exclusions.contains(sequencerId) }
        .take(count.unwrap)
  }

  private[fetcher] final case class TestTimeSource(
      results: FutureUnlessShutdown[Option[CantonTimestamp]]*
  ) {
    val invocationsCountRef = new AtomicInteger(0)
    val timeoutsRef: AtomicReference[Seq[PositiveFiniteDuration]] =
      new AtomicReference(Queue.empty[PositiveFiniteDuration])

    def fetchTime(
        timeout: PositiveFiniteDuration
    ): FutureUnlessShutdown[Option[CantonTimestamp]] = {
      val idx = invocationsCountRef.getAndIncrement()
      timeoutsRef.updateAndGet(_ :+ timeout)
      Option
        .when(results.isDefinedAt(idx))(results(idx))
        .getOrElse(fail(s"Time source invoked more than ${results.size} times"))
    }
  }

  private class TestTimeSourcesAccessor(
      readings: SequencingTimeReadings,
      onQuery: () => Unit,
  ) extends TimeSourcesAccessor {
    override def timeReadings: SequencingTimeReadings = readings

    override def queryTimeSources(
        timeSources: TimeSources,
        timeout: PositiveFiniteDuration,
        concurrent: Boolean,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[SequencerId, Option[CantonTimestamp]]] = {
      onQuery()
      FutureUnlessShutdown.pure(Map.empty)
    }
  }

  private[fetcher] def ts(seconds: Int) = CantonTimestamp.Epoch.plusSeconds(seconds.toLong)

  private[fetcher] val sequencerIds: Seq[SequencerId] =
    (0 to 3).map(n => SequencerId(UniqueIdentifier.tryCreate("ns", s"$n")))

  private[fetcher] val aTimeout = PositiveFiniteDuration.tryOfSeconds(1)

  private[fetcher] val aMaxTimeReadingsAge = Some(NonNegativeFiniteDuration.tryOfDays(1))

  private[fetcher] val aSequencingTimeInfo =
    SequencingTimeInfo(
      validTimeInterval = None,
      forTrustThreshold = PositiveInt.tryCreate(2),
      basedOnTimeReadings = Map.empty,
    )
}
