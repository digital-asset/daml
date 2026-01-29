// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.time.fetcher

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.sequencing.client.time.fetcher.OneCallAtATimeSourcesAccessor.QueryTimeSourcesRunningTask
import com.digitalasset.canton.sequencing.client.time.fetcher.SequencingTimeFetcherTest.{
  TestTimeSource,
  TestTimeSourcesPool,
  aTimeout,
  sequencerIds,
  ts,
}
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicReference

class OneCallAtATimeSourcesAccessorTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext {

  "Querying time sources" when {

    "no other concurrent call is running, regardless of 'concurrent' being true" should {
      "invoke the time sources with the passed timeout and return their results' count" in {
        val sequencingTimeFutures =
          Vector(
            Seq.fill(2)(FutureUnlessShutdown.pure(Some(ts(0)))),
            Seq.fill(2)(FutureUnlessShutdown.pure(None)),
          )
        val timeSources = sequencingTimeFutures.map(TestTimeSource(_*))
        val timeSourcesPool = new TestTimeSourcesPool(timeSources)
        val readings = mock[SequencingTimeReadings]
        val timeSourcesAccessor = newTimeSourcesAccessor(readings)

        def invokeSources(concurrent: Boolean) =
          timeSourcesAccessor
            .queryTimeSources(
              timeSourcesPool.timeSources(PositiveInt.tryCreate(2), exclusions = Set.empty).toMap,
              aTimeout,
              concurrent,
            )

        for {
          r1 <- invokeSources(true)
          r2 <- invokeSources(false)
        } yield {
          verify(readings, times(2)).recordReading(
            eqTo(sequencerIds(0)),
            eqTo(Some(ts(0))),
            any[CantonTimestamp],
          )
          verify(readings, times(2)).recordReading(
            eqTo(sequencerIds(1)),
            eqTo(None),
            any[CantonTimestamp],
          )
          r1 shouldBe Map(sequencerIds(0) -> Some(ts(0)), sequencerIds(1) -> None)
          r2 shouldBe Map(sequencerIds(0) -> Some(ts(0)), sequencerIds(1) -> None)
          timeSources.map(_.invocationsCountRef.get()) should contain only 2
          timeSources.flatMap(_.timeoutsRef.get()) should contain theSameElementsAs
            Seq.fill(4)(aTimeout)
        }
      }.failOnShutdown
    }

    "another call is running and 'concurrent' is true" should {
      "run the invocations concurrently" in {
        val sequencingTimePromises =
          Vector.fill(4)(
            PromiseUnlessShutdown.unsupervised[Option[CantonTimestamp]]()
          )
        val timeSources = sequencingTimePromises.map(p => TestTimeSource(p.futureUS))
        val timeSourcesPool = new TestTimeSourcesPool(timeSources)
        val readings = mock[SequencingTimeReadings]
        val runningTaskRef = new AtomicReference(Option.empty[QueryTimeSourcesRunningTask])
        val timeSourcesAccessor =
          newTimeSourcesAccessor(readings, runningTaskRef = runningTaskRef)

        val f1 =
          timeSourcesAccessor
            .queryTimeSources(
              timeSourcesPool.timeSources(PositiveInt.tryCreate(2), exclusions = Set.empty).toMap,
              aTimeout,
            )
        eventually() {
          runningTaskRef.get().isDefined shouldBe true
        }
        val f2 =
          timeSourcesAccessor
            .queryTimeSources(
              timeSourcesPool
                .timeSources(
                  PositiveInt.tryCreate(2),
                  exclusions = sequencerIds.slice(0, 2).toSet,
                )
                .toMap,
              aTimeout,
              concurrent = true,
            )

        sequencingTimePromises(2).outcome_(Some(ts(0)))
        sequencingTimePromises(3).outcome_(Some(ts(0)))
        for {
          r2 <- f2
          _ = sequencingTimePromises(0).outcome_(Some(ts(0)))
          _ = sequencingTimePromises(1).outcome_(Some(ts(0)))
          r1 <- f1
        } yield {
          for (i <- 0 to 3)
            verify(readings, times(1)).recordReading(
              eqTo(sequencerIds(i)),
              eqTo(Some(ts(0))),
              any[CantonTimestamp],
            )
          r1 shouldBe Map(sequencerIds(0) -> Some(ts(0)), sequencerIds(1) -> Some(ts(0)))
          r2 shouldBe Map(sequencerIds(2) -> Some(ts(0)), sequencerIds(3) -> Some(ts(0)))
          timeSources.map(_.invocationsCountRef.get()) should contain only 1
          timeSources.flatMap(_.timeoutsRef.get()) should contain theSameElementsAs
            Seq.fill(4)(aTimeout)
        }
      }.failOnShutdown
    }

    "another call is running and 'concurrent' is false" when {
      "the timeout does not expire" should {
        "call every source only once and return the aggregated result" in {
          val sequencingTimePromises =
            Vector.fill(3)(
              PromiseUnlessShutdown.unsupervised[Option[CantonTimestamp]]()
            )
          val timeSources = sequencingTimePromises.map(p => TestTimeSource(p.futureUS))
          val timeSourcesPool = new TestTimeSourcesPool(timeSources)
          val readings = mock[SequencingTimeReadings]
          val runningTaskRef = new AtomicReference(Option.empty[QueryTimeSourcesRunningTask])
          val timeSourcesAccessor =
            newTimeSourcesAccessor(readings, runningTaskRef = runningTaskRef)

          val f1 =
            timeSourcesAccessor
              .queryTimeSources(
                timeSourcesPool.timeSources(PositiveInt.tryCreate(2), exclusions = Set.empty).toMap,
                aTimeout,
              )
          eventually() {
            runningTaskRef.get().isDefined shouldBe true
          }
          val f2 =
            timeSourcesAccessor
              .queryTimeSources(
                timeSourcesPool
                  .timeSources(
                    PositiveInt.tryCreate(2),
                    exclusions = Set(sequencerIds(0)), // Overlap == sequencerIds(1)
                  )
                  .toMap,
                aTimeout,
              )

          sequencingTimePromises(0).outcome_(Some(ts(0)))
          sequencingTimePromises(1).outcome_(Some(ts(0)))
          for {
            r1 <- f1
            _ = sequencingTimePromises(2).outcome_(Some(ts(0)))
            r2 <- f2
          } yield {
            for (i <- 0 to 2)
              verify(readings, times(1)).recordReading(
                eqTo(sequencerIds(i)),
                eqTo(Some(ts(0))),
                any[CantonTimestamp],
              )
            r1 shouldBe Map(sequencerIds(0) -> Some(ts(0)), sequencerIds(1) -> Some(ts(0)))
            r2 shouldBe Map(
              sequencerIds(0) -> Some(ts(0)),
              sequencerIds(1) -> Some(ts(0)),
              sequencerIds(2) -> Some(ts(0)),
            )
            timeSources.map(_.invocationsCountRef.get()) should contain only 1
            timeSources.flatMap(_.timeoutsRef.get()) should contain theSameElementsAs
              Seq.fill(3)(aTimeout)
          }
        }.failOnShutdown
      }

      "the timeout expires" should {
        "call every source only once and return the new result only" in {
          val sequencingTimePromises =
            Vector.fill(3)(
              PromiseUnlessShutdown.unsupervised[Option[CantonTimestamp]]()
            )
          val timeSources = sequencingTimePromises.map(p => TestTimeSource(p.futureUS))
          val timeSourcesPool = new TestTimeSourcesPool(timeSources)
          val simClock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
          val readings = mock[SequencingTimeReadings]
          val runningTaskRef = new AtomicReference(Option.empty[QueryTimeSourcesRunningTask])
          val timeSourcesAccessor = newTimeSourcesAccessor(readings, simClock, runningTaskRef)

          val f1 =
            timeSourcesAccessor
              .queryTimeSources(
                timeSourcesPool.timeSources(PositiveInt.tryCreate(2), exclusions = Set.empty).toMap,
                aTimeout,
              )
          eventually() {
            runningTaskRef.get().isDefined shouldBe true
          }
          val f2 =
            timeSourcesAccessor
              .queryTimeSources(
                timeSourcesPool
                  .timeSources(
                    PositiveInt.tryCreate(2),
                    exclusions = Set(sequencerIds(0)), // Overlap == sequencerIds(1)
                  )
                  .toMap,
                aTimeout,
              )

          sequencingTimePromises(1).outcome_(Some(ts(0)))
          sequencingTimePromises(2).outcome_(Some(ts(0)))
          simClock.advance(aTimeout.duration)
          for {
            result <- f2
            // Time source 0 times out as requested, unblocking f1, which we await
            //  to ensure that time source 1 has registered its result
            _ = sequencingTimePromises(0).outcome_(None)
            _ <- f1
          } yield {
            for (i <- 1 to 2)
              verify(readings, times(1)).recordReading(
                eqTo(sequencerIds(i)),
                eqTo(Some(ts(0))),
                any[CantonTimestamp],
              )
            result shouldBe Map(sequencerIds(2) -> Some(ts(0)))
            timeSources.map(_.invocationsCountRef.get()) should contain only 1
            timeSources.flatMap(_.timeoutsRef.get()) should contain theSameElementsAs
              Seq.fill(3)(aTimeout)
          }
        }.failOnShutdown
      }
    }
  }

  private def newTimeSourcesAccessor(
      readings: SequencingTimeReadings,
      localClock: Clock = wallClock,
      runningTaskRef: AtomicReference[Option[QueryTimeSourcesRunningTask]] = new AtomicReference(
        Option.empty[QueryTimeSourcesRunningTask]
      ),
  ) =
    new OneCallAtATimeSourcesAccessor(localClock, readings, loggerFactory, runningTaskRef)
}
