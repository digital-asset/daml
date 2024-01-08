// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervals.ReconciliationInterval
import com.digitalasset.canton.protocol.DomainParameters
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalatest.EitherValues
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import java.time.{Duration, Instant}
import scala.language.implicitConversions
import scala.math.Ordering.Implicits.*

class SortedReconciliationIntervalsTest
    extends AnyWordSpec
    with BaseTest
    with SortedReconciliationIntervalsHelpers {

  private lazy val date = "2022-01-01"
  private def toTs(s: String): CantonTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse(s"${date}T${s}Z"))

  private def roundedTs(s: String): CantonTimestampSecond =
    CantonTimestampSecond.assertFromInstant(Instant.parse(s"${date}T${s}Z"))

  implicit def forgetRounding(roundedTs: CantonTimestampSecond): CantonTimestamp =
    roundedTs.forgetRefinement

  private lazy val validUntil = toTs("18:00:00")

  "SortedReconciliationIntervals factory" should {
    "build from a list" in {
      val ts1 = toTs("12:00:00")
      val ts2 = toTs("13:00:20")
      val ts3 = toTs("14:00:20")

      val parameters = List(
        mkParameters(ts2, ts3, 2),
        mkParameters(ts3, 3),
        mkParameters(ts1, ts2, 1),
      )

      val builtSortedReconciliationIntervals =
        SortedReconciliationIntervals.create(parameters, validUntil).value

      builtSortedReconciliationIntervals.intervals shouldBe List(
        ReconciliationInterval(ts3, None, PositiveSeconds.tryOfSeconds(3)),
        ReconciliationInterval(ts2, Some(ts3), PositiveSeconds.tryOfSeconds(2)),
        ReconciliationInterval(ts1, Some(ts2), PositiveSeconds.tryOfSeconds(1)),
      )

      builtSortedReconciliationIntervals.validUntil shouldBe validUntil
    }

    "return an error if there is an overlap" in {
      val ts1 = toTs("12:00:00")
      val ts2 = toTs("13:00:20")
      val ts3 = toTs("14:00:20")

      SortedReconciliationIntervals
        .create(
          List(
            mkParameters(ts2, ts3, 2),
            mkParameters(ts1, 1),
          ),
          validUntil,
        )
        .left
        .value shouldBe a[String]

      SortedReconciliationIntervals
        .create(
          List(
            mkParameters(ts2, ts3, 2),
            mkParameters(ts1, ts2 + NonNegativeFiniteDuration.tryOfMillis(1), 1),
          ),
          validUntil,
        )
        .left
        .value shouldBe a[String]
    }
  }

  "SortedReconciliationIntervals" when {
    "checking whether a timestamp falls on a tick" should {
      "answer correctly" in {
        val ts1 = toTs("12:00:00.500")
        val ts2 = roundedTs("12:00:20")

        val intervals = SortedReconciliationIntervals
          .create(
            List(mkParameters(ts1, ts2, 2), mkParameters(ts2, 3)),
            validUntil,
          )
          .value

        intervals.isAtTick(toTs("12:00:00")) shouldBe Some(
          false
        ) // Before domain parameters are set
        intervals.isAtTick(toTs("12:00:01")) shouldBe Some(false) // First tick is at 12:00:02
        intervals.isAtTick(toTs("12:00:02")) shouldBe Some(true)
        intervals.isAtTick(toTs("12:00:03")) shouldBe Some(false)

        intervals.isAtTick(toTs("12:00:20")) shouldBe Some(true) // Validity is (from, to]
        intervals.isAtTick(toTs("12:00:21")) shouldBe Some(true)

        intervals.isAtTick(validUntil.plusSeconds(1)) shouldBe None

        // Special case for CantonTimestamp.MinValue
        intervals.isAtTick(CantonTimestamp.MinValue) shouldBe Some(true)
        intervals.isAtTick(CantonTimestampSecond.MinValue) shouldBe Some(true)
      }

      "answer correctly after validity" in {
        val ts1 = toTs("12:00:00")

        val intervals =
          SortedReconciliationIntervals.create(List(mkParameters(ts1, 1)), validUntil).value

        intervals.isAtTick(validUntil) shouldBe Some(true)
        intervals.isAtTick(validUntil.plusSeconds(1)) shouldBe None
      }

      "answer correctly before epoch" in {
        val intervals = SortedReconciliationIntervals
          .create(
            List(mkParameters(CantonTimestamp.MinValue, 2)),
            validUntil,
          )
          .value

        intervals.isAtTick(
          CantonTimestamp.assertFromInstant(Instant.parse("1800-01-15T17:00:01Z"))
        ) shouldBe Some(false)
        intervals.isAtTick(
          CantonTimestamp.assertFromInstant(Instant.parse("1800-01-15T17:00:02Z"))
        ) shouldBe Some(true)
      }

      "answer false when the timestamp is in a gap" in {
        val ts1 = roundedTs("12:00:00")
        val ts2 = roundedTs("13:00:00")
        val ts3 = roundedTs("14:00:00")

        val intervals = SortedReconciliationIntervals
          .create(
            List(mkParameters(ts1, ts2, 1), mkParameters(ts3, 1)),
            validUntil,
          )
          .value

        intervals.isAtTick(ts1) shouldBe Some(false) // Validity is from exclusive
        intervals.isAtTick(toTs("12:00:01")) shouldBe Some(true)
        intervals.isAtTick(toTs("13:00:00")) shouldBe Some(true)
        intervals.isAtTick(toTs("13:00:01")) shouldBe Some(false) // in the gap
        intervals.isAtTick(toTs("14:00:01")) shouldBe Some(true)
      }
    }

    "computing the latest tick before or at a given timestamp" should {
      "answer correctly (simple case)" in {
        val ts1 = toTs("12:00:00.500")
        val ts2 = roundedTs("12:00:20")

        val intervals = SortedReconciliationIntervals
          .create(
            List(mkParameters(ts1, ts2, 2), mkParameters(ts2, 3)),
            validUntil,
          )
          .value

        intervals.tickBeforeOrAt(toTs("12:00:00")) shouldBe Some(CantonTimestampSecond.MinValue)
        intervals.tickBeforeOrAt(toTs("12:00:02")) shouldBe Some(roundedTs("12:00:02"))
        intervals.tickBeforeOrAt(toTs("12:00:02.500")) shouldBe Some(
          roundedTs("12:00:02")
        )

        intervals.tickBeforeOrAt(toTs("12:00:20")) shouldBe Some(
          roundedTs("12:00:20")
        ) // to is inclusive

        intervals.tickBeforeOrAt(toTs("12:00:21")) shouldBe Some(roundedTs("12:00:21"))
        intervals.tickBeforeOrAt(toTs("12:00:22")) shouldBe Some(roundedTs("12:00:21"))

        // At and after the validity
        intervals.tickBeforeOrAt(validUntil).isDefined shouldBe true
        intervals.tickBeforeOrAt(validUntil.plus(Duration.ofNanos(1000))).isDefined shouldBe false

        // Special case for CantonTimestamp.MinValue
        intervals.tickBeforeOrAt(CantonTimestamp.MinValue) shouldBe Some(
          CantonTimestampSecond.MinValue
        )
      }

      "answer correctly after validity" in {
        val ts1 = toTs("12:00:00")

        val intervals =
          SortedReconciliationIntervals.create(List(mkParameters(ts1, 1)), validUntil).value

        intervals.tickBeforeOrAt(validUntil).isDefined shouldBe true
        intervals.tickBeforeOrAt(validUntil.plusSeconds(1)) shouldBe None
      }

      "answer correctly before epoch" in {
        val intervals = SortedReconciliationIntervals
          .create(
            List(mkParameters(CantonTimestamp.MinValue, 2)),
            validUntil,
          )
          .value

        val even = CantonTimestampSecond.fromInstant(Instant.parse("1800-01-15T17:00:02Z")).value
        val odd = CantonTimestampSecond.fromInstant(Instant.parse("1800-01-15T17:00:03Z")).value

        intervals.tickBeforeOrAt(even) shouldBe Some(even)
        intervals.tickBeforeOrAt(odd) shouldBe Some(even)
      }

      "consider previous parameters if first tick of the interval is after the desired timestamp" in {
        val ts1 = roundedTs("12:00:00")
        val ts2 = roundedTs("12:00:04")

        val intervals = SortedReconciliationIntervals
          .create(
            List(mkParameters(ts1, ts2, 2), mkParameters(ts2, 3)),
            validUntil,
          )
          .value

        // First tick of second slice is at 06
        intervals.tickBeforeOrAt(toTs("12:00:05")) shouldBe Some(roundedTs("12:00:04"))
        intervals.tickBeforeOrAt(toTs("12:00:06")) shouldBe Some(roundedTs("12:00:06"))
      }

      "consider previous parameters if the interval contains no tick" in {
        val origin = CantonTimestampSecond.Epoch
        val ts1 = origin + PositiveSeconds.tryOfSeconds(1)

        val firstInterval = mkParameters(origin, ts1, 1)
        val noTickIntervals = (0L until 20L).map { i =>
          mkParameters(
            ts1 + NonNegativeFiniteDuration.tryOfSeconds(i),
            ts1 + NonNegativeFiniteDuration.tryOfSeconds(i + 1),
            3600, // much bigger than the bounds
          )
        }

        val intervals =
          SortedReconciliationIntervals.create(firstInterval +: noTickIntervals, validUntil).value

        intervals.tickBeforeOrAt(origin + NonNegativeFiniteDuration.tryOfSeconds(20)) shouldBe Some(
          ts1
        )
      }

      "consider previous parameters if there is a gap" in {
        val ts1 = roundedTs("12:00:00")
        val ts2 = roundedTs("13:00:00")

        val intervals =
          SortedReconciliationIntervals.create(List(mkParameters(ts1, ts2, 7)), validUntil).value

        // query after the last validity
        intervals.tickBeforeOrAt(toTs("13:30:00")) shouldBe Some(roundedTs("12:59:57"))
      }
    }

    "computing the latest tick before a given timestamp" should {
      "answer correctly (simple case)" in {
        val ts1 = toTs("12:00:00.500")
        val ts2 = roundedTs("12:00:20")

        val intervals = SortedReconciliationIntervals
          .create(
            List(mkParameters(ts1, ts2, 5), mkParameters(ts2, 4)),
            validUntil,
          )
          .value

        intervals.tickBefore(toTs("12:00:00")) shouldBe Some(CantonTimestampSecond.MinValue)
        intervals.tickBefore(toTs("12:00:05.500")) shouldBe Some(roundedTs("12:00:05"))

        intervals.tickBefore(toTs("12:00:24")) shouldBe Some(roundedTs("12:00:20"))
        intervals.tickBefore(toTs("12:00:25")) shouldBe Some(roundedTs("12:00:24"))
        intervals.tickBefore(toTs("12:00:28")) shouldBe Some(roundedTs("12:00:24"))
      }

      "answer correctly after validity" in {
        val ts1 = toTs("12:00:00")

        val intervals =
          SortedReconciliationIntervals.create(List(mkParameters(ts1, 1)), validUntil).value

        intervals.tickBefore(validUntil).isDefined shouldBe true
        intervals.tickBefore(validUntil.plusSeconds(1)) shouldBe None
      }

      "consider previous parameters if first tick of the interval is after the desired timestamp" in {
        val ts1 = roundedTs("12:00:00")
        val ts2 = roundedTs("12:00:04")

        val intervals = SortedReconciliationIntervals
          .create(
            List(mkParameters(ts1, ts2, 2), mkParameters(ts2, 3)),
            validUntil,
          )
          .value

        // First tick of second slice is at 06
        intervals.tickBefore(toTs("12:00:05")) shouldBe Some(roundedTs("12:00:04"))
        intervals.tickBefore(toTs("12:00:06")) shouldBe Some(roundedTs("12:00:04"))
        intervals.tickBefore(toTs("12:00:09")) shouldBe Some(roundedTs("12:00:06"))
      }

      "consider previous parameters if the interval contains no tick" in {
        val origin = CantonTimestampSecond.Epoch
        val ts1 = origin + PositiveSeconds.tryOfSeconds(1)

        val firstInterval = mkParameters(origin, ts1, 1)
        val noTickIntervals = (0L until 20L).map { i =>
          mkParameters(
            ts1 + NonNegativeFiniteDuration.tryOfSeconds(i),
            ts1 + NonNegativeFiniteDuration.tryOfSeconds(i + 1),
            3600, // much bigger than the bounds
          )
        }

        val intervals =
          SortedReconciliationIntervals.create(firstInterval +: noTickIntervals, validUntil).value

        intervals.tickBefore(origin + NonNegativeFiniteDuration.tryOfSeconds(20)) shouldBe Some(
          ts1
        )
      }

      "consider previous parameters if there is a gap" in {
        val ts1 = roundedTs("12:00:00")
        val ts2 = roundedTs("13:00:00")

        val intervals =
          SortedReconciliationIntervals.create(List(mkParameters(ts1, ts2, 7)), validUntil).value

        // query after the last validity
        intervals.tickBefore(toTs("13:30:00")) shouldBe Some(roundedTs("12:59:57"))
      }
    }
  }

  "SortedReconciliationIntervals.commitmentPeriodPreceding" must {
    import SortedReconciliationIntervalsTestHelpers.*

    "compute reasonable commitment periods for a basic example" in {
      val eventTimestamps = List(-2, 6, 24, 25, 26, 31L).map(CantonTimestamp.ofEpochSecond)
      val expectedPeriods = List(
        (CantonTimestamp.MinValue.getEpochSecond, -5L),
        (-5L, 5L),
        (5L, 20L),
        (20L, 25L),
        (25L, 30L),
      ).map(mkCommitmentPeriod)

      timeProofPeriodFlow(
        mkParameters(CantonTimestamp.MinValue, 5),
        eventTimestamps,
      ) shouldBe expectedPeriods
    }

    "work for a commitment period that starts at MinValue and is smaller than the reconciliation interval" in {
      val longIntervalSeconds = 5 * scala.math.pow(10, 10L).toLong
      val eventTimestamps = List(-5L, 2L).map(CantonTimestamp.ofEpochSecond)
      val expectedPeriods = List(
        (CantonTimestamp.MinValue.getEpochSecond, -longIntervalSeconds),
        (-longIntervalSeconds, 0L),
      ).map(mkCommitmentPeriod)

      timeProofPeriodFlow(
        mkParameters(CantonTimestamp.MinValue, longIntervalSeconds),
        eventTimestamps,
      ) shouldBe expectedPeriods
    }

    "work when MinValue.getEpochSeconds isn't a multiple of the reconciliation interval" in {
      val longInterval = PositiveSeconds.tryOfDays(100)
      val longIntervalSeconds = longInterval.unwrap.getSeconds
      assert(
        CantonTimestamp.MinValue.getEpochSecond % longIntervalSeconds != 0,
        "Precondition for the test to make sense",
      )
      val eventTimestamps =
        List(2L, 5L, longIntervalSeconds + 2L).map(CantonTimestamp.ofEpochSecond)
      val expectedPeriods =
        List((CantonTimestamp.MinValue.getEpochSecond, 0L), (0L, longIntervalSeconds))
          .map(mkCommitmentPeriod)

      timeProofPeriodFlow(
        mkParameters(CantonTimestamp.MinValue, longIntervalSeconds),
        eventTimestamps,
      ) shouldBe expectedPeriods
    }
  }
}

class SortedReconciliationIntervalsPropertyTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckDrivenPropertyChecks
    with SortedReconciliationIntervalsHelpers {

  import SortedReconciliationIntervalsTestHelpers.*

  "SortedReconciliationIntervals" must {
    val interval = PositiveSeconds.tryOfSeconds(5)

    "compute tick periods with no gaps" in {
      implicit lazy val incrementArb: Arbitrary[Seq[(Long, Long)]] =
        Arbitrary(for {
          len <- Gen.choose(0, 10)
          seq1 <- Gen.containerOfN[List, Long](len, Gen.choose(1L, 20L))
          seq2 <- Gen.containerOfN[List, Long](len, Gen.choose(0L, 999999L))
        } yield seq1.zip(seq2))

      implicit lazy val arbInstant: Arbitrary[CantonTimestamp] = Arbitrary(for {
        delta <- Gen.choose(0L, 20L)
      } yield CantonTimestamp.ofEpochSecond(delta))

      // Prevent scalacheck from shrinking, as it doesn't use the above generators and thus doesn't respect the required invariants
      import Shrink.shrinkAny

      forAll {
        (
            startTs: CantonTimestamp,
            increments: Seq[(Long, Long)],
        ) =>
          val incrementsToTimes =
            (increments: Seq[(Long, Long)]) =>
              increments.scanLeft(startTs) { case (ts, (diffS, diffMicros)) =>
                ts.plusSeconds(diffS).addMicros(diffMicros)
              }

          val times = incrementsToTimes(increments)
          val periods = timeProofPeriodFlow(
            mkParameters(CantonTimestamp.MinValue, interval.unwrap.getSeconds),
            times,
          )

          periods.foreach { period =>
            assert(
              Duration.between(
                period.fromExclusive.toInstant,
                period.toInclusive.toInstant,
              ) >= interval.unwrap,
              "Commitment periods must be longer than the specified interval",
            )
            assert(period.toInclusive.microsOverSecond() == 0, "period must end at whole seconds")
            assert(
              period.fromExclusive.microsOverSecond() == 0,
              "period must start at whole seconds",
            )
            assert(
              period.toInclusive.getEpochSecond % interval.unwrap.getSeconds == 0,
              "period must end at commitment ticks",
            )
            assert(
              period.fromExclusive.getEpochSecond % interval.unwrap.getSeconds == 0,
              "period must start at commitment ticks",
            )
          }

          val gaps = periods.lazyZip(periods.drop(1)).map { case (p1, p2) =>
            Duration.between(p1.toInclusive.toInstant, p2.fromExclusive.toInstant)
          }

          gaps.foreach { g =>
            assert(g === Duration.ZERO, s"All gaps must be zero; times: $times; periods: $periods")
          }
      }
    }
  }
}

private[pruning] object SortedReconciliationIntervalsTestHelpers extends EitherValues {
  // Just empty commit sets (i.e., time proofs)
  def timeProofPeriodFlow(
      dynamicDomainParameters: DomainParameters.WithValidity[PositiveSeconds],
      times: Seq[CantonTimestamp],
  ): Seq[CommitmentPeriod] = {
    val reconciliationIntervals = SortedReconciliationIntervals
      .create(Seq(dynamicDomainParameters), CantonTimestamp.MaxValue)
      .value

    times.foldLeft((None: Option[CantonTimestampSecond], Seq.empty[CommitmentPeriod])) {
      case ((lastTick, periods), ts) =>
        reconciliationIntervals.tickBefore(ts).flatMap { periodEnd =>
          reconciliationIntervals.commitmentPeriodPreceding(periodEnd, lastTick)
        } match {
          case Some(period) => (Some(period.toInclusive), periods :+ period)
          case None => (lastTick, periods)
        }
    } match {
      case (_lastTickNoLongerNeeded, commitmentPeriods) => commitmentPeriods
    }
  }

}
