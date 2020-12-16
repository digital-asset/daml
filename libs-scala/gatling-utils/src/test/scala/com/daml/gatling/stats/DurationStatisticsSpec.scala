// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.gatling.stats

import com.daml.scalatest.FlatSpecCheckLaws
import org.scalacheck.{Arbitrary, Gen}
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.Equal
import scalaz.scalacheck.ScalazProperties

class DurationStatisticsSpec
    extends AnyFlatSpec
    with FlatSpecCheckLaws
    with Matchers
    with TypeCheckedTripleEquals {

  import SimulationLog.DurationStatistics
  import DurationStatisticsSpec._

  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000)

  behavior of s"${classOf[DurationStatistics].getSimpleName} Monoid"

  checkLaws(ScalazProperties.monoid.laws[DurationStatistics])
}

private object DurationStatisticsSpec {
  import SimulationLog.DurationStatistics

  val durationStatisticsGen: Gen[DurationStatistics] = for {
    durations <- Gen.listOf(Gen.posNum[Int])
    start <- Gen.option(Gen.posNum[Long])
    end <- Gen.option(Gen.posNum[Long])
  } yield DurationStatistics(durations, start, end)

  implicit val durationStatisticsArb: Arbitrary[DurationStatistics] =
    Arbitrary(durationStatisticsGen)

  implicit val durationStatisticsEqual: Equal[DurationStatistics] = Equal.equalA
}
