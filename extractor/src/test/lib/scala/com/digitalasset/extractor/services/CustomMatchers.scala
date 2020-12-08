// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.services

import java.sql.Timestamp

import org.scalatest._
import org.scalatest.matchers.{Matcher, MatchResult}
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

trait CustomMatchers {
  class SqlTimestampMoreOrLessEquals(expected: Timestamp, tolerance: Duration)
      extends Matcher[Timestamp]
      with Matchers {
    def apply(left: Timestamp) = {
      MatchResult(
        left.getTime === (expected.getTime +- tolerance.toMillis),
        s"""Timestamp $left was not within ${tolerance} to "$expected"""",
        s"""Timestamp $left was within ${tolerance} to "$expected"""",
      )
    }
  }

  def beWithin5Minutes(expected: Timestamp) = new SqlTimestampMoreOrLessEquals(expected, 5.minutes)

  def beWithin(duration: Duration)(expected: Timestamp) =
    new SqlTimestampMoreOrLessEquals(expected, duration)

  def beWithinSeconds(seconds: Long)(expected: Timestamp) =
    new SqlTimestampMoreOrLessEquals(expected, seconds.seconds)

  def beWithinMillis(millis: Long)(expected: Timestamp) =
    new SqlTimestampMoreOrLessEquals(expected, millis.millis)
}

object CustomMatchers extends CustomMatchers
