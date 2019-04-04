// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import java.sql.Timestamp
import java.time.Instant

import com.digitalasset.ledger.client.binding.encoding.DamlDates.{
  RangeOfLocalDatesWithoutInjectiveFunctionToSqlDate,
  damlDatesWithoutInjectiveFunctionToSqlDate
}
import com.digitalasset.ledger.client.binding.{Primitive => P}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Assertion, FreeSpec, Matchers, Inspectors}

class SqlTypeConvertersTest extends FreeSpec with Matchers with GeneratorDrivenPropertyChecks {
  import PrimitiveTypeGenerators._
  import SqlTypeConverters._

  implicit override val generatorDrivenConfig = PropertyCheckConfiguration(minSuccessful = 1000000)

  "P.Date to java.sql.Date conversion and back" in forAll(primitiveDateGen) { d0 =>
    val sqlDate = primitiveDateToSqlDate(d0)
    val d1 = sqlDateToPrimitiveDate(sqlDate)
    d1 shouldBe d0
  }

  "P.Date to java.sql.Date conversion does not work for the dates from the range: " +
    RangeOfLocalDatesWithoutInjectiveFunctionToSqlDate.toString in {
    Inspectors.forEvery(damlDatesWithoutInjectiveFunctionToSqlDate) { d0: P.Date =>
      val sqlDate = primitiveDateToSqlDate(d0)
      val d1 = sqlDateToPrimitiveDate(sqlDate)
      // crazy stuff
      d1 should not be d0
    }
  }

  "P.Timestamp to java.sql.Timestamp conversion and back" in forAll(primitiveTimestampGen) { t0 =>
    val sqlTimestamp = primitiveTimestampToSqlTimestamp(t0)
    val t1 = sqlTimestampToPrimitiveTimestamp(sqlTimestamp)
    t1 shouldBe t0
  }

  private val listOfJavaInstants: List[Instant] =
    List(
      "6813-11-03T06:41:04Z",
      "6813-11-03T05:41:04Z",
      "4226-11-05T06:07:48Z",
      "4226-11-05T05:07:48Z",
      "2529-11-06T05:57:36.498937Z",
      "1582-10-05T00:00:00Z",
      "1582-10-09T00:00:00Z",
      "1582-10-14T00:00:00Z"
    ).map(Instant.parse)

  "P.Timestamp to java.sql.Timestamp conversion and back for a few values that causing issues" in {
    listOfJavaInstants.foreach { instant: Instant =>
      val t0: P.Timestamp =
        P.Timestamp
          .discardNanos(instant)
          .getOrElse(fail("expected `P.Timestamp` friendly `Instant`"))
      (instant.getEpochSecond, instant.getNano) shouldBe ((t0.getEpochSecond, t0.getNano))
      testPrimitiveTimestampConversion(t0)
    }
  }

  private def testPrimitiveTimestampConversion(t0: P.Timestamp): Assertion = {
    val sqlTimestamp: Timestamp = primitiveTimestampToSqlTimestamp(t0)
    val t1: P.Timestamp = sqlTimestampToPrimitiveTimestamp(sqlTimestamp)
    t1 shouldBe t0
    (t1.getEpochSecond, t1.getNano) shouldBe ((t0.getEpochSecond, t0.getNano))
  }
}
