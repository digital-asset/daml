// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.problems

import com.digitalasset.ledger.client.binding.encoding.DamlDates.{
  localDatesWithoutInjectiveFunctionToSqlDate,
  damlDatesWithoutInjectiveFunctionToSqlDate,
  RangeOfLocalDatesWithoutInjectiveFunctionToSqlDate
}
import com.digitalasset.ledger.client.binding.encoding.SqlTypeConverters.{
  primitiveDateToSqlDate,
  sqlDateToPrimitiveDate
}
import com.digitalasset.ledger.client.binding.{Primitive => P}
import org.scalatest.{FreeSpec, Inspectors, Matchers}

class JavaSqlDateKnownProblemSpec extends FreeSpec with Matchers {

  "java.time.Date to java.sql.Date isomorphism that we are using does not work for the range of java.time.Date values: " +
    RangeOfLocalDatesWithoutInjectiveFunctionToSqlDate.toString in {
    Inspectors.forEvery(localDatesWithoutInjectiveFunctionToSqlDate) {
      localDate0: java.time.LocalDate =>
        val sqlDate = java.sql.Date.valueOf(localDate0)
        val localDate1 = sqlDate.toLocalDate
        localDate1 should not be localDate0
        (localDate1.getDayOfMonth - localDate0.getDayOfMonth) shouldBe 10
    }
  }

  "DAML Date to java.sql.Date isomorphism that we are using does not work for the range of DAML Date values: " +
    RangeOfLocalDatesWithoutInjectiveFunctionToSqlDate.toString in {
    Inspectors.forEvery(damlDatesWithoutInjectiveFunctionToSqlDate) { pDate0: P.Date =>
      val sqlDate: java.sql.Date = primitiveDateToSqlDate(pDate0)
      val pDate1: P.Date = sqlDateToPrimitiveDate(sqlDate)
      pDate1 should not be pDate0
      (pDate1.getDayOfMonth - pDate0.getDayOfMonth) shouldBe 10
    }
  }

  "Here is the reason for the above, two java.time.Date values mapped to the same java.sql.Date" in {
    Inspectors.forEvery(localDatesWithoutInjectiveFunctionToSqlDate) {
      localDate0: java.time.LocalDate =>
        val localDate1 = localDate0.plusDays(10)
        val sqlDate0 = java.sql.Date.valueOf(localDate0)
        val sqlDate1 = java.sql.Date.valueOf(localDate1)

        localDate0 should not be localDate1
        sqlDate0 shouldBe sqlDate1
    }
  }
}
