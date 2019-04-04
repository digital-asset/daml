// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import java.time.LocalDate

import com.digitalasset.ledger.client.binding.encoding.DamlDates._
import com.digitalasset.ledger.client.binding.{Primitive => P}
import org.scalacheck.Gen

object DamlDateGen {

  private def genSqlCompatibleLocalDate: Gen[LocalDate] = {
    // skip the range
    val upTo: Long = RangeOfLocalDatesWithoutInjectiveFunctionToSqlDate._1.toEpochDay - 1
    val upFrom: Long = RangeOfLocalDatesWithoutInjectiveFunctionToSqlDate._2.toEpochDay + 1
    Gen
      .oneOf(
        Gen.choose(Min.toEpochDay, upTo),
        Gen.choose(upFrom, Max.toEpochDay)
      )
      .map(LocalDate.ofEpochDay)
  }

  def genDamlDate: Gen[P.Date] = P.Date.subst(genSqlCompatibleLocalDate)
}
