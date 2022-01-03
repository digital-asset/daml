// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.encoding

import java.time.LocalDate

import com.daml.ledger.client.binding.encoding.DamlDates._
import com.daml.ledger.client.binding.{Primitive => P}
import org.scalacheck.Gen

object DamlDateGen {

  private def genSqlCompatibleLocalDate: Gen[LocalDate] = {
    // skip the range
    val upTo: Long = RangeOfLocalDatesWithoutInjectiveFunctionToSqlDate._1.toEpochDay - 1
    val upFrom: Long = RangeOfLocalDatesWithoutInjectiveFunctionToSqlDate._2.toEpochDay + 1
    Gen
      .oneOf(
        Gen.choose(Min.toEpochDay, upTo),
        Gen.choose(upFrom, Max.toEpochDay),
      )
      .map(LocalDate.ofEpochDay)
  }

  def genDamlDate: Gen[P.Date] = P.Date.subst(genSqlCompatibleLocalDate)
}
