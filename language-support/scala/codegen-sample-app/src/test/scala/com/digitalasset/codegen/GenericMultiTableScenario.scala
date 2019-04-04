// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import com.digitalasset.ledger.client.binding.encoding.MultiTableTests
import com.digitalasset.ledger.client.binding.{Primitive => P}
import com.digitalasset.slick.SlickConnection
import org.scalatest.time.{Millis, Seconds, Span}
import slick.jdbc.JdbcProfile

/**
  * Generic test scenarios that can run against any DB supported by Slick.
  */
abstract class GenericMultiTableScenario[Profile <: JdbcProfile](val con: SlickConnection[Profile])
    extends MultiTableTests {

  private val rowsPerIteration = 1000

  // Test cases derived from the codegened `EventDecoder.templateTypes`
  // plus an example of how to manually specify additional test cases.
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  val testCases = {
    import DerivedTemplateTableTest.{fromType => fty}
    import com.digitalasset.sample.{MyMain => M}
    com.digitalasset.sample.EventDecoder.templateTypes.map { tc =>
      fty(tc.`the template LfEncodable`)
    } ++ Seq(fty[M.OptionPrice], fty[M.PolyRec[P.Int64, P.Text, P.Date]])
  }

  // must use val, not def or lazy val
  val schemata =
    derivedTemplateTableTests(con.profile)(con.db, sampleSize = rowsPerIteration)(testCases: _*)

  implicit override lazy val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = timeout(rowsPerIteration), interval = Span(50, Millis))

  private def timeout(num: Int): Span =
    if (num <= 10) Span(5, Seconds)
    else if (num <= 100) Span(10, Seconds)
    else if (num <= 1000) Span(20, Seconds)
    else Span(40, Seconds)

  override protected def beforeAll(): Unit = beforeAllFromSchemata(con.profile)(con.db)(schemata)

  override protected def afterAll(): Unit = {
    afterAllFromSchemata(con.profile)(con.db)(schemata)
    con.close()
  }
}
