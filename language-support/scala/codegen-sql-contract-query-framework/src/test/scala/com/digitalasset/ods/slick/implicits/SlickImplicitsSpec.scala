// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ods.slick.implicits

import java.time.LocalDate
import java.util.concurrent.TimeUnit

import com.digitalasset.ods.slick.{H2TestUtils, SlickH2Profile}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import com.digitalasset.ledger.client.binding.Primitive.ContractId
import com.digitalasset.ledger.api.refinements.ApiTypes.{Party, WorkflowId}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Try

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class SlickImplicitsSpec
    extends FlatSpec
    with SlickH2Profile
    with Matchers
    with SlickImplicits
    with PlatformSlickImplicits
    with GeneratorDrivenPropertyChecks
    with CustomArbitraries
    with Wrappers
    with LazyLogging
    with H2TestUtils {
  import profile.api._

  implicit override val generatorDrivenConfig =
    PropertyCheckConfiguration(minSuccessful = 5)

  // TODO: For some reason now BigDecimal and Boolean wrappers cannot find implicits needed. FIX
  //  val bigDecimalTableWrapper = wrapper[BigDecimal]("bigdecimal")
  val bigIntTableWrapper = wrapper[BigInt]("bigint")
  //val instantTableWrapper   = wrapper[Instant]("instant") PROD-9945
  val localDateTableWrapper = wrapper[LocalDate]("local_date")
  //  val booleanTableWrapper       = wrapper[Boolean]("boolean")
  val javaDurationWrapper = wrapper[java.time.Duration]("jduration")
  val durationWrapper = wrapper[Duration]("duration")
  val contractIdWrapper = wrapper[ContractId[String]]("contract_id")
  val partyWrapper = wrapper[Party]("party")

  val workflowIdWrapper = wrapper[WorkflowId]("ledger_process_id")

  def createTableIfDoesntExist(wrapper: Wrapper[_]): Unit = {
    val _ = Try(Await.result(db.run(wrapper.query.schema.create), Duration(10, TimeUnit.MINUTES)))
  }

  def testWrapper(wrapper: Wrapper[_]): Unit = {
    it should s"support ${wrapper.columnName}" in forAll(wrapper.rowClassArbitrary.arbitrary) {
      (row: wrapper.RowClass) =>
        logger.info(s"Testing ${wrapper.columnName} value ${row.t}")

        Await.result(db.run(wrapper.query.delete), Duration(10, TimeUnit.MINUTES))

        val query =
          for {
            _ <- wrapper.query += row
            result <- wrapper.query.result
          } yield result

        val result = Await.result(db.run(query), Duration(10, TimeUnit.MINUTES))
        result should contain theSameElementsAs Seq(row)
    }
  }

  behavior of "slick"

  wrappers foreach createTableIfDoesntExist
  wrappers foreach testWrapper
}
