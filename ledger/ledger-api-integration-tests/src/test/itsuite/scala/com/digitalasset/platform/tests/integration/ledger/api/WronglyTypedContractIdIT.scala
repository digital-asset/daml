// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import org.scalatest.{AsyncFreeSpec, Matchers}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundEach
}
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture, TestTemplateIds}
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.platform.participant.util.ValueConversions._
import com.google.rpc.code.Code

class WronglyTypedContractIdIT
    extends AsyncFreeSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundEach
    with ScalaFutures
    with AsyncTimeLimitedTests
    with Matchers
    with TestTemplateIds {
  override protected def config: Config = Config.default

  def createDummy(ctx: LedgerContext) = ctx.testingHelpers.simpleCreate(
    "create-dummy",
    "alice",
    templateIds.dummy,
    Record(fields = List(RecordField(value = "alice".asParty)))
  )

  "exercising something of the wrong type fails" in allFixtures { ctx =>
    for {
      ce <- createDummy(ctx)
      _ <- ctx.testingHelpers.failingExercise(
        "exercise-wrong",
        "alice",
        templateIds.dummyWithParam,
        ce.contractId,
        "DummyChoice2",
        Value(Value.Sum.Record(Record(fields = List(RecordField(value = "txt".asText))))),
        Code.INVALID_ARGUMENT,
        "wrongly typed contract id"
      )
    } yield succeed
  }

  "fetching something of the wrong type fails" in allFixtures { ctx =>
    for {
      dummyCreateEvt <- createDummy(ctx)
      delegationCreateEvt <- ctx.testingHelpers.simpleCreate(
        "create-delegation",
        "alice",
        templateIds.delegation,
        Record(
          fields = List(RecordField(value = "alice".asParty), RecordField(value = "bob".asParty)))
      )
      _ <- ctx.testingHelpers.failingExercise(
        "fetch-wrong",
        "alice",
        templateIds.delegation,
        delegationCreateEvt.contractId,
        "FetchDelegated",
        Value(
          Value.Sum.Record(
            Record(fields = List(RecordField(value = dummyCreateEvt.contractId.asContractId))))),
        Code.INVALID_ARGUMENT,
        "wrongly typed contract id"
      )
    } yield succeed
  }
}
