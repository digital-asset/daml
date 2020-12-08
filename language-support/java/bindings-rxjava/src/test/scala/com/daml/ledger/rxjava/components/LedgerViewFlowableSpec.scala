// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components

import java.util.concurrent.TimeUnit

import com.daml.ledger.javaapi.data.{Identifier, LedgerOffset, WorkflowEvent}
import com.daml.ledger.rxjava.components.helpers.{CommandsAndPendingSet, CreatedContract}
import io.reactivex.Flowable
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class LedgerViewFlowableSpec extends AnyFlatSpec with Matchers {

  behavior of "LedgerViewFlowable.of"

  it should "not emit the initial message if the ACS is empty" in {
    val ledgerViewFlowable = LedgerViewFlowable.of(
      LedgerViewFlowable.LedgerView.create[Unit](),
      Flowable.never[LedgerViewFlowable.SubmissionFailure](),
      Flowable.never[LedgerViewFlowable.CompletionFailure](),
      Flowable.never[WorkflowEvent](),
      Flowable.never[CommandsAndPendingSet](),
      _ => ()
    )

    intercept[RuntimeException] {
      // NOTE(mp): in theory this test is not perfect because the stream
      // could emit something after 10ms. Eventually the test would reveal
      // the emitted element with a proper failure.
      // The test should also never be result in a false negative, which
      // means that even is the element is emitted after the timeout, the
      // test would still be green and cause no red masters.
      ledgerViewFlowable
        .timeout(10, TimeUnit.MILLISECONDS)
        .blockingFirst()
    }
  }

  it should "emit the initial message if the ACS is not empty" in {
    val identifier = new Identifier("packageId", "moduleName", "entityName")
    val initialLedgerView =
      LedgerViewFlowable.LedgerView.create[Unit]().addActiveContract(identifier, "contractId", ())
    val ledgerViewFlowable = LedgerViewFlowable.of(
      initialLedgerView,
      Flowable.never[LedgerViewFlowable.SubmissionFailure](),
      Flowable.never[LedgerViewFlowable.CompletionFailure](),
      Flowable.never[WorkflowEvent](),
      Flowable.never[CommandsAndPendingSet](),
      _ => ()
    )

    ledgerViewFlowable
      .timeout(1, TimeUnit.SECONDS)
      .blockingFirst() shouldBe initialLedgerView
  }

  it should "use ledger begin as offset if the active contracts service returns an empty stream" in {
    val pair = LedgerViewFlowable
      .ledgerViewAndOffsetFromACS(Flowable.empty(), identity[CreatedContract])
      .blockingGet()
    pair.getSecond() shouldBe LedgerOffset.LedgerBegin.getInstance()
  }

}
