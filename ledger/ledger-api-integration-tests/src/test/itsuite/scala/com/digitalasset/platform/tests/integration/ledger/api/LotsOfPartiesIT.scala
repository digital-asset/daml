// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture, TestTemplateIds}
import com.digitalasset.platform.participant.util.ValueConversions._
import com.digitalasset.util.Ctx
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.{Assertion, AsyncWordSpec, Matchers}

import scala.collection.breakOut
import scala.concurrent.Future

class LotsOfPartiesIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with AsyncTimeLimitedTests
    with Matchers {

  protected val testTemplateIds = new TestTemplateIds(config)
  protected val templateIds = testTemplateIds.templateIds

  private val numParties = 1024
  private val allParties: List[String] =
    Range.inclusive(1, numParties).map(i => s"party$i")(breakOut)
  private val submittingParty :: observerParties = allParties
  override protected def config: Config =
    Config.default.withParties(submittingParty, observerParties: _*)
  private val commandId = this.getClass.getSimpleName

  "lots of registered parties" when {
    "submitting commands" should {
      "submit successfully and track the completion" in allFixtures { c =>
        val giver = "giver" -> submittingParty.asParty
        val observers = "observers" -> observerParties.map(_.asParty).asList
        val cmd =
          c.createCommand(
            commandId,
            templateIds.withObservers,
            List(giver, observers).asRecordFields,
            submittingParty)
        for {
          cc <- c.commandClient()
          tracker <- cc.trackCommands[Unit](List(submittingParty))
          result <- Source.single(Ctx.unit(cmd)).via(tracker).runWith(Sink.head)
        } yield {
          result.value.status.value should have('code (0))
        }
      }
    }
    "reading transactions" should {
      "see the transaction in a single multi-party subscription" in allFixtures { c =>
        for {
          tx <- getTx(c, allParties)
        } yield {
          assertExpectedWitnesses(createdEventWitnesses(tx))
        }
      }
      "see the transaction in multiple single-party subscriptions" in allFixtures { c =>
        for {
          txs <- Future.sequence(allParties.map(p => getTx(c, List(p))))
        } yield {
          assertExpectedWitnesses(txs.flatMap(tx => createdEventWitnesses(tx)))
        }
      }
    }

    "reading ACS" should {
      "see the contract in a single multi-party subscription" in allFixtures { c =>
        for {
          responses <- c.acsClient
            .getActiveContracts(filterForParties(allParties))
            .runWith(Sink.seq)
        } yield {
          val contracts = responses.flatMap(_.activeContracts)
          contracts should have length 1
          assertExpectedWitnesses(contracts.headOption.value.witnessParties)
        }
      }
      "see the contract in multiple single-party subscriptions" in allFixtures { c =>
        for {
          responses <- Future.sequence(allParties.map(p =>
            c.acsClient.getActiveContracts(filterForParties(List(p))).runWith(Sink.seq)))
        } yield {
          val contracts = responses.flatten.flatMap(_.activeContracts)
          assertExpectedWitnesses(contracts.flatMap(_.witnessParties))

        }
      }
    }
  }
  private def assertExpectedWitnesses(witnesses: Seq[String]): Assertion = {
    witnesses should have length numParties.toLong
    witnesses should contain theSameElementsAs allParties
  }

  private def createdEventWitnesses(tx: Transaction): Seq[String] = {
    tx.events should have length 1
    tx.events.headOption.value.event.created.value.witnessParties
  }

  private def getTx(c: LedgerContext, parties: List[String]): Future[Transaction] = {
    c.transactionClient
      .getTransactions(
        LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)),
        None,
        filterForParties(parties))
      .runWith(Sink.head)
  }

  private def filterForParties(parties: List[String]) = {
    TransactionFilter(parties.map(_ -> Filters())(breakOut))
  }
}
