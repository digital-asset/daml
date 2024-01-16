// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.concurrent.TimeUnit
import com.daml.ledger.javaapi.data
import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.TransactionGenerator._
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.Value.Absolute
import com.daml.ledger.api.v1.transaction_filter.TemplateFilter
import com.daml.ledger.api.v1.value.Identifier
import io.reactivex.Observable
import org.scalacheck.Shrink
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

final class TransactionsClientImplTest
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers
    with AuthMatchers
    with DataLayerHelpers {

  override val ledgerServices = new LedgerServices("transaction-service-ledger")

  implicit def tupleNoShrink[A, B]: Shrink[(A, B)] = Shrink.shrinkAny

  private val ledgerBegin = data.LedgerOffset.LedgerBegin.getInstance()
  private val ledgerEnd = data.LedgerOffset.LedgerEnd.getInstance()
  private val emptyFilter = new data.FiltersByParty(Map.empty[String, data.Filter].asJava)

  behavior of "8.1 TransactionClient.getTransactions"

  it should "return transactions from the ledger" in forAll(ledgerContentGen) {
    case (ledgerContent, expectedTransactions) =>
      ledgerServices.withTransactionsClient(Observable.fromIterable(ledgerContent.asJava)) {
        (transactionClient, _) =>
          transactionClient
            .getTransactions(ledgerBegin, ledgerEnd, emptyFilter, false)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingIterable()
            .asScala
            .toList shouldBe expectedTransactions
      }
  }

  behavior of "8.2 TransactionClient.getTransactions"

  it should "pass start offset, end offset, transaction filter and verbose flag with the request" in {
    ledgerServices.withTransactionsClient(Observable.empty()) {
      (transactionClient, transactionService) =>
        val begin = new data.LedgerOffset.Absolute("1")
        val end = new data.LedgerOffset.Absolute("2")

        val transactionFilter = new data.FiltersByParty(
          Map[String, data.Filter](
            "Alice" -> data.InclusiveFilter.ofTemplateIds(
              Set(
                new data.Identifier("p1", "m1", "e1"),
                new data.Identifier("p2", "m2", "e2"),
              ).asJava
            )
          ).asJava
        )

        transactionClient
          .getTransactions(begin, end, transactionFilter, true)
          .toList()
          .blockingGet()

        val request = transactionService.lastTransactionsRequest.get()
        request.begin shouldBe Some(LedgerOffset(Absolute("1")))
        request.end shouldBe Some(LedgerOffset(Absolute("2")))
        val filter = request.filter.get.filtersByParty
        filter.keySet shouldBe Set("Alice")
        filter("Alice").inclusive.get.templateFilters.toSet shouldBe Set(
          TemplateFilter(
            Some(Identifier("p1", moduleName = "m1", entityName = "e1")),
            includeCreatedEventBlob = false,
          ),
          TemplateFilter(
            Some(Identifier("p2", moduleName = "m2", entityName = "e2")),
            includeCreatedEventBlob = false,
          ),
        )
        request.verbose shouldBe true
    }
  }

  behavior of "8.3 TransactionClient.getTransactions"

  it should "request stream with the correct ledger id" in {
    ledgerServices.withTransactionsClient(Observable.empty()) {
      (transactionClient, transactionService) =>
        transactionClient
          .getTransactions(ledgerBegin, ledgerEnd, emptyFilter, false)
          .toList()
          .blockingGet()

        transactionService.lastTransactionsRequest.get().ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  behavior of "8.5 TransactionClient.getTransactionsTrees"

  it should "return transaction trees from the ledger" ignore forAll(ledgerContentTreeGen) {
    case (ledgerContent, expectedTransactionsTrees) =>
      ledgerServices.withTransactionsClient(Observable.fromIterable(ledgerContent.asJava)) {
        (transactionClient, _) =>
          transactionClient
            .getTransactionsTrees(ledgerBegin, ledgerEnd, emptyFilter, false)
            .blockingIterable()
            .asScala
            .toList shouldBe expectedTransactionsTrees
      }
  }

  behavior of "8.6 TransactionClient.getTransactionsTrees"

  it should "pass start offset, end offset, transaction filter and verbose flag with the request" in {
    ledgerServices.withTransactionsClient(Observable.empty()) {
      (transactionClient, transactionService) =>
        val begin = new data.LedgerOffset.Absolute("1")
        val end = new data.LedgerOffset.Absolute("2")

        val transactionFilter = new data.FiltersByParty(
          Map[String, data.Filter](
            "Alice" -> data.InclusiveFilter.ofTemplateIds(
              Set(
                new data.Identifier("p1", "m1", "e1"),
                new data.Identifier("p2", "m2", "e2"),
              ).asJava
            )
          ).asJava
        )

        transactionClient
          .getTransactionsTrees(begin, end, transactionFilter, true)
          .toList()
          .blockingGet()

        val request = transactionService.lastTransactionsTreesRequest.get()
        request.begin shouldBe Some(LedgerOffset(Absolute("1")))
        request.end shouldBe Some(LedgerOffset(Absolute("2")))
        val filter = request.filter.get.filtersByParty
        filter.keySet shouldBe Set("Alice")
        filter("Alice").inclusive.get.templateFilters.toSet shouldBe Set(
          TemplateFilter(
            Some(Identifier("p1", moduleName = "m1", entityName = "e1")),
            includeCreatedEventBlob = false,
          ),
          TemplateFilter(
            Some(Identifier("p2", moduleName = "m2", entityName = "e2")),
            includeCreatedEventBlob = false,
          ),
        )
        request.verbose shouldBe true
    }
  }

  behavior of "8.7 TransactionClient.getTransactionsTrees"

  it should "request stream with the correct ledger ID" in {
    ledgerServices.withTransactionsClient(Observable.empty()) {
      (transactionClient, transactionService) =>
        transactionClient
          .getTransactionsTrees(ledgerBegin, ledgerEnd, emptyFilter, false)
          .toList()
          .blockingGet()

        transactionService.lastTransactionsTreesRequest
          .get()
          .ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  behavior of "8.9 TransactionClient.getTransactionByEventId"

  it should "look up transaction by event ID" ignore forAll(ledgerContentWithEventIdGen) {
    case (ledgerContent, eventId, transactionTree) =>
      ledgerServices.withTransactionsClient(Observable.fromIterable(ledgerContent.asJava)) {
        (transactionClient, transactionService) =>
          transactionClient
            .getTransactionByEventId(eventId, Set.empty[String].asJava)
            .blockingGet() shouldBe transactionTree

          transactionService.lastTransactionByEventIdRequest.get().eventId shouldBe eventId
      }
  }

  behavior of "8.10 TransactionClient.getTransactionByEventId"

  it should "pass the requesting parties with the request" ignore {
    ledgerServices.withTransactionsClient(Observable.empty()) {
      (transactionClient, transactionService) =>
        val requestingParties = Set("Alice", "Bob")

        transactionClient.getTransactionByEventId("eventId", requestingParties.asJava).blockingGet()

        transactionService.lastTransactionByEventIdRequest
          .get()
          .requestingParties
          .toSet shouldBe requestingParties
    }
  }

  behavior of "8.11 TransactionClient.getTransactionByEventId"

  it should "send the correct ledger ID with the request" ignore {
    ledgerServices.withTransactionsClient(Observable.empty()) {
      (transactionClient, transactionService) =>
        transactionClient.getTransactionByEventId("eventId", Set.empty[String].asJava).blockingGet()

        transactionService.lastTransactionByEventIdRequest
          .get()
          .ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  behavior of "8.12 TransactionClient.getTransactionById"

  it should "look up transaction by transaction ID" ignore forAll(
    ledgerContentWithTransactionIdGen
  ) { case (ledgerContent, transactionId, transactionTree) =>
    ledgerServices.withTransactionsClient(Observable.fromIterable(ledgerContent.asJava)) {
      (transactionClient, transactionService) =>
        transactionClient
          .getTransactionById(transactionId, Set.empty[String].asJava)
          .blockingGet() shouldBe transactionTree

        transactionService.lastTransactionByIdRequest.get().transactionId shouldBe transactionId
    }
  }

  behavior of "8.13 TransactionClient.getTransactionById"

  it should "pass the requesting parties with the request" ignore {
    ledgerServices.withTransactionsClient(Observable.empty()) {
      (transactionClient, transactionService) =>
        val requestingParties = Set("Alice", "Bob")

        transactionClient
          .getTransactionById("transactionId", requestingParties.asJava)
          .blockingGet()

        transactionService.lastTransactionByIdRequest
          .get()
          .requestingParties
          .toSet shouldBe requestingParties
    }
  }

  behavior of "8.14 TransactionClient.getTransactionById"

  it should "send the correct ledger ID with the request" ignore {
    ledgerServices.withTransactionsClient(Observable.empty()) {
      (transactionClient, transactionService) =>
        transactionClient
          .getTransactionById("transactionId", Set.empty[String].asJava)
          .blockingGet()

        transactionService.lastTransactionByIdRequest
          .get()
          .ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  behavior of "8.15 TransactionClient.getLedgerEnd"

  it should "provide ledger end from the ledger" in forAll(nonEmptyLedgerContent) {
    case (ledgerContent, transactions) =>
      ledgerServices.withTransactionsClient(Observable.fromIterable(ledgerContent.asJava)) {
        (transactionClient, _) =>
          val expectedOffset = new data.LedgerOffset.Absolute(transactions.last.getOffset)
          transactionClient.getLedgerEnd.blockingGet() shouldBe expectedOffset
      }
  }

  it should "provide LEDGER_BEGIN from empty ledger" in
    ledgerServices.withTransactionsClient(Observable.empty()) { (transactionClient, _) =>
      transactionClient.getLedgerEnd.blockingGet() shouldBe
        data.LedgerOffset.LedgerBegin.getInstance()
    }

  behavior of "8.15 TransactionClient.getLedgerEnd"

  it should "request ledger end with correct ledger ID" in
    ledgerServices.withTransactionsClient(Observable.empty()) {
      (transactionClient, transactionService) =>
        transactionClient.getLedgerEnd.blockingGet()
        transactionService.lastLedgerEndRequest.get().ledgerId shouldBe ledgerServices.ledgerId
    }

  behavior of "Authentication"

  def toAuthenticatedServer(fn: TransactionsClient => Any): Any =
    ledgerServices.withTransactionsClient(Observable.empty(), mockedAuthService) { (client, _) =>
      fn(client)
    }

  it should "deny access without a token" in {
    withClue("getTransactions specifying end") {
      expectUnauthenticated {
        toAuthenticatedServer(
          _.getTransactions(ledgerBegin, ledgerEnd, filterFor(someParty), false)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingIterable()
            .asScala
            .size
        )
      }
    }
    withClue("getTransactions without specifying end") {
      expectUnauthenticated {
        toAuthenticatedServer(
          _.getTransactions(ledgerBegin, filterFor(someParty), false)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingIterable()
            .asScala
            .size
        )
      }
    }
    withClue("getTransactionsTree specifying end") {
      expectUnauthenticated {
        toAuthenticatedServer(
          _.getTransactionsTrees(ledgerBegin, ledgerEnd, filterFor(someParty), false)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingIterable()
            .asScala
            .size
        )
      }
    }
    withClue("getTransactionByEventId") {
      expectUnauthenticated {
        toAuthenticatedServer(_.getTransactionByEventId("...", Set(someParty).asJava).blockingGet())
      }
    }
    withClue("getTransactionById") {
      expectUnauthenticated {
        toAuthenticatedServer(_.getTransactionById("...", Set(someParty).asJava).blockingGet())
      }
    }
    withClue("getFlatTransactionByEventId") {
      expectUnauthenticated {
        toAuthenticatedServer(
          _.getFlatTransactionByEventId("...", Set(someParty).asJava).blockingGet()
        )
      }
    }
    withClue("getFlatTransactionById") {
      expectUnauthenticated {
        toAuthenticatedServer(_.getFlatTransactionById("...", Set(someParty).asJava).blockingGet())
      }
    }
    withClue("getLedgerEnd") {
      expectUnauthenticated {
        toAuthenticatedServer(_.getLedgerEnd.blockingGet())
      }
    }
  }

  it should "deny access with insufficient authorization" in {
    withClue("getTransactions specifying end") {
      expectPermissionDenied {
        toAuthenticatedServer(
          _.getTransactions(
            ledgerBegin,
            ledgerEnd,
            filterFor(someParty),
            false,
            someOtherPartyReadWriteToken,
          )
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingIterable()
            .asScala
            .size
        )
      }
    }
    withClue("getTransactions without specifying end") {
      expectPermissionDenied {
        toAuthenticatedServer(
          _.getTransactions(ledgerBegin, filterFor(someParty), false, someOtherPartyReadWriteToken)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingIterable()
            .asScala
            .size
        )
      }
    }
    withClue("getTransactionsTree specifying end") {
      expectPermissionDenied {
        toAuthenticatedServer(
          _.getTransactionsTrees(
            ledgerBegin,
            ledgerEnd,
            filterFor(someParty),
            false,
            someOtherPartyReadWriteToken,
          )
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingIterable()
            .asScala
            .size
        )
      }
    }
    withClue("getTransactionByEventId") {
      expectPermissionDenied {
        toAuthenticatedServer(
          _.getTransactionByEventId("...", Set(someParty).asJava, someOtherPartyReadWriteToken)
            .blockingGet()
        )
      }
    }
    withClue("getTransactionById") {
      expectPermissionDenied {
        toAuthenticatedServer(
          _.getTransactionById("...", Set(someParty).asJava, someOtherPartyReadWriteToken)
            .blockingGet()
        )
      }
    }
    withClue("getFlatTransactionByEventId") {
      expectPermissionDenied {
        toAuthenticatedServer(
          _.getFlatTransactionByEventId("...", Set(someParty).asJava, someOtherPartyReadWriteToken)
            .blockingGet()
        )
      }
    }
    withClue("getFlatTransactionById") {
      expectPermissionDenied {
        toAuthenticatedServer(
          _.getFlatTransactionById("...", Set(someParty).asJava, someOtherPartyReadWriteToken)
            .blockingGet()
        )
      }
    }
    withClue("getLedgerEnd") {
      expectUnauthenticated {
        toAuthenticatedServer(_.getLedgerEnd(emptyToken).blockingGet())
      }
    }
  }

  it should "allow access with sufficient authorization" in {
    withClue("getTransactions specifying end") {
      toAuthenticatedServer(
        _.getTransactions(
          ledgerBegin,
          ledgerEnd,
          filterFor(someParty),
          false,
          somePartyReadWriteToken,
        )
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingIterable()
          .asScala
          .size
      )
    }
    withClue("getTransactions without specifying end") {
      toAuthenticatedServer(
        _.getTransactions(ledgerBegin, filterFor(someParty), false, somePartyReadWriteToken)
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingIterable()
          .asScala
          .size
      )
    }
    withClue("getTransactionsTree specifying end") {
      toAuthenticatedServer(
        _.getTransactionsTrees(
          ledgerBegin,
          ledgerEnd,
          filterFor(someParty),
          false,
          somePartyReadWriteToken,
        )
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingIterable()
          .asScala
          .size
      )
    }
    withClue("getTransactionByEventId") {
      toAuthenticatedServer(
        _.getTransactionByEventId("...", Set(someParty).asJava, somePartyReadWriteToken)
          .blockingGet()
      )
    }
    withClue("getTransactionById") {
      toAuthenticatedServer(
        _.getTransactionById("...", Set(someParty).asJava, somePartyReadWriteToken)
          .blockingGet()
      )
    }
    withClue("getFlatTransactionByEventId") {
      toAuthenticatedServer(
        _.getFlatTransactionByEventId("...", Set(someParty).asJava, somePartyReadWriteToken)
          .blockingGet()
      )
    }
    withClue("getFlatTransactionById") {
      toAuthenticatedServer(
        _.getFlatTransactionById("...", Set(someParty).asJava, somePartyReadWriteToken)
          .blockingGet()
      )
    }
    withClue("getLedgerEnd") {
      toAuthenticatedServer(_.getLedgerEnd(publicToken).blockingGet())
    }
  }

}
