// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.concurrent.TimeUnit

import com.daml.ledger.javaapi.data
import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.TransactionGenerator._
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset.Value.Absolute
import com.daml.ledger.api.v1.transaction_filter.TemplateFilter
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.javaapi.data.FiltersByParty
import io.reactivex.Observable
import org.scalacheck.Shrink
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

final class UpdateClientImplTest
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers
    with AuthMatchers
    with DataLayerHelpers {

  override val ledgerServices = new LedgerServices("update-service-ledger")

  implicit def tupleNoShrink[A, B]: Shrink[(A, B)] = Shrink.shrinkAny

  private val ledgerBegin = data.ParticipantOffset.ParticipantBegin.getInstance()
  private val ledgerEnd = data.ParticipantOffset.ParticipantEnd.getInstance()
  private val emptyFilter = new FiltersByParty(Map.empty[String, data.Filter].asJava)

  behavior of "8.1 TransactionClient.getTransactions"

  it should "return transactions from the ledger" in forAll(ledgerContentGen) {
    case (ledgerContent, expectedTransactions) =>
      ledgerServices.withUpdateClient(Observable.fromIterable(ledgerContent.asJava)) {
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
    ledgerServices.withUpdateClient(Observable.empty()) { (transactionClient, transactionService) =>
      val begin = new data.ParticipantOffset.Absolute("1")
      val end = new data.ParticipantOffset.Absolute("2")

      val transactionFilter = new FiltersByParty(
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

      val request = transactionService.lastUpdatesRequest.get()
      request.beginExclusive shouldBe Some(ParticipantOffset(Absolute("1")))
      request.endInclusive shouldBe Some(ParticipantOffset(Absolute("2")))
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

  behavior of "8.5 TransactionClient.getTransactionsTrees"

  it should "return transaction trees from the ledger" ignore forAll(ledgerContentTreeGen) {
    case (ledgerContent, expectedTransactionsTrees) =>
      ledgerServices.withUpdateClient(Observable.fromIterable(ledgerContent.asJava)) {
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
    ledgerServices.withUpdateClient(Observable.empty()) { (transactionClient, transactionService) =>
      val begin = new data.ParticipantOffset.Absolute("1")
      val end = new data.ParticipantOffset.Absolute("2")

      val transactionFilter = new FiltersByParty(
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

      val request = transactionService.lastUpdatesTreesRequest.get()
      request.beginExclusive shouldBe Some(ParticipantOffset(Absolute("1")))
      request.endInclusive shouldBe Some(ParticipantOffset(Absolute("2")))
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

  behavior of "8.9 TransactionClient.getTransactionTreeByEventId"

  it should "look up transaction by event ID" ignore forAll(ledgerContentWithEventIdGen) {
    case (ledgerContent, eventId, transactionTree) =>
      ledgerServices.withUpdateClient(Observable.fromIterable(ledgerContent.asJava)) {
        (transactionClient, transactionService) =>
          transactionClient
            .getTransactionTreeByEventId(eventId, Set.empty[String].asJava)
            .blockingGet() shouldBe transactionTree

          transactionService.lastTransactionTreeByEventIdRequest.get().eventId shouldBe eventId
      }
  }

  behavior of "8.10 TransactionClient.getTransactionTreeByEventId"

  it should "pass the requesting parties with the request" ignore {
    ledgerServices.withUpdateClient(Observable.empty()) { (transactionClient, transactionService) =>
      val requestingParties = Set("Alice", "Bob")

      transactionClient
        .getTransactionTreeByEventId("eventId", requestingParties.asJava)
        .blockingGet()

      transactionService.lastTransactionTreeByEventIdRequest
        .get()
        .requestingParties
        .toSet shouldBe requestingParties
    }
  }

  behavior of "8.12 TransactionClient.getTransactionTreeById"

  it should "look up transaction by transaction ID" ignore forAll(
    ledgerContentWithTransactionIdGen
  ) { case (ledgerContent, transactionId, transactionTree) =>
    ledgerServices.withUpdateClient(Observable.fromIterable(ledgerContent.asJava)) {
      (transactionClient, transactionService) =>
        transactionClient
          .getTransactionTreeById(transactionId, Set.empty[String].asJava)
          .blockingGet() shouldBe transactionTree

        transactionService.lastTransactionTreeByIdRequest.get().updateId shouldBe transactionId
    }
  }

  behavior of "8.13 TransactionClient.getTransactionTreeById"

  it should "pass the requesting parties with the request" ignore {
    ledgerServices.withUpdateClient(Observable.empty()) { (transactionClient, transactionService) =>
      val requestingParties = Set("Alice", "Bob")

      transactionClient
        .getTransactionTreeById("transactionId", requestingParties.asJava)
        .blockingGet()

      transactionService.lastTransactionTreeByIdRequest
        .get()
        .requestingParties
        .toSet shouldBe requestingParties
    }
  }

  behavior of "Authentication"

  def toAuthenticatedServer(fn: UpdateClient => Any): Any =
    ledgerServices.withUpdateClient(Observable.empty(), mockedAuthService) { (client, _) =>
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
    withClue("getTransactionTreeByEventId") {
      expectUnauthenticated {
        toAuthenticatedServer(
          _.getTransactionTreeByEventId("...", Set(someParty).asJava).blockingGet()
        )
      }
    }
    withClue("getTransactionTreeById") {
      expectUnauthenticated {
        toAuthenticatedServer(_.getTransactionTreeById("...", Set(someParty).asJava).blockingGet())
      }
    }
    withClue("getTransactionByEventId") {
      expectUnauthenticated {
        toAuthenticatedServer(
          _.getTransactionByEventId("...", Set(someParty).asJava).blockingGet()
        )
      }
    }
    withClue("getTransactionById") {
      expectUnauthenticated {
        toAuthenticatedServer(_.getTransactionById("...", Set(someParty).asJava).blockingGet())
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
    withClue("getTransactionTreeByEventId") {
      expectPermissionDenied {
        toAuthenticatedServer(
          _.getTransactionTreeByEventId("...", Set(someParty).asJava, someOtherPartyReadWriteToken)
            .blockingGet()
        )
      }
    }
    withClue("getTransactionTreeById") {
      expectPermissionDenied {
        toAuthenticatedServer(
          _.getTransactionTreeById("...", Set(someParty).asJava, someOtherPartyReadWriteToken)
            .blockingGet()
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
    withClue("getTransactionTreeByEventId") {
      toAuthenticatedServer(
        _.getTransactionTreeByEventId("...", Set(someParty).asJava, somePartyReadWriteToken)
          .blockingGet()
      )
    }
    withClue("getTransactionTreeById") {
      toAuthenticatedServer(
        _.getTransactionTreeById("...", Set(someParty).asJava, somePartyReadWriteToken)
          .blockingGet()
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
  }

}
