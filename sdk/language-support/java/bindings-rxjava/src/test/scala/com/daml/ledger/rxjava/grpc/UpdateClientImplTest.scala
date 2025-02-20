// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.concurrent.TimeUnit
import java.time.Instant
import java.util.Optional
import com.daml.ledger.javaapi.data._
import com.daml.ledger.api.v2.TraceContextOuterClass.TraceContext

import com.daml.ledger.javaapi.data
import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.TransactionGenerator._
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.daml.ledger.api.v2.transaction_filter.TemplateFilter
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi.data.TransactionFilter
import io.reactivex.Observable
import org.scalacheck.Shrink
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

final class UpdateClientImplTest
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers
    with AuthMatchers
    with DataLayerHelpers {

  case class TransactionWithoutRecordTime(
      updateId: String,
      commandId: String,
      workflowId: String,
      effectiveAt: Instant,
      events: List[Event],
      offset: Long,
      domainId: String,
      traceContext: TraceContext,
  )

  object TransactionWithoutRecordTime {
    def apply(t: Transaction): TransactionWithoutRecordTime =
      TransactionWithoutRecordTime(
        t.getUpdateId,
        t.getCommandId,
        t.getWorkflowId,
        t.getEffectiveAt,
        t.getEvents.asScala.toList,
        t.getOffset,
        t.getSynchronizerId,
        t.getTraceContext,
      )
  }

  override val ledgerServices = new LedgerServices("update-service-ledger")

  implicit def tupleNoShrink[A, B]: Shrink[(A, B)] = Shrink.shrinkAny

  private val ledgerBegin: java.lang.Long = 0L
  private val emptyFilter =
    new TransactionFilter(Map.empty[String, data.Filter].asJava, None.toJava)

  behavior of "8.1 TransactionClient.getTransactions"

  it should "return transactions from the ledger" in forAll(ledgerContentGen) {
    case (ledgerContent, expectedTransactions) =>
      val ledgerEnd = expectedTransactions.lastOption.fold(0L)(_.getOffset)
      ledgerServices.withUpdateClient(Observable.fromIterable(ledgerContent.asJava)) {
        (transactionClient, _) =>
          {
            val result: List[TransactionWithoutRecordTime] =
              transactionClient
                .getTransactions(ledgerBegin, Optional.of(long2Long(ledgerEnd)), emptyFilter, false)
                .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
                .blockingIterable()
                .asScala
                .toList
                .map(TransactionWithoutRecordTime(_))
            val expectedTransactionsWithoutRecordTime: List[TransactionWithoutRecordTime] =
              expectedTransactions.map(TransactionWithoutRecordTime(_))
            result shouldEqual expectedTransactionsWithoutRecordTime
          }
      }
  }

  behavior of "8.2 TransactionClient.getTransactions"

  it should "pass start offset, end offset, transaction filter and verbose flag with the request" in {
    ledgerServices.withUpdateClient(Observable.empty()) { (transactionClient, transactionService) =>
      val begin = 1L
      val end = 2L

      val transactionFilter = new TransactionFilter(
        Map[String, data.Filter](
          "Alice" -> new data.CumulativeFilter(
            Map.empty.asJava,
            Map(
              new data.Identifier("p1", "m1", "e1") -> data.Filter.Template.HIDE_CREATED_EVENT_BLOB,
              new data.Identifier("p2", "m2", "e2") -> data.Filter.Template.HIDE_CREATED_EVENT_BLOB,
            ).asJava,
            None.toJava,
          )
        ).asJava,
        None.toJava,
      )

      transactionClient
        .getTransactions(begin, Optional.of(end), transactionFilter, true)
        .toList()
        .blockingGet()

      val request = transactionService.lastUpdatesRequest.get()
      request.beginExclusive shouldBe 1L
      request.endInclusive shouldBe Some(2L)
      val filterByParty =
        request.updateFormat.get.includeTransactions.get.eventFormat.get.filtersByParty
      filterByParty.keySet shouldBe Set("Alice")
      filterByParty("Alice").cumulative
        .flatMap(_.identifierFilter.templateFilter)
        .toSet shouldBe Set(
        TemplateFilter(
          Some(Identifier("p1", moduleName = "m1", entityName = "e1")),
          includeCreatedEventBlob = false,
        ),
        TemplateFilter(
          Some(Identifier("p2", moduleName = "m2", entityName = "e2")),
          includeCreatedEventBlob = false,
        ),
      )
      request.updateFormat.get.includeTransactions.get.eventFormat.get.verbose shouldBe true
    }
  }

  behavior of "8.5 TransactionClient.getTransactionsTrees"

  it should "return transaction trees from the ledger" ignore forAll(ledgerContentTreeGen) {
    case (ledgerContent, expectedTransactionsTrees) =>
      val ledgerEnd = expectedTransactionsTrees.last.getOffset
      ledgerServices.withUpdateClient(Observable.fromIterable(ledgerContent.asJava)) {
        (transactionClient, _) =>
          transactionClient
            .getTransactionsTrees(ledgerBegin, Optional.of(ledgerEnd), emptyFilter, false)
            .blockingIterable()
            .asScala
            .toList shouldBe expectedTransactionsTrees
      }
  }

  behavior of "8.6 TransactionClient.getTransactionsTrees"

  it should "pass start offset, end offset, transaction filter and verbose flag with the request" in {
    ledgerServices.withUpdateClient(Observable.empty()) { (transactionClient, transactionService) =>
      val begin = 1L
      val end: Optional[java.lang.Long] = Optional.of(2L)

      val transactionFilter = new TransactionFilter(
        Map[String, data.Filter](
          "Alice" -> new data.CumulativeFilter(
            Map.empty.asJava,
            Map(
              new data.Identifier("p1", "m1", "e1") -> data.Filter.Template.HIDE_CREATED_EVENT_BLOB,
              new data.Identifier("p2", "m2", "e2") -> data.Filter.Template.HIDE_CREATED_EVENT_BLOB,
            ).asJava,
            None.toJava,
          )
        ).asJava,
        None.toJava,
      )

      transactionClient
        .getTransactionsTrees(begin, end, transactionFilter, true)
        .toList()
        .blockingGet()

      val request = transactionService.lastUpdatesTreesRequest.get()
      request.beginExclusive shouldBe 1L
      request.endInclusive shouldBe Some(2L)
      val filterByParty = request.filter.get.filtersByParty
      filterByParty.keySet shouldBe Set("Alice")
      filterByParty("Alice").cumulative
        .flatMap(_.identifierFilter.templateFilter)
        .toSet shouldBe Set(
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

  behavior of "8.9 TransactionClient.getTransactionTreeByOffset"

  it should "look up transaction by offset" ignore forAll(ledgerContentWithOffsetGen) {
    case (ledgerContent, offset, transactionTree) =>
      ledgerServices.withUpdateClient(Observable.fromIterable(ledgerContent.asJava)) {
        (transactionClient, transactionService) =>
          transactionClient
            .getTransactionTreeByOffset(offset, Set.empty[String].asJava)
            .blockingGet() shouldBe transactionTree

          transactionService.lastTransactionTreeByOffsetRequest.get().offset shouldBe offset
      }
  }

  behavior of "8.10 TransactionClient.getTransactionTreeByOffset"

  it should "pass the requesting parties with the request" ignore {
    ledgerServices.withUpdateClient(Observable.empty()) { (transactionClient, transactionService) =>
      val requestingParties = Set("Alice", "Bob")

      transactionClient
        .getTransactionTreeByOffset(0, requestingParties.asJava)
        .blockingGet()

      transactionService.lastTransactionTreeByOffsetRequest
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
          _.getTransactions(ledgerBegin, Optional.of(1L), filterFor(someParty), false)
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
          _.getTransactionsTrees(ledgerBegin, Optional.of(1L), filterFor(someParty), false)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingIterable()
            .asScala
            .size
        )
      }
    }
    withClue("getTransactionTreeByOffset") {
      expectUnauthenticated {
        toAuthenticatedServer(
          _.getTransactionTreeByOffset(0, Set(someParty).asJava).blockingGet()
        )
      }
    }
    withClue("getTransactionTreeById") {
      expectUnauthenticated {
        toAuthenticatedServer(_.getTransactionTreeById("...", Set(someParty).asJava).blockingGet())
      }
    }
    withClue("getTransactionByOffset") {
      expectUnauthenticated {
        toAuthenticatedServer(
          _.getTransactionByOffset(0, Set(someParty).asJava).blockingGet()
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
            Optional.of(1L: java.lang.Long),
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
    withClue("getTransactionsTree specifying end") {
      expectPermissionDenied {
        toAuthenticatedServer(
          _.getTransactionsTrees(
            ledgerBegin,
            Optional.of(1L),
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
    withClue("getTransactionTreeByOffset") {
      expectPermissionDenied {
        toAuthenticatedServer(
          _.getTransactionTreeByOffset(0, Set(someParty).asJava, someOtherPartyReadWriteToken)
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
    withClue("getTransactionByOffset") {
      expectPermissionDenied {
        toAuthenticatedServer(
          _.getTransactionByOffset(0, Set(someParty).asJava, someOtherPartyReadWriteToken)
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
          Optional.of(1L: java.lang.Long),
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
    withClue("getTransactionsTree specifying end") {
      toAuthenticatedServer(
        _.getTransactionsTrees(
          ledgerBegin,
          Optional.of(1L),
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
    withClue("getTransactionTreeByOffset") {
      toAuthenticatedServer(
        _.getTransactionTreeByOffset(0, Set(someParty).asJava, somePartyReadWriteToken)
          .blockingGet()
      )
    }
    withClue("getTransactionTreeById") {
      toAuthenticatedServer(
        _.getTransactionTreeById("...", Set(someParty).asJava, somePartyReadWriteToken)
          .blockingGet()
      )
    }
    withClue("getTransactionByOffset") {
      toAuthenticatedServer(
        _.getTransactionByOffset(0, Set(someParty).asJava, somePartyReadWriteToken)
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
