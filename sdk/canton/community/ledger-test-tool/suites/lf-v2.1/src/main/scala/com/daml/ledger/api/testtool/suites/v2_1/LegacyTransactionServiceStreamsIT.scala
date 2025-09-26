// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.testtool.infrastructure.{
  LedgerTestSuite,
  OngoingStreamPackageUploadTestDar,
  TestConstraints,
}
import com.daml.ledger.test.java.model.test.{Dummy, DummyFactory}
import com.daml.ledger.test.java.ongoing_stream_package_upload.ongoingstreampackageuploadtest.OngoingStreamPackageUploadTestTemplate
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

class LegacyTransactionServiceStreamsIT extends LedgerTestSuite {
  import CompanionImplicits.*

  private[this] val testPackageResourcePath = OngoingStreamPackageUploadTestDar.path

  private def loadTestPackage()(implicit ec: ExecutionContext): Future[ByteString] = {
    val testPackage = Future {
      val in = getClass.getClassLoader.getResourceAsStream(testPackageResourcePath)
      assert(in != null, s"Unable to load test package resource at '$testPackageResourcePath'")
      in
    }
    val bytes = testPackage.map(ByteString.readFrom)
    bytes.onComplete(_ => testPackage.map(_.close()))
    bytes
  }

  test(
    "TXEndToEnd",
    "An empty stream should be served when getting transactions from and to the end of the ledger",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      _ <- ledger.create(party, new Dummy(party))
      request <- ledger.getTransactionsRequestLegacy(ledger.transactionFilter(Some(Seq(party))))
      end <- ledger.currentEnd()
      endToEnd = request.update(_.beginExclusive := end, _.endInclusive := end)
      transactions <- ledger.transactions(endToEnd)
    } yield {
      assert(
        transactions.isEmpty,
        s"No transactions were expected but ${transactions.size} were read",
      )
    }
  })

  test(
    "TXAfterEnd",
    "An OUT_OF_RANGE error should be returned when subscribing past the ledger end",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      _ <- ledger.create(party, new Dummy(party))
      futureOffset <- ledger.offsetBeyondLedgerEnd()
      request <- ledger.getTransactionsRequestLegacy(ledger.transactionFilter(Some(Seq(party))))
      beyondEnd = request.update(
        _.beginExclusive := futureOffset,
        _.optionalEndInclusive := None,
      )
      failure <- ledger.transactions(beyondEnd).mustFail("subscribing past the ledger end")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.OffsetAfterLedgerEnd,
        Some("is after ledger end"),
      )
    }
  })

  test(
    "TXServeUntilCancellation",
    "Items should be served until the client cancels",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 14
    val transactionsToRead = 10
    for {
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      transactions <- ledger.flatTransactionsLegacy(transactionsToRead, party)
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        transactions.size == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactions.size} were instead",
      )
    }
  })

  test(
    "TXServeTailingStreamFlatSingleParty",
    "Items should be served if added during subscription",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 14
    val transactionsToRead = 15
    for {
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      txReq <- ledger
        .getTransactionsRequestLegacy(ledger.transactionFilter(Some(Seq(party))))
        .map(_.update(_.optionalEndInclusive := None))
      flats = ledger.transactions(
        transactionsToRead,
        txReq,
      )
      _ <- ledger.create(party, new Dummy(party))
      transactions <- flats
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        transactions.size == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactions.size} were instead",
      )
    }
  })

  test(
    "TXServeTailingStreamFlatPartyWildcard",
    "Items should be served if party and transaction were added during subscription",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 14
    val transactionsToRead = 15
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      txReq = ledger
        .getTransactionsRequestWithEndLegacy(
          transactionFilter = ledger.transactionFilter(None),
          begin = endOffsetAtTestStart,
          end = None,
        )
      flats = ledger.transactions(
        transactionsToRead,
        txReq,
      )
      party2 <- ledger.allocateParty()
      _ <- ledger.create(party2, new Dummy(party2))
      transactions <- flats
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        transactions.size == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactions.size} were instead",
      )
    }
  })

  test(
    "TXServeTailingStreamsTemplateWildcard",
    "Items should be served if package was added during subscription",
    allocate(SingleParty),
    runConcurrently = false,
    limitation = TestConstraints.GrpcOnly(
      "PackageManagementService listKnownPackages is not available in JSON"
    ),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToRead = 2
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      knownPackagesBefore <- ledger.listKnownPackages().map(_.map(_.name))
      _ <- ledger.create(party, new Dummy(party))
      txReq = ledger
        .getTransactionsRequestWithEndLegacy(
          transactionFilter = ledger.transactionFilter(None),
          begin = endOffsetAtTestStart,
          end = None,
        )
      flats = ledger.transactions(
        transactionsToRead,
        txReq,
      )
      trees = ledger.transactions(
        transactionsToRead,
        txReq,
      )

      testPackage <- loadTestPackage()
      _ <- ledger.uploadDarFile(testPackage)
      _ <- ledger.create(party, new OngoingStreamPackageUploadTestTemplate(party))(
        OngoingStreamPackageUploadTestTemplate.COMPANION
      )

      knownPackagesAfter <- ledger.listKnownPackages().map(_.map(_.name))
      transactions <- flats
      transactionTrees <- trees
    } yield {
      assert(
        knownPackagesAfter.size == knownPackagesBefore.size + 1,
        s"the test package should not have been already uploaded," +
          s"already uploaded packages: $knownPackagesBefore",
      )
      assert(
        transactions.size == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactions.size} were instead",
      )
      assert(
        transactionTrees.size == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactionTrees.size} were instead",
      )
    }
  })

  test(
    "TXCompleteOnLedgerEnd",
    "A stream should complete as soon as the ledger end is hit",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 14
    val transactionsFuture = ledger.flatTransactionsLegacy(party)
    for {
      _ <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      _ <- transactionsFuture
    } yield {
      // doing nothing: we are just checking that `transactionsFuture` completes successfully
    }
  })

  test(
    "TXFilterByTemplate",
    "The transaction service should correctly filter by template identifier",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val create = ledger.submitAndWaitRequest(
      party,
      (new Dummy(party).create.commands.asScala ++ new DummyFactory(
        party
      ).create.commands.asScala).asJava,
    )
    for {
      _ <- ledger.submitAndWait(create)
      transactions <- ledger.flatTransactionsByTemplateIdLegacy(Dummy.TEMPLATE_ID, Some(Seq(party)))
      transactionsPartyWildcard <- ledger
        .flatTransactionsByTemplateIdLegacy(Dummy.TEMPLATE_ID, None)
    } yield {
      val contract = assertSingleton("FilterByTemplate", transactions.flatMap(createdEvents))
      assertEquals(
        "FilterByTemplate",
        contract.getTemplateId,
        Dummy.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
      )
      assertEquals(
        "FilterByTemplate transactions for party-wildcard should match the specific party",
        transactions,
        transactionsPartyWildcard,
      )
    }
  })
}
