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
import com.daml.ledger.api.v2.transaction_filter.TransactionFormat
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.test.java.model.test.{Dummy, DummyFactory}
import com.daml.ledger.test.java.ongoing_stream_package_upload.ongoingstreampackageuploadtest.OngoingStreamPackageUploadTestTemplate
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

class UpdateServiceStreamsIT extends LedgerTestSuite {
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
    "TXLedgerEffectsEndToEnd",
    "An empty stream of transactions should be served when getting transactions from and to the current ledger end",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      ledgerEnd <- ledger.currentEnd()
      request <- ledger.getTransactionsRequest(
        transactionFormat =
          ledger.transactionFormat(Some(Seq(party)), transactionShape = LedgerEffects)
      )
      fromAndToBegin =
        request.update(_.beginExclusive := ledgerEnd, _.endInclusive := ledgerEnd)
      transactions <- ledger.transactions(fromAndToBegin)
    } yield {
      assert(
        transactions.isEmpty,
        s"Received a non-empty stream with ${transactions.size} transactions in it.",
      )
    }
  })

  test(
    "TXEndToEnd",
    "An empty stream should be served when getting transactions from and to the end of the ledger",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      _ <- ledger.create(party, new Dummy(party))
      request <- ledger.getTransactionsRequest(transactionFormat =
        ledger.transactionFormat(Some(Seq(party)))
      )
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
      request <- ledger.getTransactionsRequest(
        transactionFormat = ledger.transactionFormat(Some(Seq(party)))
      )
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
      transactions <- ledger.transactions(transactionsToRead, AcsDelta, party)
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
    "TXServeTailingStreamSingleParty",
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
        .getTransactionsRequest(
          transactionFormat = ledger.transactionFormat(Some(Seq(party)))
        )
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
    "TXServeTailingStreamAcsDeltaPartyWildcard",
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
        .getTransactionsRequestWithEnd(
          transactionFormat = ledger.transactionFormat(parties = None),
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
      assertAcsDelta(
        transactions.flatMap(_.events),
        acsDelta = true,
        "The acs_delta field in created events should be set",
      )
    }
  })

  test(
    "TXServeTailingStreamLedgerEffectsPartyWildcard",
    "Transaction with ledger effects should be served if party and transaction added during subscription",
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
        .getTransactionsRequestWithEnd(
          transactionFormat =
            ledger.transactionFormat(parties = None, transactionShape = LedgerEffects),
          begin = endOffsetAtTestStart,
          end = None,
        )
      trees = ledger.transactions(
        transactionsToRead,
        txReq,
      )
      party2 <- ledger.allocateParty()
      _ <- ledger.create(party2, new Dummy(party2))
      transactions <- trees
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        transactions.size == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactions.size} were instead",
      )
      assertAcsDelta(
        transactions.flatMap(_.events),
        acsDelta = true,
        "The acs_delta field in created events should be set",
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
        .getTransactionsRequestWithEnd(
          transactionFormat = ledger.transactionFormat(parties = None),
          begin = endOffsetAtTestStart,
          end = None,
        )
      txReqLedgerEffects = ledger
        .getTransactionsRequestWithEnd(
          transactionFormat =
            ledger.transactionFormat(parties = None, transactionShape = LedgerEffects),
          begin = endOffsetAtTestStart,
          end = None,
        )
      flats = ledger.transactions(
        transactionsToRead,
        txReq,
      )
      trees = ledger.transactions(
        transactionsToRead,
        txReqLedgerEffects,
      )

      testPackage <- loadTestPackage()
      _ <- ledger.uploadDarFileAndVetOnConnectedSynchronizers(testPackage)
      _ <- ledger.create(party, new OngoingStreamPackageUploadTestTemplate(party))(
        OngoingStreamPackageUploadTestTemplate.COMPANION
      )

      knownPackagesAfter <- ledger.listKnownPackages().map(_.map(_.name))
      flatTransactions <- flats
      transactionTrees <- trees
    } yield {
      assert(
        knownPackagesAfter.size == knownPackagesBefore.size + 1,
        s"the test package should not have been already uploaded," +
          s"already uploaded packages: $knownPackagesBefore",
      )
      assert(
        flatTransactions.size == transactionsToRead,
        s"$transactionsToRead should have been received but ${flatTransactions.size} were instead",
      )
      assert(
        transactionTrees.size == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactionTrees.size} were instead",
      )
      assertAcsDelta(
        flatTransactions.flatMap(_.events),
        acsDelta = true,
        "The acs_delta field in created events should be set",
      )
      assertAcsDelta(
        transactionTrees.flatMap(_.events),
        acsDelta = true,
        "The acs_delta field in created events should be set",
      )
    }
  })

  test(
    "TXCompleteAcsDeltaOnLedgerEnd",
    "A stream should complete as soon as the ledger end is hit",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 14
    val transactionsFuture = ledger.transactions(
      transactionShape = AcsDelta,
      parties = party,
    )
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
    "TXCompleteLedgerEffectsOnLedgerEnd",
    "A stream of ledger effects transactions should complete as soon as the ledger end is hit",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 14
    val transactionsFuture = ledger.transactions(LedgerEffects, party)
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
      transactions <- ledger.transactionsByTemplateId(Dummy.TEMPLATE_ID, Some(Seq(party)))
      transactionsPartyWildcard <- ledger.transactionsByTemplateId(Dummy.TEMPLATE_ID, None)
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

  test(
    "TXFilterByInterface",
    "The transaction service should correctly filter by interface identifier",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import com.daml.ledger.test.java.model.iou.{Iou, IIou}

    val iou = new Iou(party, party, "USD", java.math.BigDecimal.ONE, Nil.asJava)
    val create = ledger.submitAndWaitRequest(party, iou.create.commands)

    for {
      _ <- ledger.submitAndWait(create)
      interfaceFilter <- ledger.getTransactionsRequest(
        ledger.transactionFormat(Some(Seq(party)), Seq.empty, Seq(IIou.TEMPLATE_ID -> true))
      )
      transactions <- ledger.transactions(interfaceFilter)
    } yield {
      import com.daml.ledger.api.v2.event.CreatedEvent.toJavaProto
      import com.daml.ledger.javaapi.data.CreatedEvent.fromProto

      val created = assertSingleton("FilterByInterface", transactions.flatMap(createdEvents))
      assertEquals(
        "FilterByInterface",
        created.getTemplateId,
        Iou.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
      )

      val view = IIou.INTERFACE.fromCreatedEvent(fromProto(toJavaProto(created)))
      assertEquals(view.data.icurrency, "USD")
    }
  })

  test(
    "TXTransactionFormat",
    "The transactions should be served when the transaction format is set",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToRead = 1
    for {
      _ <- ledger.create(party, new Dummy(party))
      end <- ledger.currentEnd()
      format = ledger.eventFormat(verbose = false, parties = Some(Seq(party)))
      reqForTransactions = ledger
        .getTransactionsRequestWithEnd(
          transactionFormat =
            ledger.transactionFormat(parties = Some(Seq(party)), transactionShape = LedgerEffects),
          end = Some(end),
        )
      reqForReassignments = ledger
        .getUpdatesRequestWithEnd(
          transactionFormatO = None,
          reassignmentsFormatO = Some(format),
          end = Some(end),
        )
      reqForBoth = ledger
        .getUpdatesRequestWithEnd(
          transactionFormatO =
            Some(TransactionFormat(Some(format), TRANSACTION_SHAPE_LEDGER_EFFECTS)),
          reassignmentsFormatO = Some(format),
          end = Some(end),
        )
      txsFromReqForTransactions <- ledger.transactions(reqForTransactions)
      txsFromReqForReassignments <- ledger.transactions(reqForReassignments)
      txsFromReqForBoth <- ledger.transactions(reqForBoth)
    } yield {
      assertLength(
        s"""$transactionsToRead transactions should have been received from the request for transactions but
           | ${txsFromReqForTransactions.size} were instead""".stripMargin,
        transactionsToRead,
        txsFromReqForTransactions,
      )
      assertLength(
        s"""No transactions should have been received from the request for reassignments but
           | ${txsFromReqForReassignments.size} were instead""".stripMargin,
        0,
        txsFromReqForReassignments,
      )
      assertLength(
        s"""$transactionsToRead transactions should have been received from the request for both reassignments and
           | transactions but ${txsFromReqForBoth.size} were instead""".stripMargin,
        transactionsToRead,
        txsFromReqForBoth,
      )
    }
  })

  test(
    "TXLedgerEffectsTransientContract",
    "The transactions stream with LedgerEffects should return non empty events for a transient contract",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
    for {
      _ <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(owner, new Dummy(owner).createAnd().exerciseArchive().commands)
      )
      txs <- ledger.transactions(
        transactionShape = LedgerEffects,
        parties = owner,
      )
    } yield {
      val tx = assertSingleton("One transaction should be found", txs)
      assert(tx.events.nonEmpty, "Expected non empty events in the transaction")
      assertAcsDelta(
        tx.events,
        acsDelta = false,
        "The acs_delta field in transient events should not be set",
      )
    }
  })

  test(
    "TXAcsDeltaTransientContract",
    "The transactions stream with AcsDelta should return no transaction for a transient contract",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
    for {
      _ <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(owner, new Dummy(owner).createAnd().exerciseArchive().commands)
      )
      txs <- ledger.transactions(
        transactionShape = AcsDelta,
        parties = owner,
      )
    } yield {
      assert(txs.isEmpty, "Expected no transactions")
    }
  })

  test(
    "TXNonConsumingChoiceAcsDeltaFlag",
    "GetUpdateById returns NOT_FOUND when command contains only a non-consuming choice",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
    for {
      contractId: Dummy.ContractId <- ledger.create(owner, new Dummy(owner))
      end <- ledger.currentEnd()
      _ <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(owner, contractId.exerciseDummyNonConsuming().commands)
      )
      txs <- ledger
        .getTransactionsRequest(
          transactionFormat =
            ledger.transactionFormat(Some(Seq(owner)), transactionShape = LedgerEffects),
          begin = end,
        )
        .flatMap(ledger.transactions)
    } yield {
      val tx = assertSingleton(
        "Expected one transaction with a non-consuming event",
        txs,
      )
      assertSingleton(
        "Expected one non-consuming event",
        tx.events,
      )
      assertAcsDelta(
        tx.events,
        acsDelta = false,
        "The acs_delta field in transient events should not be set",
      )
    }
  })

}
