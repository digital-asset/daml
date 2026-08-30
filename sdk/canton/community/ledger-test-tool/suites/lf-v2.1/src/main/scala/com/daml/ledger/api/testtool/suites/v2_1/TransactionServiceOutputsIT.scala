// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party, TestConstraints}
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.value as api
import com.daml.ledger.api.v2.value.{Record, RecordField}
import com.daml.ledger.test.java.model.test.{Dummy, NothingArgument}
import com.daml.ledger.test.java.model.trailingnones.TrailingNones
import com.daml.ledger.test.java.model.trailingnonesiface.TrailingNonesIface
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}

import java.util.Optional
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.OptionConverters.*

class TransactionServiceOutputsIT extends LedgerTestSuite {
  import ClearIdsImplicits.*
  import CompanionImplicits.*
  import com.daml.ledger.api.testtool.infrastructure.RemoveTrailingNone.Implicits

  test(
    "TXUnitAsArgumentToNothing",
    "Daml engine returns Unit as argument with trailing None fields removed",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val template = new NothingArgument(party, None.toJava)
    val create = ledger.submitAndWaitForTransactionRequest(party, template.create.commands)
    for {
      transactionResponse <- ledger.submitAndWaitForTransaction(create)
    } yield {
      val contract = assertSingleton(
        "UnitAsArgumentToNothing",
        createdEvents(transactionResponse.getTransaction),
      )
      assertEquals(
        "UnitAsArgumentToNothing",
        contract.getCreateArguments.clearValueIds,
        Record.fromJavaProto(template.toValue.withoutTrailingNoneFields.toProtoRecord),
      )
    }
  })

  test(
    "TXVerbosity",
    "Expose field names only if the verbose flag is set to true",
    allocate(SingleParty),
    limitation = TestConstraints.GrpcOnly("Labels are always emitted by Transcode/SchemaProcessor"),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      _ <- ledger.create(party, new Dummy(party))
      verboseTransactions <- ledger
        .getTransactionsRequest(
          ledger.transactionFormat(
            parties = Some(Seq(party)),
            transactionShape = AcsDelta,
            verbose = true,
          )
        )
        .flatMap(ledger.transactions)
      verboseTransactionsLedgerEffects <- ledger
        .getTransactionsRequest(
          ledger
            .transactionFormat(
              parties = Some(Seq(party)),
              transactionShape = LedgerEffects,
              verbose = true,
            )
        )
        .flatMap(ledger.transactions)
      nonVerboseTransactions <- ledger
        .getTransactionsRequest(
          ledger.transactionFormat(
            parties = Some(Seq(party)),
            transactionShape = AcsDelta,
            verbose = false,
          )
        )
        .flatMap(ledger.transactions)
      nonVerboseTransactionLedgerEffects <- ledger
        .getTransactionsRequest(
          ledger
            .transactionFormat(
              parties = Some(Seq(party)),
              transactionShape = LedgerEffects,
              verbose = false,
            )
        )
        .flatMap(ledger.transactions)
    } yield {
      assertLabelsAreExposedCorrectly(
        party,
        verboseTransactions,
        verboseTransactionsLedgerEffects,
        labelIsNonEmpty = true,
      )
      assertLabelsAreExposedCorrectly(
        party,
        nonVerboseTransactions,
        nonVerboseTransactionLedgerEffects,
        labelIsNonEmpty = false,
      )
    }
  })

  test(
    "TXVerboseNoTrailingNones",
    "Ledger API does not populate trailing Optional Nones (in non-verbose mode) on Ledger API queries",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    testDroppedTrailingNones(ledger = ledger, party = party, verbose = true)
  })

  test(
    "TXNonVerboseNoTrailingNones",
    "Ledger API does not populate trailing Optional Nones (in verbose mode) on Ledger API queries",
    allocate(SingleParty),
    limitation = TestConstraints.GrpcOnly("JSON API always outputs label names"),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    testDroppedTrailingNones(ledger = ledger, party = party, verbose = false)
  })

  private def testDroppedTrailingNones(
      ledger: ParticipantTestContext,
      party: Party,
      verbose: Boolean,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    def ifVerbosePopulated(label: String): String = if (verbose) label else ""
    for {
      contract <- ledger.create(party, new TrailingNones(party, "some", Optional.empty()))
      ledgerEnd <- ledger.currentEnd()
      acsReq = ledger.activeContractsRequest(
        parties = Some(Seq(party)),
        activeAtOffset = ledgerEnd,
        templateIds = Seq(TrailingNones.TEMPLATE_ID),
        interfaceFilters = Seq(TrailingNonesIface.INTERFACE_ID -> true),
        verbose = verbose,
      )
      acs <- ledger.activeContracts(acsReq)
      tx <- ledger
        .submitAndWaitForTransaction(
          ledger.submitAndWaitForTransactionRequest(
            party,
            contract.exerciseExerciseChoice("populated field", Optional.empty()).commands(),
            LedgerEffects,
            verbose = verbose,
          )
        )
        .map(_.getTransaction)
    } yield {
      val createdEvent = assertSingleton("Only one contract should be on the ACS", acs)

      // Assert create arguments
      val createArgFields =
        assertDefined(createdEvent.createArguments, "Create arguments should be defined").fields
      assertEquals(
        "Expected field arguments without trailing None",
        createArgFields,
        Seq(
          RecordField(
            ifVerbosePopulated("owner"),
            Some(api.Value(api.Value.Sum.Party(party.getValue))),
          ),
          RecordField(ifVerbosePopulated("nonOpt"), Some(api.Value(api.Value.Sum.Text("some")))),
        ),
      )

      // Assert interface view
      val interfaceView =
        assertSingleton("Only one interface view expected", createdEvent.interfaceViews)

      assertEquals(
        "Mismatching interface views",
        assertDefined(interfaceView.viewValue, "Interface view value should be defined").fields,
        Seq(RecordField(ifVerbosePopulated("nonOpt"), Some(api.Value(api.Value.Sum.Text("some"))))),
      )

      val exercisedEvent =
        assertSingleton("Only one exercise event expected", tx.events.map(_.getExercised))

      // Assert exercise arguments
      val exeArg = assertDefined(exercisedEvent.choiceArgument, "Choice argument should be defined")
      assertEquals(
        "Mismatching exercise argument",
        exeArg.getRecord.fields,
        Seq(
          RecordField(
            ifVerbosePopulated("nonOptArg"),
            Some(api.Value(api.Value.Sum.Text("populated field"))),
          )
        ),
      )

      // Assert exercise result
      val exerciseResult =
        assertDefined(exercisedEvent.exerciseResult, "Choice result should be defined").getRecord

      assertEquals(
        "Mismatching exercise result",
        exerciseResult.fields,
        Seq(
          RecordField(
            ifVerbosePopulated("field1"),
            Some(api.Value(api.Value.Sum.Text("populated field"))),
          )
        ),
      )
    }
  }

  private def assertLabelsAreExposedCorrectly(
      party: Party,
      transactions: Seq[Transaction],
      transactionsLedgerEffects: Seq[Transaction],
      labelIsNonEmpty: Boolean,
  ): Unit = {

    def transactionFields(createdEvent: Seq[CreatedEvent]): Seq[RecordField] = createdEvent
      .flatMap(_.getCreateArguments.fields)

    val transactionLedgerEffectsCreatedEvents: Seq[CreatedEvent] =
      for {
        transactionLedgerEffects <- transactionsLedgerEffects
        event <- transactionLedgerEffects.events
        createdEvent = event.getCreated
      } yield createdEvent

    val transactionLedgerEffectsFields: Seq[RecordField] =
      transactionFields(transactionLedgerEffectsCreatedEvents)

    val flatTransactionFields: Seq[RecordField] =
      transactionFields(
        transactions
          .flatMap(_.events)
          .map(_.getCreated)
      )

    assert(transactions.nonEmpty, s"$party expected non empty transaction list")
    assert(
      transactionsLedgerEffects.nonEmpty,
      s"$party expected non empty ledger effects transaction list",
    )

    val text = labelIsNonEmpty match {
      case true => "with"
      case false => "without"
    }
    assert(
      flatTransactionFields.forall(_.label.nonEmpty == labelIsNonEmpty),
      s"$party expected a contract $text labels, but received $transactions.",
    )
    assert(
      transactionLedgerEffectsFields.forall(_.label.nonEmpty == labelIsNonEmpty),
      s"$party expected a contract $text labels, but received $transactionsLedgerEffects.",
    )
  }

}
