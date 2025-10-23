// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_dev

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, TransactionHelpers}
import com.daml.ledger.api.testtool.suites.v2_2.CompanionImplicits.*
import com.daml.ledger.api.testtool.suites.v2_dev.EventsDescendantsIT.isDescendant
import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.event.Event.Event.Exercised
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.experimental.exceptions.ExceptionTester
import com.daml.ledger.test.java.model.test.{
  Agreement,
  AgreementFactory,
  Dummy,
  DummyFactory,
  TriProposal,
}
import com.digitalasset.canton.ledger.api.TransactionShape.LedgerEffects
import com.digitalasset.canton.platform.store.utils.EventOps.EventOps

import scala.jdk.CollectionConverters.*

class EventsDescendantsIT extends LedgerTestSuite {
  import EventsDescendantsIT.CompanionImplicits.*

  test(
    "SingleConsumingExercisedDescendants",
    "Descendant events in single consuming exercised",
    allocate(
      SingleParty
    ),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    // Transaction
    // └─ #0 Exercise DummyChoice1
    for {
      contract <- alpha.create(party, new Dummy(party))
      tx <- alpha.exercise(party, contract.exerciseDummyChoice1())
    } yield {
      val exercisedEvents = TransactionHelpers.exercisedEvents(tx)
      val exercisedEvent = assertSingleton(
        "Transaction should contain the exercised event",
        exercisedEvents,
      )

      assert(
        isDescendant(
          who = exercisedEvent.nodeId,
          of = Event(Exercised(exercisedEvent)),
        ),
        "The exercised event should have been descendant of itself",
      )

    }
  })

  test(
    "SingleCreatedInExercisedDescendants",
    "Descendant events in exercised with nested single created",
    allocate(
      SingleParty
    ),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    // Transaction
    // └─ #0 Exercise Clone
    //    └─ #1 Create Dummy
    for {
      contract <- alpha.create(party, new Dummy(party))
      txTree <- alpha.exercise(party, contract.exerciseClone())
    } yield {
      val exercisedEvents = TransactionHelpers.exercisedEvents(txTree)
      val exercisedEvent = assertSingleton(
        "Transaction should contain the exercised event",
        exercisedEvents,
      )
      val createdEvents = TransactionHelpers.createdEvents(txTree)
      assertSingleton(
        "Transaction should contain the created event",
        createdEvents,
      )

      val events = txTree.events

      events.foreach(event =>
        assert(
          isDescendant(
            who = event.nodeId,
            of = Event(Exercised(exercisedEvent)),
          ),
          s"The event $event should have been descendant of the exercised event",
        )
      )
    }
  })

  test(
    "MultipleCreatedInExercisedDescendants",
    "Descendant events in exercised with nested multiple created",
    allocate(
      SingleParty
    ),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    // Transaction
    // └─ #0 Exercise DummyFactoryCall
    //    ├─ #1 Create Dummy
    //    └─ #2 Create DummyWithParam
    for {
      dummyFactory <- alpha.create(party, new DummyFactory(party))
      txTree <- alpha.exercise(party, dummyFactory.exerciseDummyFactoryCall())
    } yield {
      val exercisedEvents = TransactionHelpers.exercisedEvents(txTree)
      val exercisedEvent = assertSingleton(
        "Transaction should contain the exercised event",
        exercisedEvents,
      )
      val createdEvents = TransactionHelpers.createdEvents(txTree)
      assertLength(
        "Transaction should contain the two created events",
        2,
        createdEvents,
      )

      val events = txTree.events

      events.foreach(event =>
        assert(
          isDescendant(
            who = event.nodeId,
            of = Event(Exercised(exercisedEvent)),
          ),
          s"The event $event should have been descendant of the exercised event",
        )
      )
    }
  })

  test(
    "DeeplyNestedDescendants",
    "Descendant events in exercised with deeply nested events",
    allocate(
      SingleParty
    ),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    // Transaction
    // └─ #0 Exercise DummyFactoryCallWithExercise
    //    ├─ #1 Create Dummy1
    //    ├─ #2 Create Dummy2
    //    ├─ #3 Create Dummy3
    //    ├─ #4 Create DummyWithParam
    //    ├─ #5 Exercise Clone on Dummy1
    //    │  └─ #6 Create B1
    //    ├─ #7 Exercise DummyChoice1 on Dummy2
    //    └─ #8 Exercise DummyNonConsuming on Dummy3
    for {
      dummyFactory <- alpha.create(party, new DummyFactory(party))
      txTree <- alpha.exercise(party, dummyFactory.exerciseDummyFactoryCallWithExercise())
    } yield {
      val exercisedEvents = TransactionHelpers.exercisedEvents(txTree)
      val seminalExercisedEvent = assertSingleton(
        "Transaction should contain at least one exercised event",
        exercisedEvents.headOption.toList,
      )
      val events = txTree.events

      // all the events should be descendants of the first exercise (even itself)
      events.foreach(event =>
        assert(
          isDescendant(
            who = event.nodeId,
            of = Event(Exercised(seminalExercisedEvent)),
          ),
          s"The event $event should have been descendant of the exercised event",
        )
      )

      val createdEvents = TransactionHelpers.createdEvents(txTree)
      val lastCreatedEvent = assertSingleton(
        "Transaction should contain 4 created events",
        createdEvents.sortBy(_.nodeId).lastOption.toList,
      )

      val cloneExercisedEvent = assertSingleton(
        "Transaction should contain the Clone exercised event",
        exercisedEvents.filter(_.choice == "Clone").toList,
      )

      assert(
        isDescendant(
          who = lastCreatedEvent.nodeId,
          of = Event(Exercised(cloneExercisedEvent)),
        ),
        s"The last created event $lastCreatedEvent should have been descendant of the Clone exercised event",
      )

    }
  })

  test(
    "DescendantsWithFetch",
    "Descendant events in transaction with fetch",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(operator)), Participant(_, Seq(_))) =>
    // Transaction
    // └─ #0 Exercise AcceptTriProposal
    //    ├─ #1 Fetch                      (filtered out)
    //    └─ #2 Exercise TriProposalAccept
    //       └─ #3 Create TriAgreement
    for {
      agreementFactory <- alpha.create(operator, new AgreementFactory(operator, operator))
      agreement <-
        alpha.exerciseAndGetContract[Agreement.ContractId, Agreement](
          operator,
          agreementFactory.exerciseAgreementFactoryAccept(),
        )
      triProposalTemplate = new TriProposal(operator, operator, operator)
      triProposal <- alpha.create(operator, triProposalTemplate)
      txTree <- eventually("exerciseAcceptTriProposal") {
        alpha.exercise(operator, agreement.exerciseAcceptTriProposal(triProposal))
      }
    } yield {
      val exercisedEvents = TransactionHelpers.exercisedEvents(txTree)
      assertLength(
        "Transaction should contain two exercised events",
        2,
        exercisedEvents,
      )
      val createdEvents = TransactionHelpers.createdEvents(txTree)
      val createdEvent = assertSingleton(
        "Transaction should contain the created event",
        createdEvents,
      )

      // created event should be descendant of both exercised events
      exercisedEvents.foreach(exercisedEvent =>
        assert(
          isDescendant(
            who = createdEvent.nodeId,
            of = Event(Exercised(exercisedEvent)),
          ),
          s"The created event $createdEvent should have been descendant of the exercised event $exercisedEvent",
        )
      )

      val seminalExercisedEvent = assertSingleton(
        "Transaction should contain at least one exercised event",
        exercisedEvents.headOption.toList,
      )

      // exercised events should be descendant of the first exercised event
      exercisedEvents.foreach(exercisedEvent =>
        assert(
          isDescendant(
            who = exercisedEvent.nodeId,
            of = Event(Exercised(seminalExercisedEvent)),
          ),
          s"The exercised event $exercisedEvent should have been descendant of the first exercised event $seminalExercisedEvent",
        )
      )

    }
  })

  test(
    "NonDescendantsRollbackFetch",
    "Events that are not descendant in transaction with rollback of fetch",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    // Transaction
    // ├─ #0 Exercise RollbackFetch
    // │  └─ #1 Rollback            (filtered out)
    // │     └─ #2 Fetch            (filtered out)
    // └─ #3 Exercise Noop
    for {
      t <- alpha.create(party, new ExceptionTester(party))
      tFetch <- alpha.create(party, new ExceptionTester(party))
      commands = t.exerciseRollbackFetch(tFetch).commands().asScala
        ++ tFetch.exerciseNoop.commands().asScala
      ledgerEffects <- alpha
        .submitAndWaitForTransaction(
          alpha.submitAndWaitForTransactionRequest(party, commands.asJava, LedgerEffects)
        )
        .map(_.getTransaction)

    } yield {
      val exercisedEvents = TransactionHelpers.exercisedEvents(ledgerEffects)
      assertLength(
        "Transaction should contain two exercised events",
        2,
        exercisedEvents,
      )
      val first = assertSingleton(
        "exercised events should contain at least one event",
        exercisedEvents.headOption.toList,
      )
      val second = assertSingleton(
        "exercised events should contain at least one event",
        exercisedEvents.lastOption.toList,
      )

      // exercised events should not be descendant of each other
      Seq(first -> second, second -> first).foreach { case (event1, event2) =>
        assert(
          !isDescendant(
            who = event1.nodeId,
            of = Event(Exercised(event2)),
          ),
          s"The exercised event $event1 should have NOT been descendant of the exercised event $event2",
        )
      }

    }
  })

  test(
    "DescendantsRollbackFetch",
    "Descendant events in transaction with rollback of fetch",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    // Transaction
    // └─ #0 Exercise RollbackFetchWithExercise
    //    ├─ #1 Rollback                        (filtered out)
    //    │  └─ #2 Fetch                        (filtered out)
    //    └─ #3 Exercise Noop
    for {
      t <- alpha.create(party, new ExceptionTester(party))
      tFetch <- alpha.create(party, new ExceptionTester(party))
      txTree <- alpha.exercise(party, t.exerciseRollbackFetchWithExercise(tFetch))
    } yield {
      val exercisedEvents = TransactionHelpers.exercisedEvents(txTree)
      assertLength(
        "Transaction should contain two exercised events",
        2,
        exercisedEvents,
      )
      val seminalExercisedEvent = assertSingleton(
        "Transaction should contain at least one exercised event",
        exercisedEvents.headOption.toList,
      )

      val events = txTree.events

      // events should be descendant of the first exercised event
      events.foreach(event =>
        assert(
          isDescendant(
            who = event.nodeId,
            of = Event(Exercised(seminalExercisedEvent)),
          ),
          s"The event $event should have been descendant of the first exercised event $seminalExercisedEvent",
        )
      )

    }
  })

  test(
    "DescendantsRollbackExercise",
    "Descendant events in transaction with rollback of exercise",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    // Transaction
    // └─ #0 Exercise RollbackConsumingWithCreate
    //    ├─ #1 Rollback                          (filtered out)
    //    │  └─ #2 Exercise                       (filtered out)
    //    └─ #3 Create ExceptionTester
    for {
      t <- alpha.create(party, new ExceptionTester(party))
      tExercise <- alpha.create(party, new ExceptionTester(party))
      txTree <- alpha.exercise(party, t.exerciseRollbackConsumingWithCreate(tExercise))
    } yield {
      val exercisedEvents = TransactionHelpers.exercisedEvents(txTree)
      val seminalExercisedEvent = assertSingleton(
        "Transaction should one exercised event",
        exercisedEvents,
      )

      val events = txTree.events

      // events should be descendant of the first exercised event
      events.foreach(event =>
        assert(
          isDescendant(
            who = event.nodeId,
            of = Event(Exercised(seminalExercisedEvent)),
          ),
          s"The event $event should have been descendant of the first exercised event $seminalExercisedEvent",
        )
      )

    }
  })

  test(
    "DescendantsRollbackCreate",
    "Descendant events in transaction with rollback of create",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    // Transaction
    // └─ #0 Exercise RollbackCreateWithExercise
    //    ├─ #1 Rollback                         (filtered out)
    //    │  └─ #2 Exercise RolledBackChoice     (filtered out)
    //    │     └─ #3 Create                     (filtered out)
    //    └─ #4 Exercise Noop
    for {
      t <- alpha.create(party, new ExceptionTester(party))
      txTree <- alpha.exercise(party, t.exerciseRollbackCreateWithExercise())
    } yield {
      val exercisedEvents = TransactionHelpers.exercisedEvents(txTree)
      assertLength(
        "Transaction should contain two exercised events",
        2,
        exercisedEvents,
      )
      val seminalExercisedEvent = assertSingleton(
        "Transaction should contain at least one exercised event",
        exercisedEvents.headOption.toList,
      )

      val events = txTree.events

      // events should be descendant of the first exercised event
      events.foreach(event =>
        assert(
          isDescendant(
            who = event.nodeId,
            of = Event(Exercised(seminalExercisedEvent)),
          ),
          s"The event $event should have been descendant of the first exercised event $seminalExercisedEvent",
        )
      )
    }
  })

}

object EventsDescendantsIT {
  def isDescendant(who: Int, of: Event): Boolean = {
    val nodeId = of.nodeId
    val lastDescendantNodeId = of.event.exercised.fold(nodeId)(_.lastDescendantNodeId)

    who >= nodeId && who <= lastDescendantNodeId
  }

  private object CompanionImplicits {
    implicit val exceptionTesterCompanion: ContractCompanion.WithoutKey[
      ExceptionTester.Contract,
      ExceptionTester.ContractId,
      ExceptionTester,
    ] = ExceptionTester.COMPANION
  }
}
