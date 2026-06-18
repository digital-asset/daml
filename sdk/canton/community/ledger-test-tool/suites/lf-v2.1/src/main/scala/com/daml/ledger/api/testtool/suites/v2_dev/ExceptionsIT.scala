// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_dev

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.experimental.da.types
import com.daml.ledger.test.java.experimental.exceptions.{
  Divulger,
  ExceptionTester,
  Fetcher,
  Informer,
  RollbackNestingHelper,
  WithKey,
  WithKeyDelegate,
  WithSimpleKey,
}
import com.digitalasset.base.error.{ErrorCategory, ErrorCode}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.error.groups.{CommandExecutionErrors, ConsistencyErrors}

import java.lang
import scala.jdk.CollectionConverters.*

final class ExceptionsIT extends LedgerTestSuite {
  import ExceptionsIT.CompanionImplicits.*

  test(
    "ExUncaught",
    "Uncaught exception returns INVALID_ARGUMENT",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      failure <- ledger.exercise(party, t.exerciseThrowUncaught()).mustFail("Unhandled exception")
    } yield {
      assertGrpcError(
        failure,
        new ErrorCode(
          CommandExecutionErrors.Interpreter.FailureStatus.id,
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        )(
          CommandExecutionErrors.Interpreter.FailureStatus.parent
        ) {},
        Some("UNHANDLED_EXCEPTION"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "ExCaughtBasic",
    "Exceptions can be caught",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      tree <- ledger.exercise(party, t.exerciseThrowCaught())
    } yield {
      assertLength(s"1 successful exercise", 1, exercisedEvents(tree)).discard
    }
  })

  test(
    "ExCaughtNested",
    "Exceptions can be caught when thrown from a nested try block",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      tree <- ledger.exercise(party, t.exerciseNestedCatch())
    } yield {
      assertLength(s"1 successful exercise", 1, exercisedEvents(tree)).discard
    }
  })

  test(
    "ExRollbackActiveFetch",
    "Rollback node depends on activeness of contract in a fetch",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      tFetch <- ledger.create(party, new ExceptionTester(party))
      _ <- ledger.exercise(party, t.exerciseRollbackFetch(tFetch))
      _ <- ledger.exercise(party, tFetch.exerciseArchive())
      failure <- ledger
        .exercise(party, t.exerciseRollbackFetch(tFetch))
        .mustFail("contract is archived")
    } yield {
      assertGrpcError(
        failure,
        ConsistencyErrors.ContractNotFound,
        Some("Contract could not be found"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "ExRollbackActiveExerciseConsuming",
    "Rollback node depends on activeness of contract in a consuming exercise",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      tExercise <- ledger.create(party, new ExceptionTester(party))
      _ <- ledger.exercise(party, t.exerciseRollbackConsuming(tExercise))
      _ <- ledger.exercise(party, tExercise.exerciseArchive())
      failure <- ledger
        .exercise(party, t.exerciseRollbackConsuming(tExercise))
        .mustFail("contract is archived")
    } yield {
      assertGrpcError(
        failure,
        ConsistencyErrors.ContractNotFound,
        Some("Contract could not be found"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "ExRollbackActiveExerciseNonConsuming",
    "Rollback node depends on activeness of contract in a non-consuming exercise",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      tExercise <- ledger.create(party, new ExceptionTester(party))
      _ <- ledger.exercise(party, t.exerciseRollbackNonConsuming(tExercise))
      _ <- ledger.exercise(party, tExercise.exerciseArchive())
      failure <- ledger
        .exercise(party, t.exerciseRollbackNonConsuming(tExercise))
        .mustFail("contract is archived")
    } yield {
      assertGrpcError(
        failure,
        ConsistencyErrors.ContractNotFound,
        Some("Contract could not be found"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "ExRolledbackArchiveConsuming",
    "Rolled back archive does not block consuming exercise",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      withKey <- ledger.create(party, new WithSimpleKey(party))
      _ <- ledger.exercise(party, t.exerciseRolledbackArchiveConsuming(withKey))
    } yield ()
  })

  test(
    "ExRolledbackArchiveNonConsuming",
    "Rolled back archive does not block non-consuming exercise",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      withKey <- ledger.create(party, new WithSimpleKey(party))
      _ <- ledger.exercise(party, t.exerciseRolledbackArchiveNonConsuming(withKey))
    } yield ()
  })

  test(
    "ExRolledbackKeyCreation",
    "Rolled back key creation does not block creation of the same key",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      _ <- ledger.exercise(party, t.exerciseRolledbackDuplicateKey())
    } yield ()
  })

  test(
    "ExRollbackDuplicateKeyCreated",
    "Rollback fails once contract with same key is created",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      _ <- ledger.exercise(party, t.exerciseDuplicateKey())
      _ <- ledger.create(party, new WithSimpleKey(party))
      failure <- ledger.exercise(party, t.exerciseDuplicateKey()).mustFail("duplicate key")
    } yield {
      assertGrpcError(
        failure,
        ConsistencyErrors.DuplicateContractKey,
        Some("DuplicateKey"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "ExRollbackDuplicateKeyArchived",
    "Rollback succeeds once contract with same key is archived",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      withKey <- ledger.create(party, new WithSimpleKey(party))
      failure <- ledger.exercise(party, t.exerciseDuplicateKey()).mustFail("duplicate key")
      _ = assertGrpcError(
        failure,
        ConsistencyErrors.DuplicateContractKey,
        Some("DuplicateKey"),
        checkDefiniteAnswerMetadata = true,
      )
      _ <- ledger.exercise(party, withKey.exerciseArchive())
      _ <- ledger.exercise(party, t.exerciseDuplicateKey())
    } yield ()
  })

  test(
    "ExRollbackKeyFetchCreated",
    "Rollback with key fetch fails once contract is archived",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      withKey <- ledger.create(party, new WithSimpleKey(party))
      _ <- ledger.exercise(party, t.exerciseFetchKey())
      _ <- ledger.exercise(party, withKey.exerciseArchive())
      failure <- ledger.exercise(party, t.exerciseFetchKey()).mustFail("couldn't find key")
    } yield {
      assertGrpcError(
        failure,
        CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("couldn't find key"),
        checkDefiniteAnswerMetadata = true,
      )
      ()
    }
  })

  test(
    "ExRollbackKeyFetchArchived",
    "Rollback with key fetch succeeds once contract is created",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      failure <- ledger.exercise(party, t.exerciseFetchKey()).mustFail("contract not found")
      _ = assertGrpcError(
        failure,
        CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("couldn't find key"),
        checkDefiniteAnswerMetadata = true,
      )
      _ <- ledger.create(party, new WithSimpleKey(party))
      _ <- ledger.exercise(party, t.exerciseFetchKey())
    } yield ()
  })

  test(
    "ExRollbackHidden",
    "Create and exercise in rollback node is not exposed on ledger API",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      tree <- ledger.exercise(party, t.exerciseRollbackCreate())
    } yield {
      // Create node should not be included
      assertLength(s"no creates", 0, createdEvents(tree)).discard
      // Only the root exercise should be included not the one in the rollback node.
      val exercise = assertSingleton(s"1 exercise", exercisedEvents(tree))
      assert(exercise.choice == "RollbackCreate", "Choice name mismatch")
      ()
    }
  })

  test(
    "ExRollbackProjectionNormalization",
    "Projection normalization is correctly applied",
    allocate(SingleParty, SingleParty, SingleParty),
  )(implicit ec => {
    // We cannot test projection & normalization directly via the ledger API
    // since rollback nodes are erased so this test only ensures
    // that the code paths for this are exercised and do not
    // throw errors.
    case p @ Participants(
          Participant(aLedger, Seq(aParty)),
          Participant(bLedger, Seq(bParty)),
          Participant(cLedger, Seq(cParty)),
        ) =>
      for {
        abInformer <- aLedger.create(aParty, new Informer(aParty, List(bParty.getValue).asJava))
        acInformer <- aLedger.create(aParty, new Informer(aParty, List(cParty.getValue).asJava))
        abcInformer <- aLedger.create(
          aParty,
          new Informer(aParty, List(bParty, cParty).map(_.getValue).asJava),
        )
        keyDelegate <- bLedger.create(bParty, new WithKeyDelegate(aParty, bParty))
        _ <- p.synchronize
        tester <- aLedger.create(aParty, new ExceptionTester(aParty))
        _ <- aLedger.exercise(
          aParty,
          tester.exerciseProjectionNormalization(
            bParty,
            keyDelegate,
            abInformer,
            acInformer,
            abcInformer,
          ),
        )
      } yield ()
  })

  test(
    "ExRollbackProjectionNesting",
    "Nested rollback nodes are handled properly",
    allocate(SingleParty, SingleParty, SingleParty),
  )(implicit ec => {
    // We cannot test projection & normalization directly via the ledger API
    // since rollback nodes are erased so this test only ensures
    // that the code paths for this are exercised and do not
    // throw errors.
    case p @ Participants(
          Participant(aLedger, Seq(aParty)),
          Participant(bLedger, Seq(bParty)),
          Participant(cLedger, Seq(cParty)),
        ) =>
      for {
        keyDelegate <- bLedger.create(bParty, new WithKeyDelegate(aParty, bParty))
        nestingHelper <- cLedger.create(cParty, new RollbackNestingHelper(aParty, bParty, cParty))
        _ <- p.synchronize
        tester <- aLedger.create(aParty, new ExceptionTester(aParty))
        _ <- aLedger.exercise(
          aParty,
          tester.exerciseProjectionNesting(bParty, keyDelegate, nestingHelper),
        )
      } yield ()
  })

  test(
    "ExCKRollbackGlobalArchivedLookup",
    "Create with key succeeds after archive & rolledback negative lookup",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(
          Participant(ledger, Seq(party))
        ) =>
      for {
        t <- ledger.create(party, new ExceptionTester(party))
        withKey <- ledger.create(party, new WithSimpleKey(party))
        _ <- ledger.exercise(party, t.exerciseRollbackGlobalArchivedLookup(withKey))
      } yield ()
  })

  test(
    "ExCKRollbackGlobalArchivedCreate",
    "Create with key succeeds after archive & rolledback negative lookup",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(
          Participant(ledger, Seq(party))
        ) =>
      for {
        t <- ledger.create(party, new ExceptionTester(party))
        withKey <- ledger.create(party, new WithSimpleKey(party))
        _ <- ledger.exercise(party, t.exerciseRollbackGlobalArchivedCreate(withKey))
      } yield ()
  })

  test(
    "ExRollbackCreate",
    "Archiving a contract created within a rolled-back try-catch block, fails",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new ExceptionTester(party))
      failure <- ledger
        .exercise(party, t.exerciseRollbackCreateBecomesInactive())
        .mustFail("contract is inactive")
    } yield {
      assertGrpcError(
        failure,
        ConsistencyErrors.ContractNotFound,
        Some("Contract could not be found"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "ExRollbackExerciseCreateLookup",
    "Lookup a contract Archiving a contract created within a rolled-back try-catch block, fails",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      helper <- ledger.create(party, new ExceptionTester(party))
      withKey <- ledger.create(party, new WithSimpleKey(party))
      _ <- ledger.exercise(party, helper.exerciseRollbackExerciseCreateLookup(withKey))
    } yield ()
  })

}

object ExceptionsIT {
  private object CompanionImplicits {
    implicit val exceptionTesterCompanion: ContractCompanion.WithoutKey[
      ExceptionTester.Contract,
      ExceptionTester.ContractId,
      ExceptionTester,
    ] = ExceptionTester.COMPANION
    implicit val withSimpleKeyCompanion: ContractCompanion.WithKey[
      WithSimpleKey.Contract,
      WithSimpleKey.ContractId,
      WithSimpleKey,
      String,
    ] = WithSimpleKey.COMPANION
    implicit val withKeyCompanion: ContractCompanion.WithKey[
      WithKey.Contract,
      WithKey.ContractId,
      WithKey,
      types.Tuple2[String, lang.Long],
    ] = WithKey.COMPANION
    implicit val informerCompanion
        : ContractCompanion.WithoutKey[Informer.Contract, Informer.ContractId, Informer] =
      Informer.COMPANION
    implicit val withKeyDelegateCompanion: ContractCompanion.WithoutKey[
      WithKeyDelegate.Contract,
      WithKeyDelegate.ContractId,
      WithKeyDelegate,
    ] = WithKeyDelegate.COMPANION
    implicit val divulgerCompanion
        : ContractCompanion.WithoutKey[Divulger.Contract, Divulger.ContractId, Divulger] =
      Divulger.COMPANION
    implicit val fetcherCompanion
        : ContractCompanion.WithoutKey[Fetcher.Contract, Fetcher.ContractId, Fetcher] =
      Fetcher.COMPANION
    implicit val rollbackNestingHelperCompanion: ContractCompanion.WithoutKey[
      RollbackNestingHelper.Contract,
      RollbackNestingHelper.ContractId,
      RollbackNestingHelper,
    ] = RollbackNestingHelper.COMPANION
  }
}
