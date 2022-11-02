// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.util.regex.Pattern
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.client.binding.Primitive.ContractId
import com.daml.ledger.test.model.DA.Types.Tuple2
import com.daml.ledger.test.model.Test
import com.daml.ledger.test.model.Test.CallablePayout
import com.daml.ledger.errors.LedgerApiErrors
import scalaz.Tag

final class ContractKeysIT extends LedgerTestSuite {
  test(
    "CKNoContractKey",
    "There should be no contract key if the template does not specify one",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha @ _, receiver), Participant(beta, giver)) =>
      for {
        _ <- beta.create(giver, CallablePayout(giver, receiver))
        transactions <- beta.flatTransactions(giver, receiver)
      } yield {
        val contract = assertSingleton("NoContractKey", transactions.flatMap(createdEvents))
        assert(
          contract.getContractKey.sum.isEmpty,
          s"The key is not empty: ${contract.getContractKey}",
        )
      }
  })

  test(
    "CKFetchOrLookup",
    "Divulged contracts cannot be fetched or looked up by key by non-stakeholders",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, owner), Participant(beta, delegate)) =>
    import Test.{Delegated, Delegation, ShowDelegated}
    val key = alpha.nextKeyId()
    for {
      // create contracts to work with
      delegated <- alpha.create(owner, Delegated(owner, key))
      delegation <- alpha.create(owner, Delegation(owner, delegate))
      showDelegated <- alpha.create(owner, ShowDelegated(owner, delegate))

      // divulge the contract
      _ <- alpha.exercise(owner, showDelegated.exerciseShowIt(delegated))

      // fetch delegated
      _ <- eventually("exerciseFetchDelegated") {
        beta.exercise(delegate, delegation.exerciseFetchDelegated(delegated))
      }

      // fetch by key should fail during interpretation
      // Reason: Only stakeholders see the result of fetchByKey, beta is neither stakeholder nor divulgee
      fetchFailure <- beta
        .exercise(delegate, delegation.exerciseFetchByKeyDelegated(owner, key))
        .mustFail("fetching by key with a party that cannot see the contract")

      // lookup by key delegation should fail during validation
      // Reason: During command interpretation, the lookup did not find anything due to privacy rules,
      // but validation determined that this result is wrong as the contract is there.
      lookupByKeyFailure <- beta
        .exercise(delegate, delegation.exerciseLookupByKeyDelegated(owner, key))
        .mustFail("looking up by key with a party that cannot see the contract")
    } yield {
      assertGrpcError(
        fetchFailure,
        LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("couldn't find key"),
      )
      assertGrpcErrorRegex(
        lookupByKeyFailure,
        LedgerApiErrors.ConsistencyErrors.InconsistentContractKey,
        Some(Pattern.compile("Inconsistent|Contract key lookup with different results")),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CKNoFetchUndisclosed",
    "Contract Keys should reject fetching an undisclosed contract",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, owner), Participant(beta, delegate)) =>
    import Test.{Delegated, Delegation}
    val key = alpha.nextKeyId()
    for {
      // create contracts to work with
      delegated <- alpha.create(owner, Delegated(owner, key))
      delegation <- alpha.create(owner, Delegation(owner, delegate))

      _ <- synchronize(alpha, beta)

      // fetch should fail
      // Reason: contract not divulged to beta
      fetchFailure <- beta
        .exercise(delegate, delegation.exerciseFetchDelegated(delegated))
        .mustFail("fetching a contract with a party that cannot see it")

      // fetch by key should fail
      // Reason: Only stakeholders see the result of fetchByKey, beta is only a divulgee
      fetchByKeyFailure <- beta
        .exercise(delegate, delegation.exerciseFetchByKeyDelegated(owner, key))
        .mustFail("fetching a contract by key with a party that cannot see it")

      // lookup by key should fail
      // Reason: During command interpretation, the lookup did not find anything due to privacy rules,
      // but validation determined that this result is wrong as the contract is there.
      lookupByKeyFailure <- beta
        .exercise(delegate, delegation.exerciseLookupByKeyDelegated(owner, key))
        .mustFail("looking up a contract by key with a party that cannot see it")
    } yield {
      assertGrpcError(
        fetchFailure,
        LedgerApiErrors.ConsistencyErrors.ContractNotFound,
        Some("Contract could not be found"),
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        fetchByKeyFailure,
        LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("couldn't find key"),
      )
      assertGrpcErrorRegex(
        lookupByKeyFailure,
        LedgerApiErrors.ConsistencyErrors.InconsistentContractKey,
        Some(Pattern.compile("Inconsistent|Contract key lookup with different results")),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CKMaintainerScoped",
    "Contract keys should be scoped by maintainer",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    import Test.{MaintainerNotSignatory, TextKey, TextKeyOperations}
    val key1 = alpha.nextKeyId()
    val key2 = alpha.nextKeyId()
    val unknownKey = alpha.nextKeyId()

    for {
      // create contracts to work with
      tk1 <- alpha.create(alice, TextKey(alice, key1, List(bob)))
      tk2 <- alpha.create(alice, TextKey(alice, key2, List(bob)))
      aliceTKO <- alpha.create(alice, TextKeyOperations(alice))
      bobTKO <- beta.create(bob, TextKeyOperations(bob))

      _ <- synchronize(alpha, beta)

      // creating a contract with a duplicate key should fail
      duplicateKeyFailure <- alpha
        .create(alice, TextKey(alice, key1, List(bob)))
        .mustFail("creating a contract with a duplicate key")

      // trying to lookup an unauthorized key should fail
      bobLooksUpTextKeyFailure <- beta
        .exercise(bob, bobTKO.exerciseTKOLookup(Tuple2(alice, key1), Some(tk1)))
        .mustFail("looking up a contract with an unauthorized key")

      // trying to lookup an unauthorized non-existing key should fail
      bobLooksUpBogusTextKeyFailure <- beta
        .exercise(bob, bobTKO.exerciseTKOLookup(Tuple2(alice, unknownKey), None))
        .mustFail("looking up a contract with an unauthorized, non-existing key")

      // successful, authorized lookup
      _ <- alpha.exercise(alice, aliceTKO.exerciseTKOLookup(Tuple2(alice, key1), Some(tk1)))

      // successful fetch
      _ <- alpha.exercise(alice, aliceTKO.exerciseTKOFetch(Tuple2(alice, key1), tk1))

      // successful, authorized lookup of non-existing key
      _ <- alpha.exercise(alice, aliceTKO.exerciseTKOLookup(Tuple2(alice, unknownKey), None))

      // failing fetch
      aliceFailedFetch <- alpha
        .exercise(alice, aliceTKO.exerciseTKOFetch(Tuple2(alice, unknownKey), tk1))
        .mustFail("fetching a contract by an unknown key")

      // now we exercise the contract, thus archiving it, and then verify
      // that we cannot look it up anymore
      _ <- alpha.exercise(alice, tk1.exerciseTextKeyChoice())
      _ <- alpha.exercise(alice, aliceTKO.exerciseTKOLookup(Tuple2(alice, key1), None))

      // lookup the key, consume it, then verify we cannot look it up anymore
      _ <- alpha.exercise(
        alice,
        aliceTKO.exerciseTKOConsumeAndLookup(tk2, Tuple2(alice, key2)),
      )

      // failing create when a maintainer is not a signatory
      maintainerNotSignatoryFailed <- alpha
        .create(alice, MaintainerNotSignatory(alice, bob))
        .mustFail("creating a contract where a maintainer is not a signatory")
    } yield {
      assertGrpcErrorRegex(
        duplicateKeyFailure,
        LedgerApiErrors.ConsistencyErrors.DuplicateContractKey,
        Some(Pattern.compile("Inconsistent|contract key is not unique")),
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        bobLooksUpTextKeyFailure,
        LedgerApiErrors.CommandExecution.Interpreter.AuthorizationError,
        Some("requires authorizers"),
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        bobLooksUpBogusTextKeyFailure,
        LedgerApiErrors.CommandExecution.Interpreter.AuthorizationError,
        Some("requires authorizers"),
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        aliceFailedFetch,
        LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("couldn't find key"),
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        maintainerNotSignatoryFailed,
        LedgerApiErrors.CommandExecution.Interpreter.AuthorizationError,
        Some("are not a subset of the signatories"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test("CKRecreate", "Contract keys can be recreated in single transaction", allocate(SingleParty))(
    implicit ec => { case Participants(Participant(ledger, owner)) =>
      import Test.Delegated
      val key = ledger.nextKeyId()
      for {
        delegated1TxTree <- ledger
          .submitAndWaitForTransactionTree(
            ledger.submitAndWaitRequest(owner, Delegated(owner, key).create.command)
          )
          .map(_.getTransaction)
        delegated1Id = com.daml.ledger.client.binding.Primitive
          .ContractId[Delegated](delegated1TxTree.eventsById.head._2.getCreated.contractId)

        delegated2TxTree <- ledger.exercise(owner, delegated1Id.exerciseRecreate())
      } yield {
        assert(delegated2TxTree.eventsById.size == 2)
        val event = delegated2TxTree.eventsById.filter(_._2.kind.isCreated).head._2
        assert(
          Tag.unwrap(delegated1Id) != event.getCreated.contractId,
          "New contract was not created",
        )
        assert(
          event.getCreated.contractKey == delegated1TxTree.eventsById.head._2.getCreated.contractKey,
          "Contract keys did not match",
        )

      }
    }
  )

  test(
    "CKTransients",
    "Contract keys created by transient contracts are properly archived",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, owner)) =>
    import Test.{Delegated, Delegation}
    val key = ledger.nextKeyId()
    val key2 = ledger.nextKeyId()

    for {
      delegation <- ledger.create(owner, Delegation(owner, owner))
      delegated <- ledger.create(owner, Delegated(owner, key))

      failedFetch <- ledger
        .exercise(owner, delegation.exerciseFetchByKeyDelegated(owner, key2))
        .mustFail("fetching a contract with an unknown key")

      // Create a transient contract with a key that is created and archived in same transaction.
      _ <- ledger.exercise(owner, delegated.exerciseCreateAnotherAndArchive(key2))

      // Try it again, expecting it to succeed.
      _ <- ledger.exercise(owner, delegated.exerciseCreateAnotherAndArchive(key2))

    } yield {
      assertGrpcError(
        failedFetch,
        LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("couldn't find key"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CKExposedByTemplate",
    "The contract key should be exposed if the template specifies one",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import Test.TextKey
    val expectedKey = ledger.nextKeyId()
    for {
      _ <- ledger.create(party, TextKey(party, expectedKey, List.empty))
      transactions <- ledger.flatTransactions(party)
    } yield {
      val contract = assertSingleton("CKExposedByTemplate", transactions.flatMap(createdEvents))
      assertEquals(
        "CKExposedByTemplate",
        contract.getContractKey.getRecord.fields,
        Seq(
          RecordField("_1", Some(Value(Value.Sum.Party(Tag.unwrap(party))))),
          RecordField("_2", Some(Value(Value.Sum.Text(expectedKey)))),
        ),
      )
    }
  })

  test(
    "CKExerciseByKey",
    "Exercising by key should be possible only when the corresponding contract is available",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import Test.TextKey
    val keyString = ledger.nextKeyId()
    val expectedKey = Value(
      Value.Sum.Record(
        Record(
          fields = Seq(
            RecordField("_1", Some(Value(Value.Sum.Party(Tag.unwrap(party))))),
            RecordField("_2", Some(Value(Value.Sum.Text(keyString)))),
          )
        )
      )
    )
    for {
      failureBeforeCreation <- ledger
        .exerciseByKey(
          party,
          TextKey.id,
          expectedKey,
          "TextKeyChoice",
          Value(Value.Sum.Record(Record())),
        )
        .mustFail("exercising before creation")
      _ <- ledger.create(party, TextKey(party, keyString, List.empty))
      _ <- ledger.exerciseByKey(
        party,
        TextKey.id,
        expectedKey,
        "TextKeyChoice",
        Value(Value.Sum.Record(Record())),
      )
      failureAfterConsuming <- ledger
        .exerciseByKey(
          party,
          TextKey.id,
          expectedKey,
          "TextKeyChoice",
          Value(Value.Sum.Record(Record())),
        )
        .mustFail("exercising after consuming")
    } yield {
      assertGrpcError(
        failureBeforeCreation,
        LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("dependency error: couldn't find key"),
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        failureAfterConsuming,
        LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("dependency error: couldn't find key"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CKLocalKeyVisibility",
    "Visibility should be checked for fetch-by-key/lookup-by-key of contracts created in the current transaction",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger1, party1), Participant(ledger2, party2)) =>
      import Test.LocalKeyVisibilityOperations
      for {
        ops <- ledger1.create(party1, LocalKeyVisibilityOperations(party1, party2))
        _ <- synchronize(ledger1, ledger2)
        failedLookup <- ledger2
          .exercise(party2, ops.exerciseLocalLookup())
          .mustFail("lookup not visible")
        failedFetch <- ledger2
          .exercise(party2, ops.exerciseLocalFetch())
          .mustFail("fetch not visible")
        _ <- ledger2.exercise(
          actAs = List(party2),
          readAs = List(party1),
          ops.exerciseLocalLookup(),
        )
        _ <- ledger2.exercise(
          actAs = List(party2),
          readAs = List(party1),
          ops.exerciseLocalFetch(),
        )
      } yield {
        assertGrpcError(
          failedLookup,
          LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError,
          Some("not visible"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          failedFetch,
          LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError,
          Some("not visible"),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  test(
    "CKDisclosedContractKeyReusabilityBasic",
    "Subsequent disclosed contracts can use the same contract key",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger1, party1), Participant(ledger2, party2)) =>
      import Test.{WithKey, WithKeyCreator, WithKeyFetcher}
      for {
        // Create a helper contract and exercise a choice creating and disclosing a WithKey contract
        creator1 <- ledger1.create(party1, WithKeyCreator(party1, party2))
        withKey1 <- ledger1.exerciseAndGetContract[WithKey](
          party1,
          creator1.exerciseWithKeyCreator_DiscloseCreate(party1),
        )

        // Verify that the withKey1 contract is usable by the party2
        fetcher <- ledger1.create(party1, WithKeyFetcher(party1, party2))

        _ <- synchronize(ledger1, ledger2)

        _ <- ledger2.exercise(party2, fetcher.exerciseWithKeyFetcher_Fetch(withKey1))

        // Archive the disclosed contract
        _ <- ledger1.exercise(party1, withKey1.exerciseArchive())

        _ <- synchronize(ledger1, ledger2)

        // Verify that fetching the contract is no longer possible after it was archived
        _ <- ledger2
          .exercise(party2, fetcher.exerciseWithKeyFetcher_Fetch(withKey1))
          .mustFail("fetching an archived contract")

        // Repeat the same steps for the second time
        creator2 <- ledger1.create(party1, WithKeyCreator(party1, party2))
        _ <- ledger1.exerciseAndGetContract[WithKey](
          party1,
          creator2.exerciseWithKeyCreator_DiscloseCreate(party1),
        )

        // Synchronize to verify that the second participant is working
        _ <- synchronize(ledger1, ledger2)
      } yield ()
  })

  test(
    "CKDisclosedContractKeyReusabilityAsSubmitter",
    "Subsequent disclosed contracts can use the same contract key (disclosure because of submitting)",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger1, party1), Participant(ledger2, party2)) =>
      import Test.{WithKey, WithKeyCreatorAlternative}
      for {
        // Create a helper contract and exercise a choice creating and disclosing a WithKey contract
        creator1 <- ledger1.create(party1, WithKeyCreatorAlternative(party1, party2))

        _ <- synchronize(ledger1, ledger2)

        _ <- ledger2.exercise(
          party2,
          creator1.exerciseWithKeyCreatorAlternative_DiscloseCreate(),
        )

        _ <- synchronize(ledger1, ledger2)

        Seq(withKey1Event) <- ledger1.activeContractsByTemplateId(List(WithKey.id), party1)
        withKey1 = ContractId.apply[WithKey](withKey1Event.contractId)
        // Archive the disclosed contract
        _ <- ledger1.exercise(party1, withKey1.exerciseArchive())

        _ <- synchronize(ledger1, ledger2)

        // Repeat the same steps for the second time
        _ <- ledger2.exercise(
          party2,
          creator1.exerciseWithKeyCreatorAlternative_DiscloseCreate(),
        )

        _ <- synchronize(ledger1, ledger2)
      } yield ()
  })

  test(
    "CKGlocalKeyVisibility",
    "Contract keys should be visible",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {

      // create contracts to work with
      cid <- ledger.create(alice, Test.WithKey(alice))

      // double check its key can be found if visible
      _ <- ledger.submit(
        ledger.submitRequest(
          alice,
          Test.WithKey.key(alice).exerciseWithKey_NoOp(alice).command,
        )
      )

      // divulge the contract
      helper <- ledger.create(bob, Test.WithKeyDivulgenceHelper(bob, alice))
      _ <- ledger.exercise(alice, helper.exerciseWithKeyDivulgenceHelper_Fetch(cid))

      // double check it is properly divulged
      _ <- ledger.exercise(bob, cid.exerciseWithKey_NoOp(bob))

      request = ledger.submitRequest(
        bob,
        // exercise by key the contract
        Test.WithKey.key(alice).exerciseWithKey_NoOp(bob).command,
      )
      failure1 <- ledger.submit(request).mustFail("exercise of a non visible key")

      request = ledger.submitRequest(
        bob,
        // bring the contract in the engine cache
        cid.exerciseWithKey_NoOp(bob).command,
        // exercise by key the contract
        Test.WithKey.key(alice).exerciseWithKey_NoOp(bob).command,
      )
      failure2 <- ledger.submit(request).mustFail("exercise of a non visible key")

    } yield {
      List(failure1, failure2).foreach { failure =>
        val results = LazyList(
          LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError ->
            "key of contract not visible",
          LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound ->
            "couldn't find key",
        ).map { case (errorCode, message) =>
          scala.util.Try(
            assertGrpcError(
              failure,
              errorCode,
              Some(message),
              checkDefiniteAnswerMetadata = true,
            )
          )
        }
        results.collectFirst { case scala.util.Success(value) => value }.getOrElse(results.head.get)
      }
    }
  })

}
