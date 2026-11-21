// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_dev

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.v2.commands.DisclosedContract
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
  TransactionFormat,
}
import com.daml.ledger.api.v2.value.{RecordField, Value}
import com.daml.ledger.javaapi.data.{DamlRecord, Party, Text}
import com.daml.ledger.test.java.experimental.da.types.Tuple2
import com.daml.ledger.test.java.experimental.test.{
  Delegated,
  Delegation,
  LocalKeyVisibilityOperations,
  MaintainerNotSignatory,
  ShowDelegated,
  TextKey,
  TextKeyOperations,
  WithKey,
  WithKeyCreatorAlternative,
}
import com.daml.ledger.test.java.model.test.CallablePayout
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.ledger.error.groups.{CommandExecutionErrors, ConsistencyErrors}

import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

final class ContractKeysIT extends LedgerTestSuite {
  import ContractKeysCompanionImplicits.*

  test(
    "CKNoContractKey",
    "There should be no contract key if the template does not specify one",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha @ _, Seq(receiver)), Participant(beta, Seq(giver))) =>
      for {
        _ <- beta.create(giver, new CallablePayout(giver, receiver))
        transactions <- beta.transactions(AcsDelta, giver, receiver)
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
  )(implicit ec => {
    case Participants(Participant(alpha, Seq(owner)), Participant(beta, Seq(delegate))) =>
      val key = alpha.nextKeyId()
      for {
        // create contracts to work with
        delegated <- alpha.create(owner, new Delegated(owner, key))
        delegation <- alpha.create(owner, new Delegation(owner, delegate))
        showDelegated <- alpha.create(owner, new ShowDelegated(owner, delegate))

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
          CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound,
          Some("couldn't find key"),
        )
        assertGrpcErrorRegex(
          lookupByKeyFailure,
          ConsistencyErrors.InconsistentContractKey,
          Some(Pattern.compile("Inconsistent|Contract key lookup with different results")),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  test(
    "CKNoFetchUndisclosed",
    "Contract Keys should reject fetching an undisclosed contract",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(owner)), Participant(beta, Seq(delegate))) =>
      val key = alpha.nextKeyId()
      for {
        // create contracts to work with
        delegated <- alpha.create(owner, new Delegated(owner, key))
        delegation <- alpha.create(owner, new Delegation(owner, delegate))

        _ <- p.synchronize

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
          ConsistencyErrors.ContractNotFound,
          Some("Contract could not be found"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          fetchByKeyFailure,
          CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound,
          Some("couldn't find key"),
        )
        assertGrpcErrorRegex(
          lookupByKeyFailure,
          ConsistencyErrors.InconsistentContractKey,
          Some(Pattern.compile("Inconsistent|Contract key lookup with different results")),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  test(
    "CKMaintainerScoped",
    "Contract keys should be scoped by maintainer",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(alice)), Participant(beta, Seq(bob))) =>
      val key1 = alpha.nextKeyId()
      val key2 = alpha.nextKeyId()
      val unknownKey = alpha.nextKeyId()

      for {
        // create contracts to work with
        tk1 <- alpha.create(alice, new TextKey(alice, key1, List(bob.getValue).asJava))
        tk2 <- alpha.create(alice, new TextKey(alice, key2, List(bob.getValue).asJava))
        aliceTKO <- alpha.create(alice, new TextKeyOperations(alice))
        bobTKO <- beta.create(bob, new TextKeyOperations(bob))

        _ <- p.synchronize

        // creating a contract with a duplicate key should fail
        duplicateKeyFailure <- alpha
          .create(alice, new TextKey(alice, key1, List(bob.getValue).asJava))
          .mustFail("creating a contract with a duplicate key")

        // trying to lookup an unauthorized key should fail
        bobLooksUpTextKeyFailure <- beta
          .exercise(bob, bobTKO.exerciseTKOLookup(new Tuple2(alice, key1), Some(tk1).toJava))
          .mustFail("looking up a contract with an unauthorized key")

        // trying to lookup an unauthorized non-existing key should fail
        bobLooksUpBogusTextKeyFailure <- beta
          .exercise(bob, bobTKO.exerciseTKOLookup(new Tuple2(alice, unknownKey), None.toJava))
          .mustFail("looking up a contract with an unauthorized, non-existing key")

        // successful, authorized lookup
        _ <- alpha.exercise(
          alice,
          aliceTKO.exerciseTKOLookup(new Tuple2(alice, key1), Some(tk1).toJava),
        )

        // successful fetch
        _ <- alpha.exercise(alice, aliceTKO.exerciseTKOFetch(new Tuple2(alice, key1), tk1))

        // successful, authorized lookup of non-existing key
        _ <- alpha.exercise(
          alice,
          aliceTKO.exerciseTKOLookup(new Tuple2(alice, unknownKey), None.toJava),
        )

        // failing fetch
        aliceFailedFetch <- alpha
          .exercise(alice, aliceTKO.exerciseTKOFetch(new Tuple2(alice, unknownKey), tk1))
          .mustFail("fetching a contract by an unknown key")

        // now we exercise the contract, thus archiving it, and then verify
        // that we cannot look it up anymore
        _ <- alpha.exercise(alice, tk1.exerciseTextKeyChoice())
        _ <- alpha.exercise(alice, aliceTKO.exerciseTKOLookup(new Tuple2(alice, key1), None.toJava))

        // lookup the key, consume it, then verify we cannot look it up anymore
        _ <- alpha.exercise(
          alice,
          aliceTKO.exerciseTKOConsumeAndLookup(tk2, new Tuple2(alice, key2)),
        )

        // failing create when a maintainer is not a signatory
        maintainerNotSignatoryFailed <- alpha
          .create(alice, new MaintainerNotSignatory(alice, bob))
          .mustFail("creating a contract where a maintainer is not a signatory")
      } yield {
        assertGrpcErrorRegex(
          duplicateKeyFailure,
          ConsistencyErrors.DuplicateContractKey,
          Some(Pattern.compile("Inconsistent|contract key is not unique")),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          bobLooksUpTextKeyFailure,
          CommandExecutionErrors.Interpreter.AuthorizationError,
          Some("requires authorizers"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          bobLooksUpBogusTextKeyFailure,
          CommandExecutionErrors.Interpreter.AuthorizationError,
          Some("requires authorizers"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          aliceFailedFetch,
          CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound,
          Some("couldn't find key"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          maintainerNotSignatoryFailed,
          CommandExecutionErrors.Interpreter.AuthorizationError,
          Some("are not a subset of the signatories"),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  test("CKRecreate", "Contract keys can be recreated in single transaction", allocate(SingleParty))(
    implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
      val key = ledger.nextKeyId()
      for {
        delegated1Tx <- ledger
          .submitAndWaitForTransaction(
            ledger
              .submitAndWaitForTransactionRequest(owner, new Delegated(owner, key).create.commands)
          )
          .map(_.getTransaction)
        delegated1Id = new Delegated.ContractId(
          delegated1Tx.events.head.getCreated.contractId
        )

        delegated2TxTree <- ledger.exercise(owner, delegated1Id.exerciseRecreate())
      } yield {
        assert(delegated2TxTree.events.size == 2)
        val event = delegated2TxTree.events.filter(_.event.isCreated).head
        assert(
          delegated1Id.contractId != event.getCreated.contractId,
          "New contract was not created",
        )
        assert(
          event.getCreated.contractKey == delegated1Tx.events.head.getCreated.contractKey,
          "Contract keys did not match",
        )

      }
    }
  )

  test(
    "CKTransients",
    "Contract keys created by transient contracts are properly archived",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
    val key = ledger.nextKeyId()
    val key2 = ledger.nextKeyId()

    for {
      delegation <- ledger.create(owner, new Delegation(owner, owner))
      delegated <- ledger.create(owner, new Delegated(owner, key))

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
        CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("couldn't find key"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CKExposedByTemplate",
    "The contract key should be exposed if the template specifies one",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val expectedKey = ledger.nextKeyId()
    for {
      _ <- ledger.create(party, new TextKey(party, expectedKey, List.empty.asJava))
      transactions <- ledger.transactions(AcsDelta, party)
    } yield {
      val contract = assertSingleton("CKExposedByTemplate", transactions.flatMap(createdEvents))
      assertEquals(
        "CKExposedByTemplate",
        contract.getContractKey.getRecord.fields,
        Seq(
          RecordField("_1", Some(Value(Value.Sum.Party(party.getValue)))),
          RecordField("_2", Some(Value(Value.Sum.Text(expectedKey)))),
        ),
      )
    }
  })

  test(
    "CKExerciseByKey",
    "Exercising by key should be possible only when the corresponding contract is available",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val keyString = ledger.nextKeyId()
    val expectedKey = new DamlRecord(
      new DamlRecord.Field("_1", new Party(party.getValue)),
      new DamlRecord.Field("_2", new Text(keyString)),
    )
    for {
      failureBeforeCreation <- ledger
        .exerciseByKey(
          party,
          TextKey.TEMPLATE_ID_WITH_PACKAGE_ID,
          expectedKey,
          "TextKeyChoice",
          new DamlRecord(),
        )
        .mustFail("exercising before creation")
      _ <- ledger.create(party, new TextKey(party, keyString, List.empty.asJava))
      _ <- ledger.exerciseByKey(
        party,
        TextKey.TEMPLATE_ID_WITH_PACKAGE_ID,
        expectedKey,
        "TextKeyChoice",
        new DamlRecord(),
      )
      failureAfterConsuming <- ledger
        .exerciseByKey(
          party,
          TextKey.TEMPLATE_ID_WITH_PACKAGE_ID,
          expectedKey,
          "TextKeyChoice",
          new DamlRecord(),
        )
        .mustFail("exercising after consuming")
    } yield {
      assertGrpcError(
        failureBeforeCreation,
        CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("dependency error: couldn't find key"),
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        failureAfterConsuming,
        CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("dependency error: couldn't find key"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CKLocalLookupByKeyVisibility",
    "Visibility should not be checked for lookup-by-key of contracts created in the current transaction",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(ledger1, Seq(party1)), Participant(ledger2, Seq(party2))) =>
      for {
        ops: LocalKeyVisibilityOperations.ContractId <- ledger1.create(
          party1,
          new LocalKeyVisibilityOperations(party1, party2),
        )
        _ <- p.synchronize
        _ <- ledger2.exercise(party2, ops.exerciseLocalLookup())
      } yield ()
  })

  test(
    "CKLocalFetchByKeyVisibility",
    "Visibility should not be checked for fetch-by-key of contracts created in the current transaction",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(ledger1, Seq(party1)), Participant(ledger2, Seq(party2))) =>
      for {
        ops: LocalKeyVisibilityOperations.ContractId <- ledger1.create(
          party1,
          new LocalKeyVisibilityOperations(party1, party2),
        )
        _ <- p.synchronize
        _ <- ledger2.exercise(party2, ops.exerciseLocalFetch())
      } yield ()
  })

  test(
    "CKDisclosedContractKeyReusabilityAsSubmitter",
    "Subsequent disclosed contracts can use the same contract key (disclosure because of submitting)",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(ledger1, Seq(party1)), Participant(ledger2, Seq(party2))) =>
      for {
        // Create a helper contract and exercise a choice creating and disclosing a WithKey contract
        creator1: WithKeyCreatorAlternative.ContractId <- ledger1.create(
          party1,
          new WithKeyCreatorAlternative(party1, party2),
        )(WithKeyCreatorAlternative.COMPANION)

        _ <- p.synchronize

        _ <- ledger2.exercise(
          party2,
          creator1.exerciseWithKeyCreatorAlternative_DiscloseCreate(),
        )

        _ <- p.synchronize

        Seq(withKey1Event) <- ledger1.activeContractsByTemplateId(
          List(WithKey.TEMPLATE_ID),
          Some(Seq(party1)),
        )
        withKey1 = new WithKey.ContractId(withKey1Event.contractId)
        // Archive the disclosed contract
        _ <- ledger1.exercise(party1, withKey1.exerciseArchive())

        _ <- p.synchronize

        // Repeat the same steps for the second time
        _ <- ledger2.exercise(
          party2,
          creator1.exerciseWithKeyCreatorAlternative_DiscloseCreate(),
        )

        _ <- p.synchronize
      } yield ()
  })

  test(
    "CKGlocalKeyVisibility",
    "Contract keys should be visible",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    for {

      // create contracts to work with
      cid <- ledger.create(alice, new WithKey(alice))

      // double check its key can be found if visible
      _ <- ledger.submit(
        ledger.submitRequest(
          alice,
          WithKey.byKey(alice).exerciseWithKey_NoOp(alice).commands,
        )
      )

      end <- ledger.currentEnd()
      // explicitly disclose the contract
      withKeyTxs <- ledger.transactions(
        ledger.getTransactionsRequestWithEnd(
          transactionFormat = TransactionFormat(
            Some(
              EventFormat(
                filtersByParty = Map(
                  alice.getValue -> new Filters(
                    Seq(
                      CumulativeFilter(
                        IdentifierFilter.TemplateFilter(
                          TemplateFilter(
                            Some(WithKey.TEMPLATE_ID.toV1),
                            includeCreatedEventBlob = true,
                          )
                        )
                      )
                    )
                  )
                ),
                filtersForAnyParty = None,
                verbose = false,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          ),
          end = Some(end),
        )
      )
      withKeyCreatedEvent = createdEvents(withKeyTxs.head).head
      disclosedWithKey = DisclosedContract(
        templateId = withKeyCreatedEvent.templateId,
        contractId = withKeyCreatedEvent.contractId,
        createdEventBlob = withKeyCreatedEvent.createdEventBlob,
        synchronizerId = "",
      )

      // double check it is properly disclosed
      _ <- ledger.submitAndWait(
        ledger
          .submitAndWaitRequest(
            bob,
            cid.exerciseWithKey_NoOp(bob).commands,
          )
          .update(_.commands.disclosedContracts := Seq(disclosedWithKey))
      )

      // without explicit disclosure key lookup should fail
      request = ledger
        .submitRequest(
          bob,
          // exercise by key the contract
          WithKey.byKey(alice).exerciseWithKey_NoOp(bob).commands,
        )
      failure <- ledger
        .submit(request)
        .mustFail("exercise of a non visible key")

      // with explicit disclosure key lookup should succeed
      request = ledger
        .submitRequest(
          bob,
          // exercise by key the contract
          WithKey.byKey(alice).exerciseWithKey_NoOp(bob).commands,
        )
        .update(_.commands.disclosedContracts := Seq(disclosedWithKey))
      _ <- ledger.submit(request)

    } yield {
      assertGrpcError(
        failure,
        CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("couldn't find key"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

}
