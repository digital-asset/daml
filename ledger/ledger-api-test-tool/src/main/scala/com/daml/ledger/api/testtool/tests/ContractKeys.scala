// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.UUID

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.ledger.test_stable.DA.Types.Tuple2
import com.digitalasset.ledger.test_stable.Test.Delegated._
import com.digitalasset.ledger.test_stable.Test.Delegation._
import com.digitalasset.ledger.test_stable.Test.ShowDelegated._
import com.digitalasset.ledger.test_stable.Test.TextKey._
import com.digitalasset.ledger.test_stable.Test.TextKeyOperations._
import com.digitalasset.ledger.test_stable.Test._
import io.grpc.Status
import scalaz.Tag

final class ContractKeys(session: LedgerSession) extends LedgerTestSuite(session) {
  test(
    "CKFetchOrLookup",
    "Divulged contracts can be fetched or looked up by key",
    allocate(SingleParty, SingleParty),
  ) {
    case Participants(Participant(alpha, owner), Participant(beta, delegate)) =>
      val key = s"${UUID.randomUUID.toString}-key"
      for {
        // create contracts to work with
        delegated <- alpha.create(owner, Delegated(owner, key))
        delegation <- alpha.create(owner, Delegation(owner, delegate))
        showDelegated <- alpha.create(owner, ShowDelegated(owner, delegate))

        // divulge the contract
        _ <- alpha.exercise(owner, showDelegated.exerciseShowIt(_, delegated))

        // fetch delegated
        _ <- eventually {
          beta.exercise(delegate, delegation.exerciseFetchDelegated(_, delegated))
        }

        // fetch by key should fail during interpretation
        // Reason: Only stakeholders see the result of fetchByKey, beta is neither stakeholder nor divulgee
        fetchFailure <- beta
          .exercise(delegate, delegation.exerciseFetchByKeyDelegated(_, owner, key, None))
          .failed

        // lookup by key delegation is should fail during validation
        // Reason: During command interpretation, the lookup did not find anything due to privacy rules,
        // but validation determined that this result is wrong as the contract is there.
        lookupByKeyFailure <- beta
          .exercise(delegate, delegation.exerciseLookupByKeyDelegated(_, owner, key, None))
          .failed
      } yield {
        assertGrpcError(fetchFailure, Status.Code.INVALID_ARGUMENT, "couldn't find key")
        assertGrpcError(lookupByKeyFailure, Status.Code.INVALID_ARGUMENT, "InvalidLookup")
      }
  }

  test(
    "CKNoFetchUndisclosed",
    "Contract Keys should reject fetching an undisclosed contract",
    allocate(SingleParty, SingleParty),
  ) {
    case Participants(Participant(alpha, owner), Participant(beta, delegate)) =>
      val key = s"${UUID.randomUUID.toString}-key"
      for {
        // create contracts to work with
        delegated <- alpha.create(owner, Delegated(owner, key))
        delegation <- alpha.create(owner, Delegation(owner, delegate))

        _ <- synchronize(alpha, beta)

        // fetch should fail
        // Reason: contract not divulged to beta
        fetchFailure <- beta
          .exercise(delegate, delegation.exerciseFetchDelegated(_, delegated))
          .failed

        // fetch by key should fail
        // Reason: Only stakeholders see the result of fetchByKey, beta is only a divulgee
        fetchByKeyFailure <- beta
          .exercise(delegate, delegation.exerciseFetchByKeyDelegated(_, owner, key, None))
          .failed

        // lookup by key should fail
        // Reason: During command interpretation, the lookup did not find anything due to privacy rules,
        // but validation determined that this result is wrong as the contract is there.
        lookupByKeyFailure <- beta
          .exercise(delegate, delegation.exerciseLookupByKeyDelegated(_, owner, key, None))
          .failed
      } yield {
        assertGrpcError(
          fetchFailure,
          Status.Code.INVALID_ARGUMENT,
          "dependency error: couldn't find contract",
        )
        assertGrpcError(fetchByKeyFailure, Status.Code.INVALID_ARGUMENT, "couldn't find key")
        assertGrpcError(lookupByKeyFailure, Status.Code.INVALID_ARGUMENT, "InvalidLookup")
      }
  }

  test(
    "CKMaintainerScoped",
    "Contract keys should be scoped by maintainer",
    allocate(SingleParty, SingleParty),
  ) {
    case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
      val keyPrefix = UUID.randomUUID.toString
      val key1 = s"$keyPrefix-some-key"
      val key2 = s"$keyPrefix-some-other-key"
      val unknownKey = s"$keyPrefix-unknown-key"

      for {
        // create contracts to work with
        tk1 <- alpha.create(alice, TextKey(alice, key1, List(bob)))
        tk2 <- alpha.create(alice, TextKey(alice, key2, List(bob)))
        aliceTKO <- alpha.create(alice, TextKeyOperations(alice))
        bobTKO <- beta.create(bob, TextKeyOperations(bob))

        _ <- synchronize(alpha, beta)

        // creating a contract with a duplicate key should fail
        duplicateKeyFailure <- alpha.create(alice, TextKey(alice, key1, List(bob))).failed

        // trying to lookup an unauthorized key should fail
        bobLooksUpTextKeyFailure <- beta
          .exercise(bob, bobTKO.exerciseTKOLookup(_, Tuple2(alice, key1), Some(tk1)))
          .failed

        // trying to lookup an unauthorized non-existing key should fail
        bobLooksUpBogusTextKeyFailure <- beta
          .exercise(bob, bobTKO.exerciseTKOLookup(_, Tuple2(alice, unknownKey), None))
          .failed

        // successful, authorized lookup
        _ <- alpha.exercise(alice, aliceTKO.exerciseTKOLookup(_, Tuple2(alice, key1), Some(tk1)))

        // successful fetch
        _ <- alpha.exercise(alice, aliceTKO.exerciseTKOFetch(_, Tuple2(alice, key1), tk1))

        // successful, authorized lookup of non-existing key
        _ <- alpha.exercise(alice, aliceTKO.exerciseTKOLookup(_, Tuple2(alice, unknownKey), None))

        // failing fetch
        aliceFailedFetch <- alpha
          .exercise(alice, aliceTKO.exerciseTKOFetch(_, Tuple2(alice, unknownKey), tk1))
          .failed

        // now we exercise the contract, thus archiving it, and then verify
        // that we cannot look it up anymore
        _ <- alpha.exercise(alice, tk1.exerciseTextKeyChoice)
        _ <- alpha.exercise(alice, aliceTKO.exerciseTKOLookup(_, Tuple2(alice, key1), None))

        // lookup the key, consume it, then verify we cannot look it up anymore
        _ <- alpha.exercise(
          alice,
          aliceTKO.exerciseTKOConsumeAndLookup(_, tk2, Tuple2(alice, key2)),
        )

        // failing create when a maintainer is not a signatory
        maintainerNotSignatoryFailed <- alpha
          .create(alice, MaintainerNotSignatory(alice, bob))
          .failed
      } yield {
        assertGrpcError(duplicateKeyFailure, Status.Code.INVALID_ARGUMENT, "DuplicateKey")
        assertGrpcError(
          bobLooksUpTextKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          "requires authorizers",
        )
        assertGrpcError(
          bobLooksUpBogusTextKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          "requires authorizers",
        )
        assertGrpcError(aliceFailedFetch, Status.Code.INVALID_ARGUMENT, "couldn't find key")
        assertGrpcError(
          maintainerNotSignatoryFailed,
          Status.Code.INVALID_ARGUMENT,
          "are not a subset of the signatories",
        )
      }
  }

  test("CKRecreate", "Contract keys can be recreated in single transaction", allocate(SingleParty)) {
    case Participants(Participant(ledger, owner)) =>
      val key = s"${UUID.randomUUID.toString}-key"
      for {
        delegated1TxTree <- ledger
          .submitAndWaitRequest(owner, Delegated(owner, key).create.command)
          .flatMap(ledger.submitAndWaitForTransactionTree)
        delegated1Id = com.digitalasset.ledger.client.binding.Primitive
          .ContractId[Delegated](delegated1TxTree.eventsById.head._2.getCreated.contractId)

        delegated2TxTree <- ledger.exercise(owner, delegated1Id.exerciseRecreate)
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

  test(
    "CKTransients",
    "Contract keys created by transient contracts are properly archived",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, owner)) =>
      val key = s"${UUID.randomUUID.toString}-key"
      val key2 = s"${UUID.randomUUID.toString}-key"

      for {
        delegation <- ledger.create(owner, Delegation(owner, owner))
        delegated <- ledger.create(owner, Delegated(owner, key))

        failedFetch <- ledger
          .exercise(owner, delegation.exerciseFetchByKeyDelegated(_, owner, key2, None))
          .failed

        // Create a transient contract with a key that is created and archived in same transaction.
        _ <- ledger.exercise(owner, delegated.exerciseCreateAnotherAndArchive(_, key2))

        // Try it again, expecting it to succeed.
        _ <- ledger.exercise(owner, delegated.exerciseCreateAnotherAndArchive(_, key2))

      } yield {
        assertGrpcError(failedFetch, Status.Code.INVALID_ARGUMENT, "couldn't find key")
      }
  }

  test(
    "CKExposedByTemplate",
    "The contract key should be exposed if the template specifies one",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val expectedKey = "some-fancy-key"
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
  }

  test(
    "CKExerciseByKey",
    "Exercising by key should be possible only when the corresponding contract is available",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val keyString = UUID.randomUUID.toString
      val expectedKey = Value(
        Value.Sum.Record(
          Record(
            fields = Seq(
              RecordField("_1", Some(Value(Value.Sum.Party(Tag.unwrap(party))))),
              RecordField("_2", Some(Value(Value.Sum.Text(keyString)))),
            ),
          ),
        ),
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
          .failed
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
          .failed
      } yield {
        assertGrpcError(
          failureBeforeCreation,
          Status.Code.INVALID_ARGUMENT,
          "dependency error: couldn't find key",
        )
        assertGrpcError(
          failureAfterConsuming,
          Status.Code.INVALID_ARGUMENT,
          "dependency error: couldn't find key",
        )
      }
  }
}
