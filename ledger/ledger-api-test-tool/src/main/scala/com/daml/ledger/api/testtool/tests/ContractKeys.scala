// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.UUID

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.test_stable.DA.Types.Tuple2
import com.digitalasset.ledger.test_stable.Test.Delegation._
import com.digitalasset.ledger.test_stable.Test.Delegated._
import com.digitalasset.ledger.test_stable.Test.ShowDelegated._
import com.digitalasset.ledger.test_stable.Test.TextKey._
import com.digitalasset.ledger.test_stable.Test.TextKeyOperations._
import com.digitalasset.ledger.test_stable.Test._
import io.grpc.Status
import scalaz.Tag

final class ContractKeys(session: LedgerSession) extends LedgerTestSuite(session) {

  val fetchDivulgedContract =
    LedgerTest("CKFetchOrLookup", "Divulged contracts can be fetched or looked up by key") {
      context =>
        val key = s"${UUID.randomUUID.toString}-key"
        for {
          Vector(alpha, beta) <- context.participants(2)
          owner <- alpha.allocateParty()
          delegate <- beta.allocateParty()

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

          // fetch by key delegation is allowed
          _ <- beta.exercise(
            delegate,
            delegation.exerciseFetchByKeyDelegated(_, owner, key, Some(delegated)))

          // lookup by key delegation is allowed
          _ <- beta.exercise(
            delegate,
            delegation.exerciseLookupByKeyDelegated(_, owner, key, Some(delegated)))
        } yield {
          // No assertions to make, since all exercises went through as expected
          ()
        }
    }

  val rejectFetchingUndisclosedContract =
    LedgerTest(
      "CKNoFetchUndisclosed",
      "Contract Keys should reject fetching an undisclosed contract") { context =>
      val key = s"${UUID.randomUUID.toString}-key"
      for {
        Vector(alpha, beta) <- context.participants(2)
        owner <- alpha.allocateParty()
        delegate <- beta.allocateParty()

        // create contracts to work with
        delegated <- alpha.create(owner, Delegated(owner, key))
        delegation <- alpha.create(owner, Delegation(owner, delegate))

        _ <- synchronize(alpha, beta)

        // fetch should fail
        fetchFailure <- beta
          .exercise(delegate, delegation.exerciseFetchDelegated(_, delegated))
          .failed

        // this fetch still fails even if we do not check that the submitter
        // is in the lookup maintainer, since we have the visibility check
        // implement as part of #753.
        fetchByKeyFailure <- beta
          .exercise(delegate, delegation.exerciseFetchByKeyDelegated(_, owner, key, None))
          .failed

        // lookup by key should work
        _ <- beta.exercise(delegate, delegation.exerciseLookupByKeyDelegated(_, owner, key, None))
      } yield {
        assertGrpcError(
          fetchFailure,
          Status.Code.INVALID_ARGUMENT,
          "dependency error: couldn't find contract")
        assertGrpcError(fetchByKeyFailure, Status.Code.INVALID_ARGUMENT, "couldn't find key")
      }
    }

  val processContractKeys =
    LedgerTest("CKMaintainerScoped", "Contract keys should be scoped by maintainer") { context =>
      val keyPrefix = UUID.randomUUID.toString
      val key1 = s"$keyPrefix-some-key"
      val key2 = s"$keyPrefix-some-other-key"
      val unknownKey = s"$keyPrefix-unknown-key"

      for {
        Vector(alpha, beta) <- context.participants(2)
        alice <- alpha.allocateParty()
        bob <- beta.allocateParty()

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
          aliceTKO.exerciseTKOConsumeAndLookup(_, tk2, Tuple2(alice, key2)))

        // failing create when a maintainer is not a signatory
        maintainerNotSignatoryFailed <- alpha
          .create(alice, MaintainerNotSignatory(alice, bob))
          .failed
      } yield {
        assertGrpcError(duplicateKeyFailure, Status.Code.INVALID_ARGUMENT, "DuplicateKey")
        assertGrpcError(
          bobLooksUpTextKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          "requires authorizers")
        assertGrpcError(
          bobLooksUpBogusTextKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          "requires authorizers")
        assertGrpcError(aliceFailedFetch, Status.Code.INVALID_ARGUMENT, "couldn't find key")
        assertGrpcError(
          maintainerNotSignatoryFailed,
          Status.Code.INVALID_ARGUMENT,
          "are not a subset of the signatories")
      }
    }

  val recreateContractKeys =
    LedgerTest("CKRecreate", "Contract keys can be recreated in single transaction") { context =>
      val key = s"${UUID.randomUUID.toString}-key"
      for {
        ledger <- context.participant()
        owner <- ledger.allocateParty()

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
          "New contract was not created")
        assert(
          event.getCreated.contractKey == delegated1TxTree.eventsById.head._2.getCreated.contractKey,
          "Contract keys did not match")

      }
    }

  val transientContractsArchiveKeys =
    LedgerTest("CKTransients", "Contract keys created by transient contracts are properly archived") {
      context =>
        val key = s"${UUID.randomUUID.toString}-key"
        val key2 = s"${UUID.randomUUID.toString}-key"

        for {
          ledger <- context.participant()
          owner <- ledger.allocateParty()
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

  override val tests: Vector[LedgerTest] = Vector(
    fetchDivulgedContract,
    rejectFetchingUndisclosedContract,
    processContractKeys,
    recreateContractKeys,
    transientContractsArchiveKeys
  )

}
