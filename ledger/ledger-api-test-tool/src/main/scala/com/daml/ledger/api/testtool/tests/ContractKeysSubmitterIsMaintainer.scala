// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.UUID

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.test_dev.DA.Types.{Tuple2 => DamlTuple2}
import com.digitalasset.ledger.test_dev.Test.Delegation._
import com.digitalasset.ledger.test_dev.Test.ShowDelegated._
import com.digitalasset.ledger.test_dev.Test.TextKey._
import com.digitalasset.ledger.test_dev.Test.TextKeyOperations._
import com.digitalasset.ledger.test_dev.Test._
import io.grpc.Status

final class ContractKeysSubmitterIsMaintainer(session: LedgerSession)
    extends LedgerTestSuite(session) {

  val fetchDivulgedContract =
    LedgerTest("CKNoFetchOrLookup", "Divulged contracts cannot be fetched or looked up by key") {
      context =>
        val key = s"${UUID.randomUUID.toString}-key"
        for {
          ledger <- context.participant()
          Vector(owner, delegate) <- ledger.allocateParties(2)

          // create contracts to work with
          delegated <- ledger.create(owner, Delegated(owner, key))
          delegation <- ledger.create(owner, Delegation(owner, delegate))
          showDelegated <- ledger.create(owner, ShowDelegated(owner, delegate))

          // divulge the contract
          _ <- ledger.exercise(owner, showDelegated.exerciseShowIt(_, delegated))
          // fetch delegated
          _ <- ledger.exercise(delegate, delegation.exerciseFetchDelegated(_, delegated))

          // fetch by key delegation is not allowed
          fetchByKeyFailure <- ledger
            .exercise(
              delegate,
              delegation
                .exerciseFetchByKeyDelegated(_, owner, key, Some(delegated)))
            .failed

          // lookup by key delegation is not allowed
          lookupByKeyFailure <- ledger
            .exercise(
              delegate,
              delegation
                .exerciseLookupByKeyDelegated(_, owner, key, Some(delegated)))
            .failed
        } yield {
          assertGrpcError(
            fetchByKeyFailure,
            Status.Code.INVALID_ARGUMENT,
            s"Expected the submitter '$delegate' to be in maintainers '$owner'")
          assertGrpcError(
            lookupByKeyFailure,
            Status.Code.INVALID_ARGUMENT,
            s"Expected the submitter '$delegate' to be in maintainers '$owner'")
        }
    }

  val rejectFetchingUndisclosedContract =
    LedgerTest(
      "CKSubmitterIsMaintainerNoFetchUndisclosed",
      "Contract Keys should reject fetching an undisclosed contract") { context =>
      val key = s"${UUID.randomUUID.toString}-key"
      for {
        ledger <- context.participant()
        Vector(owner, delegate) <- ledger.allocateParties(2)

        // create contracts to work with
        delegated <- ledger.create(owner, Delegated(owner, key))
        delegation <- ledger.create(owner, Delegation(owner, delegate))

        // fetch should fail
        fetchFailure <- ledger
          .exercise(
            delegate,
            delegation
              .exerciseFetchDelegated(_, delegated))
          .failed

        // fetch by key should fail
        fetchByKeyFailure <- ledger
          .exercise(
            delegate,
            delegation
              .exerciseFetchByKeyDelegated(_, owner, key, None))
          .failed

        // lookup by key should fail
        lookupByKeyFailure <- ledger
          .exercise(
            delegate,
            delegation
              .exerciseLookupByKeyDelegated(_, owner, key, None))
          .failed
      } yield {
        assertGrpcError(
          fetchFailure,
          Status.Code.INVALID_ARGUMENT,
          "dependency error: couldn't find contract")
        assertGrpcError(
          fetchByKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          s"Expected the submitter '$delegate' to be in maintainers '$owner'")
        assertGrpcError(
          lookupByKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          s"Expected the submitter '$delegate' to be in maintainers '$owner'")
      }
    }

  val processContractKeys =
    LedgerTest(
      "CKSubmitterIsMaintainerMaintainerScoped",
      "Contract keys should be scoped by maintainer") { context =>
      val keyPrefix = UUID.randomUUID.toString
      val key1 = s"$keyPrefix-some-key"
      val key2 = s"$keyPrefix-some-other-key"
      val unknownKey = s"$keyPrefix-unknown-key"

      for {
        ledger <- context.participant()
        Vector(alice, bob) <- ledger.allocateParties(2)

        //create contracts to work with
        tk1 <- ledger.create(alice, TextKey(alice, key1, List(bob)))
        tk2 <- ledger.create(alice, TextKey(alice, key2, List(bob)))
        aliceTKO <- ledger.create(alice, TextKeyOperations(alice))
        bobTKO <- ledger.create(bob, TextKeyOperations(bob))

        // creating a contract with a duplicate key should fail
        duplicateKeyFailure <- ledger.create(alice, TextKey(alice, key1, List(bob))).failed

        // trying to lookup an unauthorized key should fail
        bobLooksUpTextKeyFailure <- ledger
          .exercise(
            bob,
            bobTKO
              .exerciseTKOLookup(_, DamlTuple2(alice, key1), Some(tk1)))
          .failed

        // trying to lookup an unauthorized non-existing key should fail
        bobLooksUpBogusTextKeyFailure <- ledger
          .exercise(bob, bobTKO.exerciseTKOLookup(_, DamlTuple2(alice, unknownKey), None))
          .failed

        // successful, authorized lookup
        _ <- ledger.exercise(
          alice,
          aliceTKO
            .exerciseTKOLookup(_, DamlTuple2(alice, key1), Some(tk1)))

        // successful fetch
        _ <- ledger.exercise(alice, aliceTKO.exerciseTKOFetch(_, DamlTuple2(alice, key1), tk1))

        // successful, authorized lookup of non-existing key
        _ <- ledger.exercise(
          alice,
          aliceTKO.exerciseTKOLookup(_, DamlTuple2(alice, unknownKey), None))

        // failing fetch
        aliceFailedFetch <- ledger
          .exercise(
            alice,
            aliceTKO
              .exerciseTKOFetch(_, DamlTuple2(alice, unknownKey), tk1))
          .failed

        // now we exercise the contract, thus archiving it, and then verify
        // that we cannot look it up anymore
        _ <- ledger.exercise(alice, tk1.exerciseTextKeyChoice)
        _ <- ledger.exercise(alice, aliceTKO.exerciseTKOLookup(_, DamlTuple2(alice, key1), None))

        // lookup the key, consume it, then verify we cannot look it up anymore
        _ <- ledger.exercise(
          alice,
          aliceTKO.exerciseTKOConsumeAndLookup(_, tk2, DamlTuple2(alice, key2)))

        // failing create when a maintainer is not a signatory
        maintainerNotSignatoryFailed <- ledger
          .create(alice, MaintainerNotSignatory(alice, bob))
          .failed
      } yield {
        assertGrpcError(duplicateKeyFailure, Status.Code.INVALID_ARGUMENT, "DuplicateKey")
        assertGrpcError(
          bobLooksUpTextKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          s"Expected the submitter '$bob' to be in maintainers '$alice'")
        assertGrpcError(
          bobLooksUpBogusTextKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          s"Expected the submitter '$bob' to be in maintainers '$alice'")
        assertGrpcError(aliceFailedFetch, Status.Code.INVALID_ARGUMENT, "couldn't find key")
        assertGrpcError(
          maintainerNotSignatoryFailed,
          Status.Code.INVALID_ARGUMENT,
          "are not a subset of the signatories")
      }
    }

  override val tests: Vector[LedgerTest] = Vector(
    fetchDivulgedContract,
    rejectFetchingUndisclosedContract,
    processContractKeys
  )

}
