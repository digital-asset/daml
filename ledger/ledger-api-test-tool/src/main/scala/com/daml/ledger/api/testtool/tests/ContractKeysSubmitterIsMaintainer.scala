// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.UUID

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.test_dev.DA.Types.{Tuple2 => DamlTuple2}
import com.digitalasset.ledger.test_dev.Test.Delegation._
import com.digitalasset.ledger.test_dev.Test.ShowDelegated._
import com.digitalasset.ledger.test_dev.Test.TextKey._
import com.digitalasset.ledger.test_dev.Test.TextKeyOperations._
import com.digitalasset.ledger.test_dev.Test._
import io.grpc.Status

final class ContractKeysSubmitterIsMaintainer(session: LedgerSession)
    extends LedgerTestSuite(session) {
  test(
    "CKNoFetchOrLookup",
    "Divulged contracts cannot be fetched or looked up by key",
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

        // fetch by key delegation is not allowed
        fetchByKeyFailure <- beta
          .exercise(
            delegate,
            delegation
              .exerciseFetchByKeyDelegated(_, owner, key, Some(delegated)),
          )
          .failed

        // lookup by key delegation is not allowed
        lookupByKeyFailure <- beta
          .exercise(
            delegate,
            delegation
              .exerciseLookupByKeyDelegated(_, owner, key, Some(delegated)),
          )
          .failed
      } yield {
        assertGrpcError(
          fetchByKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          s"Expected the submitter '$delegate' to be in maintainers '$owner'",
        )
        assertGrpcError(
          lookupByKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          s"Expected the submitter '$delegate' to be in maintainers '$owner'",
        )
      }
  }

  test(
    "CKSubmitterIsMaintainerNoFetchUndisclosed",
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
        fetchFailure <- beta
          .exercise(
            delegate,
            delegation
              .exerciseFetchDelegated(_, delegated),
          )
          .failed

        // fetch by key should fail
        fetchByKeyFailure <- beta
          .exercise(
            delegate,
            delegation
              .exerciseFetchByKeyDelegated(_, owner, key, None),
          )
          .failed

        // lookup by key should fail
        lookupByKeyFailure <- beta
          .exercise(
            delegate,
            delegation
              .exerciseLookupByKeyDelegated(_, owner, key, None),
          )
          .failed
      } yield {
        assertGrpcError(
          fetchFailure,
          Status.Code.INVALID_ARGUMENT,
          "dependency error: couldn't find contract",
        )
        assertGrpcError(
          fetchByKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          s"Expected the submitter '$delegate' to be in maintainers '$owner'",
        )
        assertGrpcError(
          lookupByKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          s"Expected the submitter '$delegate' to be in maintainers '$owner'",
        )
      }
  }

  test(
    "CKSubmitterIsMaintainerMaintainerScoped",
    "Contract keys should be scoped by maintainer",
    allocate(SingleParty, SingleParty),
  ) {
    case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
      val keyPrefix = UUID.randomUUID.toString
      val key1 = s"$keyPrefix-some-key"
      val key2 = s"$keyPrefix-some-other-key"
      val unknownKey = s"$keyPrefix-unknown-key"

      for {
        //create contracts to work with
        tk1 <- alpha.create(alice, TextKey(alice, key1, List(bob)))
        tk2 <- alpha.create(alice, TextKey(alice, key2, List(bob)))
        aliceTKO <- alpha.create(alice, TextKeyOperations(alice))
        bobTKO <- beta.create(bob, TextKeyOperations(bob))

        // creating a contract with a duplicate key should fail
        duplicateKeyFailure <- alpha.create(alice, TextKey(alice, key1, List(bob))).failed

        _ <- synchronize(alpha, beta)

        // trying to lookup an unauthorized key should fail
        bobLooksUpTextKeyFailure <- beta
          .exercise(
            bob,
            bobTKO
              .exerciseTKOLookup(_, DamlTuple2(alice, key1), Some(tk1)),
          )
          .failed

        // trying to lookup an unauthorized non-existing key should fail
        bobLooksUpBogusTextKeyFailure <- beta
          .exercise(bob, bobTKO.exerciseTKOLookup(_, DamlTuple2(alice, unknownKey), None))
          .failed

        // successful, authorized lookup
        _ <- alpha.exercise(
          alice,
          aliceTKO
            .exerciseTKOLookup(_, DamlTuple2(alice, key1), Some(tk1)),
        )

        // successful fetch
        _ <- alpha.exercise(alice, aliceTKO.exerciseTKOFetch(_, DamlTuple2(alice, key1), tk1))

        // successful, authorized lookup of non-existing key
        _ <- alpha.exercise(
          alice,
          aliceTKO.exerciseTKOLookup(_, DamlTuple2(alice, unknownKey), None),
        )

        // failing fetch
        aliceFailedFetch <- alpha
          .exercise(
            alice,
            aliceTKO
              .exerciseTKOFetch(_, DamlTuple2(alice, unknownKey), tk1),
          )
          .failed

        // now we exercise the contract, thus archiving it, and then verify
        // that we cannot look it up anymore
        _ <- alpha.exercise(alice, tk1.exerciseTextKeyChoice)
        _ <- alpha.exercise(alice, aliceTKO.exerciseTKOLookup(_, DamlTuple2(alice, key1), None))

        // lookup the key, consume it, then verify we cannot look it up anymore
        _ <- alpha.exercise(
          alice,
          aliceTKO.exerciseTKOConsumeAndLookup(_, tk2, DamlTuple2(alice, key2)),
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
          s"Expected the submitter '$bob' to be in maintainers '$alice'",
        )
        assertGrpcError(
          bobLooksUpBogusTextKeyFailure,
          Status.Code.INVALID_ARGUMENT,
          s"Expected the submitter '$bob' to be in maintainers '$alice'",
        )
        assertGrpcError(aliceFailedFetch, Status.Code.INVALID_ARGUMENT, "couldn't find key")
        assertGrpcError(
          maintainerNotSignatoryFailed,
          Status.Code.INVALID_ARGUMENT,
          "are not a subset of the signatories",
        )
      }
  }
}
