// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.UUID

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.test_1_6.DA.Types.Tuple2
import com.digitalasset.ledger.test_1_6.Test.Delegation._
import com.digitalasset.ledger.test_1_6.Test.ShowDelegated._
import com.digitalasset.ledger.test_1_6.Test.TextKey._
import com.digitalasset.ledger.test_1_6.Test.TextKeyOperations._
import com.digitalasset.ledger.test_1_6.Test._
import io.grpc.Status

final class ContractKeys(session: LedgerSession) extends LedgerTestSuite(session) {

  val fetchDivulgedContract =
    LedgerTest("CKFetchOrLookup", "Divulged contracts can be fetched or looked up by key") {
      implicit context =>
        val key = s"${UUID.randomUUID.toString}-key"
        for {
          Vector(owner, delegate) <- allocateParties(2)

          // create contracts to work with
          delegated <- create(Delegated(owner, key))(owner)
          delegation <- create(Delegation(owner, delegate))(owner)
          showDelegated <- create(ShowDelegated(owner, delegate))(owner)

          // divulge the contract
          _ <- exercise(showDelegated.contractId.exerciseShowIt(_, delegated.contractId))(owner)

          // fetch delegated
          _ <- exercise(delegation.contractId.exerciseFetchDelegated(_, delegated.contractId))(
            delegate)

          // fetch by key delegation is not allowed
          _ <- exercise(
            delegation.contractId
              .exerciseFetchByKeyDelegated(_, owner, key, Some(delegated.contractId)))(delegate)

          // lookup by key delegation is not allowed
          _ <- exercise(
            delegation.contractId
              .exerciseLookupByKeyDelegated(_, owner, key, Some(delegated.contractId)))(delegate)
        } yield {
          // No assertions to make, since all exercises went through as expected
          ()
        }
    }

  val rejectFetchingUndisclosedContract =
    LedgerTest(
      "CKNoFetchUndisclosed",
      "Contract Keys should reject fetching an undisclosed contract") { implicit context =>
      val key = s"${UUID.randomUUID.toString}-key"
      for {
        Vector(owner, delegate) <- allocateParties(2)

        // create contracts to work with
        delegated <- create(Delegated(owner, key))(owner)
        delegation <- create(Delegation(owner, delegate))(owner)

        // fetch should fail
        fetchFailure <- exercise(
          delegation.contractId
            .exerciseFetchDelegated(_, delegated.contractId))(delegate).failed

        // this fetch still fails even if we do not check that the submitter
        // is in the lookup maintainer, since we have the visibility check
        // implement as part of #753.
        fetchByKeyFailure <- exercise(
          delegation.contractId
            .exerciseFetchByKeyDelegated(_, owner, key, None))(delegate).failed

        // lookup by key should work
        _ <- exercise(
          delegation.contractId
            .exerciseLookupByKeyDelegated(_, owner, key, None))(delegate)
      } yield {
        assertGrpcError(
          fetchFailure,
          Status.Code.INVALID_ARGUMENT,
          "dependency error: couldn't find contract")
        assertGrpcError(fetchByKeyFailure, Status.Code.INVALID_ARGUMENT, "couldn't find key")
      }
    }

  val processContractKeys =
    LedgerTest("CKMaintainerScoped", "Contract keys should be scoped by maintainer") {
      implicit context =>
        val keyPrefix = UUID.randomUUID.toString
        val key1 = s"$keyPrefix-some-key"
        val key2 = s"$keyPrefix-some-other-key"
        val unknownKey = s"$keyPrefix-unknown-key"

        for {
          Vector(alice, bob) <- allocateParties(2)

          //create contracts to work with
          tk1 <- create(TextKey(alice, key1, List(bob)))(alice)
          tk2 <- create(TextKey(alice, key2, List(bob)))(alice)
          aliceTKO <- create(TextKeyOperations(alice))(alice)
          bobTKO <- create(TextKeyOperations(bob))(bob)

          // creating a contract with a duplicate key should fail
          duplicateKeyFailure <- create(TextKey(alice, key1, List(bob)))(alice).failed

          // trying to lookup an unauthorized key should fail
          bobLooksUpTextKeyFailure <- exercise(
            bobTKO.contractId
              .exerciseTKOLookup(_, Tuple2(alice, key1), Some(tk1.contractId)))(bob).failed

          // trying to lookup an unauthorized non-existing key should fail
          bobLooksUpBogusTextKeyFailure <- exercise(
            bobTKO.contractId.exerciseTKOLookup(_, Tuple2(alice, unknownKey), None))(bob).failed

          // successful, authorized lookup
          _ <- exercise(
            aliceTKO.contractId
              .exerciseTKOLookup(_, Tuple2(alice, key1), Some(tk1.contractId)))(alice)

          // successful fetch
          _ <- exercise(
            aliceTKO.contractId.exerciseTKOFetch(_, Tuple2(alice, key1), tk1.contractId))(alice)

          // successful, authorized lookup of non-existing key
          _ <- exercise(aliceTKO.contractId.exerciseTKOLookup(_, Tuple2(alice, unknownKey), None))(
            alice)

          // failing fetch
          aliceFailedFetch <- exercise(
            aliceTKO.contractId.exerciseTKOFetch(_, Tuple2(alice, unknownKey), tk1.contractId))(
            alice).failed

          // now we exercise the contract, thus archiving it, and then verify
          // that we cannot look it up anymore
          _ <- exercise(tk1.contractId.exerciseTextKeyChoice)(alice)
          _ <- exercise(aliceTKO.contractId.exerciseTKOLookup(_, Tuple2(alice, key1), None))(alice)

          // lookup the key, consume it, then verify we cannot look it up anymore
          _ <- exercise(
            aliceTKO.contractId
              .exerciseTKOConsumeAndLookup(_, tk2.contractId, Tuple2(alice, key2)))(alice)

          // failing create when a maintainer is not a signatory
          maintainerNotSignatoryFailed <- create(MaintainerNotSignatory(alice, bob))(alice).failed
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

  override val tests: Vector[LedgerTest] = Vector(
    fetchDivulgedContract,
    rejectFetchingUndisclosedContract,
    processContractKeys
  )

}
