// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.UUID

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.daml.ledger.api.testtool.templates.{
  Delegated,
  Delegation,
  MaintainerNotSignatory,
  ShowDelegated,
  TextKey,
  TextKeyOperations
}
import io.grpc.Status

final class ContractKeys(session: LedgerSession) extends LedgerTestSuite(session) {

  val fetchDivulgedContract = LedgerTest("Contract keys should fetch a divulged contract") {
    implicit context =>
      val key = s"${UUID.randomUUID.toString}-key"
      for {
        Vector(owner, delegate) <- allocateParties(2)
        delegated <- Delegated(owner, key)
        delegation <- Delegation(owner, delegate)
        showId <- ShowDelegated(owner, delegate)
        _ <- showId.showIt(delegated.contractId)
        _ <- delegation.fetchDelegated(delegated.contractId)
        fetchByKeyFailure <- delegation.fetchByKeyDelegated(key, Some(delegated.contractId)).failed
        lookupByKeyFailure <- delegation
          .lookupByKeyDelegated(key, Some(delegated.contractId))
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
    LedgerTest("Contract Keys should reject fetching an undisclosed contract") { implicit context =>
      val key = s"${UUID.randomUUID.toString}-key"
      for {
        Vector(owner, delegate) <- allocateParties(2)
        delegated <- Delegated(owner, key)
        delegation <- Delegation(owner, delegate)
        fetchFailure <- delegation.fetchDelegated(delegated.contractId).failed
        fetchByKeyFailure <- delegation.fetchByKeyDelegated(key, None).failed
        lookupByKeyFailure <- delegation.lookupByKeyDelegated(key, None).failed
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

  val processContractKeys = LedgerTest("Conract keys should be scoped by maintainer") {
    implicit context =>
      val keyPrefix = UUID.randomUUID.toString
      val key1 = s"$keyPrefix-some-key"
      val key2 = s"$keyPrefix-some-key"
      val unknownKey = s"$keyPrefix-unknown-key"

      for {
        Vector(alice, bob) <- allocateParties(2)
        tk1 <- TextKey(alice, key1, List(bob))
        duplicateKeyFailure <- TextKey(alice, key1, List(bob)).failed
        aliceTKO <- TextKeyOperations(alice)
        bobTKO <- TextKeyOperations(bob)

        // trying to lookup an unauthorized key should fail
        bobLooksUpTextKeyFailure <- bobTKO.lookup((alice, key1), Some(tk1.contractId)).failed
        // trying to lookup an unauthorized non-existing key should fail
        bobLooksUpBogusTextKeyFailure <- bobTKO.lookup((alice, unknownKey), None).failed

        // successful, authorized lookup
        _ <- aliceTKO.lookup((alice, key1), Some(tk1.contractId))

        // successful fetch
        _ <- aliceTKO.fetch((alice, key1), tk1.contractId)

        // successful, authorized lookup of non-existing key
        _ <- aliceTKO.lookup((alice, unknownKey), None)

        // failing fetch
        aliceFailedFetch <- aliceTKO.fetch((alice, unknownKey), tk1.contractId).failed

        // now we exercise the contract, thus archiving it, and then verify
        // that we cannot look it up anymore
        _ <- tk1.choice()
        _ <- aliceTKO.lookup((alice, key1), None)

        tk2 <- TextKey(alice, key2, List(bob))
        _ <- aliceTKO.consumeAndLookup((alice, key2), tk2.contractId)

        // failing create when a maintainer is not a signatory
        failedMaintainerNotSignatory <- MaintainerNotSignatory(alice, bob).failed
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
          failedMaintainerNotSignatory,
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
