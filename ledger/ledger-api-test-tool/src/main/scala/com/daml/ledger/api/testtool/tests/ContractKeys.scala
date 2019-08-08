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
        fetchByKeyFailureCode <- context.extractErrorStatus(
          delegation.fetchByKeyDelegated(key, Some(delegated.contractId)))
        lookupByKeyFailureCode <- context.extractErrorStatus(
          delegation.lookupByKeyDelegated(key, Some(delegated.contractId)))
      } yield {
        assert(fetchByKeyFailureCode == Status.Code.INVALID_ARGUMENT)
        assert(lookupByKeyFailureCode == Status.Code.INVALID_ARGUMENT)
      }
  }

  val rejectFetchingUndisclosedContract =
    LedgerTest("Contract Keys should reject fetching an undisclosed contract") { implicit context =>
      val key = s"${UUID.randomUUID.toString}-key"
      for {
        Vector(owner, delegate) <- allocateParties(2)
        delegated <- Delegated(owner, key)
        delegation <- Delegation(owner, delegate)
        fetchFailureCode <- context.extractErrorStatus(
          delegation.fetchDelegated(delegated.contractId))
        fetchByKeyFailureCode <- context.extractErrorStatus(
          delegation.fetchByKeyDelegated(key, None))
        lookupByKeyFailureCode <- context.extractErrorStatus(
          delegation.lookupByKeyDelegated(key, None))
      } yield {
        assert(fetchFailureCode == Status.Code.INVALID_ARGUMENT)
        assert(fetchByKeyFailureCode == Status.Code.INVALID_ARGUMENT)
        assert(lookupByKeyFailureCode == Status.Code.INVALID_ARGUMENT)
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
        duplicateKeyCode <- context.extractErrorStatus(TextKey(alice, key1, List(bob)))
        aliceTKO <- TextKeyOperations(alice)
        bobTKO <- TextKeyOperations(bob)

        // trying to lookup an unauthorized key should fail
        bobLooksUpTKErrorCode <- context.extractErrorStatus(
          bobTKO.lookup((alice, key1), Some(tk1.contractId)))
        // trying to lookup an unauthorized non-existing key should fail
        bobLooksUpBogusTKErrorCode <- context.extractErrorStatus(
          bobTKO.lookup((alice, unknownKey), None))

        // successful, authorized lookup
        _ <- aliceTKO.lookup((alice, key1), Some(tk1.contractId))

        // successful fetch
        _ <- aliceTKO.fetch((alice, key1), tk1.contractId)

        // successful, authorized lookup of non-existing key
        _ <- aliceTKO.lookup((alice, unknownKey), None)

        // failing fetch
        aliceFailedFetchErrorCode <- context.extractErrorStatus(
          aliceTKO.fetch((alice, unknownKey), tk1.contractId))

        // now we exercise the contract, thus archiving it, and then verify
        // that we cannot look it up anymore
        _ <- tk1.choice()
        _ <- aliceTKO.lookup((alice, key1), None)

        tk2 <- TextKey(alice, key2, List(bob))
        _ <- aliceTKO.consumeAndLookup((alice, key2), tk2.contractId)

        // failing create when a maintainer is not a signatory
        failedMaintainerNotSignatory <- context.extractErrorStatus(
          MaintainerNotSignatory(alice, bob))
      } yield {
        assert(duplicateKeyCode == Status.Code.INVALID_ARGUMENT)
        assert(bobLooksUpTKErrorCode == Status.Code.INVALID_ARGUMENT)
        assert(bobLooksUpBogusTKErrorCode == Status.Code.INVALID_ARGUMENT)
        assert(aliceFailedFetchErrorCode == Status.Code.INVALID_ARGUMENT)
        assert(failedMaintainerNotSignatory == Status.Code.INVALID_ARGUMENT)
      }
  }

  override val tests: Vector[LedgerTest] = Vector(
    fetchDivulgedContract,
    rejectFetchingUndisclosedContract,
    processContractKeys
  )

}
