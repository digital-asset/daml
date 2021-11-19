// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com
package daml.ledger.api.testtool.suites

import daml.error.definitions.LedgerApiErrors
import daml.ledger.api.testtool.infrastructure.Allocation._
import daml.ledger.api.testtool.infrastructure.Assertions._
import daml.ledger.api.testtool.infrastructure.Eventually.eventually
import daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import daml.ledger.test.model.DA.Types.Tuple2
import daml.ledger.test.model.Test

import io.grpc.Status

import java.util.regex.Pattern

// This test suite contains tests with assertions adapted specifically Sandbox classic with in-memory ledger backend (deprecated).
// It asserts a more generic `INCONSISTENT` error code instead of `INCONSISTENT_CONTRACT_KEY` or `DUPLICATE_CONTRACT_KEY`
// TODO sandbox-classic removal: Remove this tests
final class DeprecatedSandboxClassicMemoryContractKeysIT extends LedgerTestSuite {
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
      _ <- alpha.exercise(owner, showDelegated.exerciseShowIt(_, delegated))

      // fetch delegated
      _ <- eventually {
        beta.exercise(delegate, delegation.exerciseFetchDelegated(_, delegated))
      }

      // fetch by key should fail during interpretation
      // Reason: Only stakeholders see the result of fetchByKey, beta is neither stakeholder nor divulgee
      fetchFailure <- beta
        .exercise(delegate, delegation.exerciseFetchByKeyDelegated(_, owner, key))
        .mustFail("fetching by key with a party that cannot see the contract")

      // lookup by key delegation should fail during validation
      // Reason: During command interpretation, the lookup did not find anything due to privacy rules,
      // but validation determined that this result is wrong as the contract is there.
      lookupByKeyFailure <- beta
        .exercise(delegate, delegation.exerciseLookupByKeyDelegated(_, owner, key))
        .mustFail("looking up by key with a party that cannot see the contract")
    } yield {
      assertGrpcError(
        beta,
        fetchFailure,
        Status.Code.INVALID_ARGUMENT,
        LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("couldn't find key"),
      )
      assertGrpcErrorRegex(
        beta,
        lookupByKeyFailure,
        Status.Code.ABORTED,
        LedgerApiErrors.ConsistencyErrors.Inconsistent,
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
        .exercise(delegate, delegation.exerciseFetchDelegated(_, delegated))
        .mustFail("fetching a contract with a party that cannot see it")

      // fetch by key should fail
      // Reason: Only stakeholders see the result of fetchByKey, beta is only a divulgee
      fetchByKeyFailure <- beta
        .exercise(delegate, delegation.exerciseFetchByKeyDelegated(_, owner, key))
        .mustFail("fetching a contract by key with a party that cannot see it")

      // lookup by key should fail
      // Reason: During command interpretation, the lookup did not find anything due to privacy rules,
      // but validation determined that this result is wrong as the contract is there.
      lookupByKeyFailure <- beta
        .exercise(delegate, delegation.exerciseLookupByKeyDelegated(_, owner, key))
        .mustFail("looking up a contract by key with a party that cannot see it")
    } yield {
      assertGrpcError(
        beta,
        fetchFailure,
        Status.Code.ABORTED,
        LedgerApiErrors.ConsistencyErrors.ContractNotFound,
        Some("Contract could not be found"),
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        beta,
        fetchByKeyFailure,
        Status.Code.INVALID_ARGUMENT,
        LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("couldn't find key"),
      )
      assertGrpcErrorRegex(
        beta,
        lookupByKeyFailure,
        Status.Code.ABORTED,
        LedgerApiErrors.ConsistencyErrors.Inconsistent,
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
        .exercise(bob, bobTKO.exerciseTKOLookup(_, Tuple2(alice, key1), Some(tk1)))
        .mustFail("looking up a contract with an unauthorized key")

      // trying to lookup an unauthorized non-existing key should fail
      bobLooksUpBogusTextKeyFailure <- beta
        .exercise(bob, bobTKO.exerciseTKOLookup(_, Tuple2(alice, unknownKey), None))
        .mustFail("looking up a contract with an unauthorized, non-existing key")

      // successful, authorized lookup
      _ <- alpha.exercise(alice, aliceTKO.exerciseTKOLookup(_, Tuple2(alice, key1), Some(tk1)))

      // successful fetch
      _ <- alpha.exercise(alice, aliceTKO.exerciseTKOFetch(_, Tuple2(alice, key1), tk1))

      // successful, authorized lookup of non-existing key
      _ <- alpha.exercise(alice, aliceTKO.exerciseTKOLookup(_, Tuple2(alice, unknownKey), None))

      // failing fetch
      aliceFailedFetch <- alpha
        .exercise(alice, aliceTKO.exerciseTKOFetch(_, Tuple2(alice, unknownKey), tk1))
        .mustFail("fetching a contract by an unknown key")

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
        .mustFail("creating a contract where a maintainer is not a signatory")
    } yield {
      assertGrpcErrorRegex(
        alpha,
        duplicateKeyFailure,
        Status.Code.ABORTED,
        LedgerApiErrors.ConsistencyErrors.Inconsistent,
        Some(Pattern.compile("Inconsistent|contract key is not unique")),
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        beta,
        bobLooksUpTextKeyFailure,
        Status.Code.INVALID_ARGUMENT,
        LedgerApiErrors.CommandExecution.Interpreter.AuthorizationError,
        Some("requires authorizers"),
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        beta,
        bobLooksUpBogusTextKeyFailure,
        Status.Code.INVALID_ARGUMENT,
        LedgerApiErrors.CommandExecution.Interpreter.AuthorizationError,
        Some("requires authorizers"),
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        alpha,
        aliceFailedFetch,
        Status.Code.INVALID_ARGUMENT,
        LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound,
        Some("couldn't find key"),
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        alpha,
        maintainerNotSignatoryFailed,
        Status.Code.INVALID_ARGUMENT,
        LedgerApiErrors.CommandExecution.Interpreter.AuthorizationError,
        Some("are not a subset of the signatories"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })
}
