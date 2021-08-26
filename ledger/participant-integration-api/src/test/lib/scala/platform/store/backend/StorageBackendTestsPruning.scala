// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.lf.data.Ref
import com.daml.platform.store.appendonlydao.events.ContractId
import com.daml.platform.store.backend.EventStorageBackend.{FilterParams, RangeParams}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsPruning extends Matchers with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "StorageBackend (pruning)"

  import StorageBackendTestValues._

  it should "correctly update the pruning offset" in {
    val someOffset = offset(3)
    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      initialPruningOffset <- executeSql(backend.prunedUptoInclusive)
      _ <- executeSql(backend.updatePrunedUptoInclusive(someOffset))
      updatedPruningOffset <- executeSql(backend.prunedUptoInclusive)
    } yield {
      initialPruningOffset shouldBe empty
      updatedPruningOffset shouldBe Some(someOffset)
    }
  }

  it should "prune an archived contract" in {
    val someParty = Ref.Party.assertFromString("party")
    val create = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = "#1",
      signatory = someParty,
    )
    val createTransactionId = dtoTransactionId(create)
    val archive = dtoExercise(
      offset = offset(2),
      eventSequentialId = 2L,
      consuming = true,
      contractId = "#1",
      signatory = someParty,
    )
    val range = RangeParams(0L, 2L, None, None)
    val filter = FilterParams(Set(someParty), Set.empty)
    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      // Ingest a create and archive event
      _ <- executeSql(ingest(Vector(create, archive), _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(2), 2L)))
      // Make sure the events are visible
      before1 <- executeSql(backend.transactionEvents(range, filter))
      before2 <- executeSql(backend.activeContractEvents(range, filter, offset(1)))
      before3 <- executeSql(backend.flatTransaction(createTransactionId, filter))
      before4 <- executeSql(backend.transactionTreeEvents(range, filter))
      before5 <- executeSql(backend.transactionTree(createTransactionId, filter))
      before6 <- executeSql(backend.rawEvents(0, 2L))
      // Prune
      // TODO Divulgence pruning: Adapt the tests for all divulged contracts pruning and set the flag to `true`
      _ <- executeSql(
        backend.pruneEvents(offset(2), pruneAllDivulgedContracts = true)(_, loggingContext)
      )
      _ <- executeSql(backend.updatePrunedUptoInclusive(offset(2)))
      // Make sure the events are not visible anymore
      after1 <- executeSql(backend.transactionEvents(range, filter))
      after2 <- executeSql(backend.activeContractEvents(range, filter, offset(1)))
      after3 <- executeSql(backend.flatTransaction(createTransactionId, filter))
      after4 <- executeSql(backend.transactionTreeEvents(range, filter))
      after5 <- executeSql(backend.transactionTree(createTransactionId, filter))
      after6 <- executeSql(backend.rawEvents(0, 2L))
    } yield {
      before1 should not be empty
      before2 should not be empty
      before3 should not be empty
      before4 should not be empty
      before5 should not be empty
      before6 should not be empty

      after1 shouldBe empty
      after2 shouldBe empty
      after3 shouldBe empty
      after4 shouldBe empty
      after5 shouldBe empty
      after6 shouldBe empty
    }
  }

  it should "not prune an active contract" in {
    val partyName = "party"
    val someParty = Ref.Party.assertFromString(partyName)
    val partyEntry = dtoPartyEntry(offset(1), "party")
    val create = dtoCreate(
      offset = offset(2),
      eventSequentialId = 1L,
      contractId = "#1",
      signatory = someParty,
    )
    val createTransactionId = dtoTransactionId(create)
    val range = RangeParams(0L, 1L, None, None)
    val filter = FilterParams(Set(someParty), Set.empty)
    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      // Ingest a create and archive event
      _ <- executeSql(ingest(Vector(partyEntry, create), _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(2), 1L)))
      // Make sure the events are visible
      before1 <- executeSql(backend.transactionEvents(range, filter))
      before2 <- executeSql(backend.activeContractEvents(range, filter, offset(2)))
      before3 <- executeSql(backend.flatTransaction(createTransactionId, filter))
      before4 <- executeSql(backend.transactionTreeEvents(range, filter))
      before5 <- executeSql(backend.transactionTree(createTransactionId, filter))
      before6 <- executeSql(backend.rawEvents(0, 1L))
      // Prune
      // TODO Divulgence pruning: Adapt the tests for all divulged contracts pruning and set the flag to `true`
      _ <- executeSql(
        backend.pruneEvents(offset(2), pruneAllDivulgedContracts = true)(_, loggingContext)
      )
      _ <- executeSql(backend.updatePrunedUptoInclusive(offset(2)))
      // Make sure the events are still visible - active contracts should not be pruned
      after1 <- executeSql(backend.transactionEvents(range, filter))
      after2 <- executeSql(backend.activeContractEvents(range, filter, offset(2)))
      after3 <- executeSql(backend.flatTransaction(createTransactionId, filter))
      after4 <- executeSql(backend.transactionTreeEvents(range, filter))
      after5 <- executeSql(backend.transactionTree(createTransactionId, filter))
      after6 <- executeSql(backend.rawEvents(0, 1L))
    } yield {
      before1 should not be empty
      before2 should not be empty
      before3 should not be empty
      before4 should not be empty
      before5 should not be empty
      before6 should not be empty

      // TODO is it intended that the transaction lookups don't see the active contracts?
      after1 should not be empty
      after2 should not be empty
      after3 shouldBe empty // should not be empty
      after4 should not be empty
      after5 shouldBe empty // should not be empty
      after6 should not be empty
    }
  }

  it should "prune all retroactively and immediately divulged contracts (if pruneAllDivulgedContracts is set)" in {
    val partyName = "party"
    val divulgee = Ref.Party.assertFromString(partyName)
    val contract1_id = "#1"
    val contract2_id = "#2"

    val contract1_immediateDivulgence = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = contract1_id,
      signatory = divulgee,
    )
    val partyEntry = dtoPartyEntry(offset(2), partyName)
    val contract2_createWithLocalStakeholders = dtoCreate(
      offset = offset(3),
      eventSequentialId = 2L,
      contractId = contract2_id,
      signatory = divulgee,
    )
    val contract1_retroactiveDivulgence =
      dtoDivulgence(
        offset = offset(3),
        eventSequentialId = 3L,
        contractId = contract1_id,
        divulgee = partyName,
      )

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      // Ingest
      _ <- executeSql(
        ingest(
          Vector(
            contract1_immediateDivulgence,
            partyEntry,
            contract1_retroactiveDivulgence,
            contract2_createWithLocalStakeholders,
          ),
          _,
        )
      )
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(4), 4L)))
      contract1_beforePruning <- executeSql(
        backend.activeContractWithoutArgument(
          Set(divulgee),
          ContractId.assertFromString(contract1_id),
        )
      )
      contract2_beforePruning <- executeSql(
        backend.activeContractWithoutArgument(
          Set(divulgee),
          ContractId.assertFromString(contract2_id),
        )
      )
      _ <- executeSql(
        backend.pruneEvents(offset(3), pruneAllDivulgedContracts = true)(_, loggingContext)
      )
      contract1_afterPruning <- executeSql(
        backend.activeContractWithoutArgument(
          Set(divulgee),
          ContractId.assertFromString(contract1_id),
        )
      )
      contract2_afterPruning <- executeSql(
        backend.activeContractWithoutArgument(
          Set(divulgee),
          ContractId.assertFromString(contract2_id),
        )
      )
    } yield {
      contract1_beforePruning should not be empty
      contract2_beforePruning should not be empty

      contract1_afterPruning shouldBe empty
      // Immediate divulgence for contract 2 occurred after the associated party became locally hosted
      // It is not pruned
      contract2_afterPruning should not be empty
    }
  }

  it should "only prune retroactively divulged contracts if there exists an associated consuming exercise (if pruneAllDivulgedContracts is not set)" in {
    val signatory = "signatory"
    val divulgee = Ref.Party.assertFromString("party")
    val contract1_id = "#1"
    val contract2_id = "#2"

    val contract1_create = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = contract1_id,
      signatory = signatory,
    )
    val contract1_divulgence = dtoDivulgence(
      offset = offset(2),
      eventSequentialId = 2L,
      contractId = contract1_id,
      divulgee = "party",
    )
    val contract1_consumingExercise = dtoExercise(
      offset = offset(3),
      eventSequentialId = 3L,
      consuming = true,
      contractId = contract1_id,
    )
    val contract2_divulgence = dtoDivulgence(
      offset = offset(4),
      eventSequentialId = 4L,
      contractId = contract2_id,
      divulgee = divulgee,
    )

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      // Ingest
      _ <- executeSql(
        ingest(
          Vector(
            contract1_create,
            contract1_divulgence,
            contract1_consumingExercise,
            contract2_divulgence,
          ),
          _,
        )
      )
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(5), 5L)))
      contract1_beforePruning <- executeSql(
        backend.activeContractWithoutArgument(
          Set(divulgee),
          ContractId.assertFromString(contract1_id),
        )
      )
      contract2_beforePruning <- executeSql(
        backend.activeContractWithoutArgument(
          Set(divulgee),
          ContractId.assertFromString(contract2_id),
        )
      )
      _ <- executeSql(
        backend.pruneEvents(offset(4), pruneAllDivulgedContracts = false)(_, loggingContext)
      )
      contract1_afterPruning <- executeSql(
        backend.activeContractWithoutArgument(
          Set(divulgee),
          ContractId.assertFromString(contract1_id),
        )
      )
      contract2_afterPruning <- executeSql(
        backend.activeContractWithoutArgument(
          Set(divulgee),
          ContractId.assertFromString(contract2_id),
        )
      )
    } yield {
      contract1_beforePruning should not be empty
      contract2_beforePruning should not be empty

      contract1_afterPruning shouldBe empty
      contract2_afterPruning should not be empty
    }
  }

  it should "prune all retroactively and immediately divulged contracts if pruneAllDivulgedContracts is set" in {
    val partyName = "party"
    val divulgee = Ref.Party.assertFromString(partyName)
    val contract1_id = "#1"
    val contract2_id = "#2"

    val contract1_immediateDivulgence = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = contract1_id,
      signatory = divulgee,
    )
    val partyEntry = dtoPartyEntry(offset(2), partyName)
    val contract2_createWithLocalStakeholders = dtoCreate(
      offset = offset(3),
      eventSequentialId = 2L,
      contractId = contract2_id,
      signatory = divulgee,
    )
    val contract1_retroactiveDivulgence =
      dtoDivulgence(
        offset = offset(3),
        eventSequentialId = 3L,
        contractId = contract1_id,
        divulgee = partyName,
      )

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      // Ingest
      _ <- executeSql(
        ingest(
          Vector(
            contract1_immediateDivulgence,
            partyEntry,
            contract1_retroactiveDivulgence,
            contract2_createWithLocalStakeholders,
          ),
          _,
        )
      )
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(4), 4L)))
      contract1_beforePruning <- executeSql(
        backend.activeContractWithoutArgument(
          Set(divulgee),
          ContractId.assertFromString(contract1_id),
        )
      )
      contract2_beforePruning <- executeSql(
        backend.activeContractWithoutArgument(
          Set(divulgee),
          ContractId.assertFromString(contract2_id),
        )
      )
      _ <- executeSql(
        backend.pruneEvents(offset(3), pruneAllDivulgedContracts = true)(_, loggingContext)
      )
      contract1_afterPruning <- executeSql(
        backend.activeContractWithoutArgument(
          Set(divulgee),
          ContractId.assertFromString(contract1_id),
        )
      )
      contract2_afterPruning <- executeSql(
        backend.activeContractWithoutArgument(
          Set(divulgee),
          ContractId.assertFromString(contract2_id),
        )
      )
    } yield {
      contract1_beforePruning should not be empty
      contract2_beforePruning should not be empty

      contract1_afterPruning shouldBe empty
      contract2_afterPruning should not be empty
    }
  }

  it should "prune completions" in {
    val someParty = Ref.Party.assertFromString("party")
    val completion = dtoCompletion(
      offset = offset(1),
      submitter = someParty,
    )
    val applicationId = dtoApplicationId(completion)
    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      // Ingest a completion
      _ <- executeSql(ingest(Vector(completion), _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(1), 1L)))
      // Make sure the completion is visible
      before <- executeSql(
        backend.commandCompletions(offset(0), offset(1), applicationId, Set(someParty))
      )
      // Prune
      _ <- executeSql(backend.pruneCompletions(offset(1))(_, loggingContext))
      _ <- executeSql(backend.updatePrunedUptoInclusive(offset(1)))
      // Make sure the completion is not visible anymore
      after <- executeSql(
        backend.commandCompletions(offset(0), offset(1), applicationId, Set(someParty))
      )
    } yield {
      before should not be empty
      after shouldBe empty
    }
  }
}
