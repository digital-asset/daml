// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

import org.scalatest.LoneElement
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[appendonlydao] trait JdbcLedgerDaoTransactionsWriterSpec extends LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private val ok = io.grpc.Status.Code.OK.value()

  behavior of "JdbcLedgerDao (TransactionsWriter)"

  it should "serialize a valid positive lookupByKey" in {
    val keyValue = "positive-lookup-by-key"

    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, create) <- store(txCreateContractWithKey(alice, keyValue))
      createdContractId = nonTransient(create).loneElement
      (_, lookup) <- store(txLookupByKey(alice, keyValue, Some(createdContractId)))
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
    } yield {
      completions should contain.allOf(
        create.commandId.get -> ok,
        lookup.commandId.get -> ok,
      )
    }
  }

  it should "serialize a valid fetch" in {
    val keyValue = "valid-fetch"

    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, create) <- store(txCreateContractWithKey(alice, keyValue))
      createdContractId = nonTransient(create).loneElement
      (_, fetch) <- store(txFetch(alice, createdContractId))
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
    } yield {
      completions should contain.allOf(
        create.commandId.get -> ok,
        fetch.commandId.get -> ok,
      )
    }
  }

  // TODO Remove once decided that we don't need to assert blinding info since it is not used with the SqlLedger
//  it should "prefer pre-computed blinding info" in {
//    val mismatchingBlindingInfo =
//      BlindingInfo(Map(NodeId(0) -> Set(Ref.Party.assertFromString("zoe"))), Map())
//    for {
//      (_, tx) <- store(
//        offsetAndTx = singleCreate,
//        blindingInfo = Some(mismatchingBlindingInfo),
//        divulgedContracts = Map.empty,
//      )
//      result <- ledgerDao.lookupActiveOrDivulgedContract(nonTransient(tx).loneElement, Set(alice))
//    } yield {
//      result shouldBe None
//    }
//  }

  // TODO do we need incremental offset checks for append-only?
//  it should "fail trying to store transactions with non-incremental offsets" in {
//    val (offset, tx) = singleCreate
//    recoverToSucceededIf[LedgerEndUpdateError](
//      storeOffsetStepAndTx(
//        offsetStepAndTx = IncrementalOffsetStep(nextOffset(), offset) -> tx,
//        blindingInfo = None,
//        divulgedContracts = Map.empty,
//      )
//    )
//  }
}
