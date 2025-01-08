// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{LoneElement, OptionValues}

private[dao] trait JdbcLedgerDaoTransactionsWriterSpec extends LoneElement with OptionValues {
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
      completions <- getCompletions(
        from.fold(Offset.firstOffset)(_.lastOffset.increment),
        to.map(_.lastOffset).getOrElse(fail("ledger end should not have been empty")),
        defaultAppId,
        Set(alice),
      )
    } yield {
      completions should contain.allOf(
        create.commandId.value -> ok,
        lookup.commandId.value -> ok,
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
      completions <- getCompletions(
        from.fold(Offset.firstOffset)(_.lastOffset.increment),
        to.map(_.lastOffset).getOrElse(fail("ledger end should not have been empty")),
        defaultAppId,
        Set(alice),
      )
    } yield {
      completions should contain.allOf(
        create.commandId.value -> ok,
        fetch.commandId.value -> ok,
      )
    }
  }

  it should "prefer stakeholder info" in {
    for {
      (offset, tx) <- store(offsetAndTx = singleCreate)
      result <- ledgerDao.contractsReader.lookupContractState(
        nonTransient(tx).loneElement,
        offset,
      )
    } yield {
      result.collect { case active: LedgerDaoContractsReader.ActiveContract =>
        active.stakeholders -> active.signatories
      } shouldBe Some(Set(alice, bob) -> Set(alice, bob))
    }
  }
}
