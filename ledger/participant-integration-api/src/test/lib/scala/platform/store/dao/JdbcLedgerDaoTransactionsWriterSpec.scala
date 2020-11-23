// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.lf.data.Ref
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction
import com.daml.lf.transaction.{BlindingInfo, NodeId}
import com.daml.platform.store.dao.events.TransactionsWriter
import org.scalatest.{AsyncFlatSpec, LoneElement, Matchers}

private[dao] trait JdbcLedgerDaoTransactionsWriterSpec extends LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private val ok = io.grpc.Status.Code.OK.value()

  behavior of "JdbcLedgerDao (TransactionsWriter)"

  it should "serialize a valid positive lookupByKey" in {
    val keyValue = s"positive-lookup-by-key"

    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, create) <- store(txCreateContractWithKey(alice, keyValue))
      createdContractId = nonTransient(create).loneElement
      (_, lookup) <- store(txLookupByKey(alice, keyValue, Some(createdContractId)))
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
    } yield {
      completions should contain allOf (
        create.commandId.get -> ok,
        lookup.commandId.get -> ok,
      )
    }
  }

  it should "serialize a valid fetch" in {
    val keyValue = s"valid-fetch"

    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, create) <- store(txCreateContractWithKey(alice, keyValue))
      createdContractId = nonTransient(create).loneElement
      (_, fetch) <- store(txFetch(alice, createdContractId))
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
    } yield {
      completions should contain allOf (
        create.commandId.get -> ok,
        fetch.commandId.get -> ok,
      )
    }
  }

  it should "prefer pre-computed blinding info" in {
    for {
      (_, tx) <- store(singleCreate)
    } yield {
      TransactionsWriter.extractBlindingInfo(
        transaction.CommittedTransaction(tx.transaction),
        Some(aBlindingInfo),
      ) should be(aBlindingInfo)
    }
  }

  it should "fall back to computing blinding info from transaction" in {
    for {
      (_, tx) <- store(singleCreate)
    } yield {
      val committedTransaction = transaction.CommittedTransaction(tx.transaction)
      TransactionsWriter.extractBlindingInfo(
        committedTransaction,
        None,
      ) should be(Blinding.blind(committedTransaction))
    }
  }

  private lazy val aBlindingInfo =
    BlindingInfo(Map(NodeId(0) -> Set(Ref.Party.assertFromString("aParty"))), Map())
}
