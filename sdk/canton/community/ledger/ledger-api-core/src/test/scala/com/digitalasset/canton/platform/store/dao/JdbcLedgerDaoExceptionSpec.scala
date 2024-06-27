// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.daml.lf.transaction.test.TreeTransactionBuilder.*
import com.digitalasset.daml.lf.transaction.test.{
  TestIdFactory,
  TestNodeBuilder,
  TreeTransactionBuilder,
}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement, OptionValues}

/** There are two important parts to cover with Daml exceptions:
  * - Create and exercise nodes under rollback nodes should not be indexed
  * - Lookup and fetch nodes under rollback nodes may lead to divulgence
  */
private[dao] trait JdbcLedgerDaoExceptionSpec
    extends LoneElement
    with Inside
    with OptionValues
    with TestIdFactory {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private def contractsReader = ledgerDao.contractsReader

  behavior of "JdbcLedgerDao (exceptions)"

  it should "not find contracts created under rollback nodes" in {

    val cid1 = newCid
    val cid2 = newCid

    val tx = TreeTransactionBuilder.toCommittedTransaction(
      TestNodeBuilder
        .rollback()
        .withChildren(
          createNode(absCid = cid1, signatories = Set(alice), stakeholders = Set(alice))
        ),
      createNode(absCid = cid2, signatories = Set(alice), stakeholders = Set(alice)),
    )
    val offsetAndEntry = fromTransaction(tx)

    for {
      (offset, _) <- store(offsetAndEntry)
      result1 <- contractsReader.lookupContractState(cid1, offset)
      result2 <- contractsReader.lookupContractState(cid2, offset)
    } yield {
      result1 shouldBe None
      result2.value shouldBe a[LedgerDaoContractsReader.ActiveContract]
    }
  }
}
