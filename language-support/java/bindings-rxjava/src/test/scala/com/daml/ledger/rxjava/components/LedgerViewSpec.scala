// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components

import com.daml.ledger.javaapi.data.Identifier
import org.pcollections.HashTreePMap
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Random

class LedgerViewSpec extends AnyFlatSpec with Matchers {

  "LedgerView" should "add active contract" in {
    val state = LedgerViewFlowable.LedgerView.create[Unit]()
    val tid = templateId()
    val cid = contractId()
    val newState = state.addActiveContract(tid, cid, ())
    newState.getContracts(tid) shouldBe HashTreePMap.singleton(cid, ())
  }

  it should "set contract pending" in {
    val state = LedgerViewFlowable.LedgerView.create[Unit]()
    val tid = templateId()
    val cid = contractId()
    val commId = commandId()
    val newState = state.addActiveContract(tid, cid, ()).setContractPending(commId, tid, cid)
    newState.getContracts(tid) shouldBe HashTreePMap.empty()
  }

  it should "archive a contract" in {
    val state = LedgerViewFlowable.LedgerView.create[Unit]()
    val tid = templateId()
    val cid = contractId()
    val newState = state.addActiveContract(tid, cid, ()).archiveContract(tid, cid)
    newState.getContracts(tid) shouldBe HashTreePMap.empty()
  }

  val random = new Random()
  val packageId = "packageId"

  def commandId(): String = s"commandId_${random.nextInt()}"
  def templateId(): Identifier =
    new Identifier(packageId, "moduleName", s"templateName_${random.nextInt()}")
  def contractId(): String = s"contractId_${random.nextInt()}"
}
