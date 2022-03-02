// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import java.util.UUID

import com.daml.ledger.client.binding.{Primitive => P}
import com.daml.sample.MyMain.PayOut
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class CodeGenExampleSpec extends AnyWordSpec with Matchers {
  val alice = P.Party("Alice")
  val bob = P.Party("Bob")
  val charlie = P.Party("Charlie")

  "create CallablePayout contract should compile" in {
    import com.daml.sample.MyMain.CallablePayout

    val createCommand: P.Update[P.ContractId[CallablePayout]] =
      CallablePayout(giver = alice, receiver = bob).create
    sendCommand(createCommand)
  }

  "exercise Call choice should compile" in {
    import com.daml.sample.MyMain.CallablePayout

    val givenContractId: P.ContractId[CallablePayout] = receiveContractIdFromTheLedger
    val exerciseCommand: P.Update[P.ContractId[PayOut]] =
      givenContractId.exerciseCall2(actor = alice)
    sendCommand(exerciseCommand)
  }

  "exercise Transfer choice should compile" in {
    import com.daml.sample.MyMain.CallablePayout

    val givenContractId: P.ContractId[CallablePayout] = receiveContractIdFromTheLedger
    val exerciseCommand: P.Update[P.ContractId[CallablePayout]] =
      givenContractId.exerciseTransfer(actor = bob, newReceiver = charlie)
    sendCommand(exerciseCommand)
  }

  "create contract with tuple should compile" in {
    import com.daml.sample.{MyMain, DA}
    val ct = MyMain.Twoples(alice, DA.Types.Tuple2(1, 2))
    val createCommand = ct.create
    sendCommand(createCommand)
  }

  private def sendCommand[T](command: P.Update[P.ContractId[T]]): Assertion =
    command should not be null

  private def receiveContractIdFromTheLedger[T]: P.ContractId[T] =
    P.ContractId(UUID.randomUUID.toString)
}
