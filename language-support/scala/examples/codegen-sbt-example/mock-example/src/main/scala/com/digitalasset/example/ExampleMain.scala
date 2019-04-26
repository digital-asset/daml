// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.example

import java.util.UUID

import com.digitalasset.example.daml.Main.CallablePayout
import com.digitalasset.ledger.client.binding.{Primitive => P}

object ExampleMain extends App {

  val alice = P.Party("Alice")
  val bob = P.Party("Bob")
  val charlie = P.Party("Charlie")

  private def sendCommand[T](command: P.Update[P.ContractId[T]]): Unit =
    println(s"Sending command: $command")

  private def receiveContractIdFromTheLedger[T]: P.ContractId[T] =
    P.ContractId(UUID.randomUUID.toString)

  private def createAndSendCommand(): Unit = {
    val createCommand: P.Update[P.ContractId[CallablePayout]] =
      CallablePayout(giver = alice, receiver = bob).create
    sendCommand(createCommand)
  }

  private def exerciseTransferChoice(): Unit = {
    val givenContractId: P.ContractId[CallablePayout] = receiveContractIdFromTheLedger
    val exerciseCommand: P.Update[P.ContractId[CallablePayout]] =
      givenContractId.exerciseTransfer(actor = bob, newReceiver = charlie)
    sendCommand(exerciseCommand)
  }

  createAndSendCommand()
  exerciseTransferChoice()
}
