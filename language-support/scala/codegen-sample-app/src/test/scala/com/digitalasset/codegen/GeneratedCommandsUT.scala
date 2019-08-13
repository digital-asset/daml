// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import com.digitalasset.sample.MyMain.SimpleListExample
import com.digitalasset.ledger.api.v1.{commands => rpccmd}
import com.digitalasset.ledger.client.binding.{Primitive => P}

import org.scalatest.{Inside, Matchers, WordSpec}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class GeneratedCommandsUT extends WordSpec with Matchers with Inside {
  private val alice = P.Party("Alice")
  private val contract = SimpleListExample(alice, List(42))

  "create" should {
    "make a create command" in {
      inside(contract.create.command.command) {
        case rpccmd.Command.Command.Create(rpccmd.CreateCommand(_, _)) => ()
      }
    }
  }

  "createAnd" should {
    "make a create-and-exercise command" in {
      inside(contract.createAnd.exerciseGo(alice).command.command) {
        case rpccmd.Command.Command
              .CreateAndExercise(rpccmd.CreateAndExerciseCommand(_, _, _, _)) =>
          ()
      }
    }
  }
}
