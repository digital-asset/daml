// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen

import com.daml.sample.MyMain
import MyMain.{KeyedNumber, Increment, SimpleListExample}
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.{commands => rpccmd}
import com.daml.ledger.client.binding.{Primitive => P}

import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GeneratedCommandsUT extends AnyWordSpec with Matchers with Inside {
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

  "exercise" should {
    import com.daml.ledger.client.binding.Value.encode
    val imId: P.ContractId[MyMain.InterfaceMixer] = P.ContractId("fakeimid")
    val DirectTemplateId = ApiTypes.TemplateId unwrap MyMain.InterfaceMixer.id

    "invoke directly-defined choices" in {
      inside(imId.exerciseOverloadedInTemplate(alice).command.command) {
        case rpccmd.Command.Command.Exercise(
              rpccmd.ExerciseCommand(
                Some(DirectTemplateId),
                cid,
                "OverloadedInTemplate",
                Some(choiceArg),
              )
            ) =>
          cid should ===(imId)
          choiceArg should ===(encode(MyMain.OverloadedInTemplate()))
      }
    }

    "invoke interface-inherited choices, directly from template" in {
      inside(imId.exerciseInheritedOnly(alice).command.command) {
        case rpccmd.Command.Command.Exercise(
              rpccmd.ExerciseCommand(
                Some(DirectTemplateId), // TODO(#13349, #13668) must be interface ID
                cid,
                "InheritedOnly",
                Some(choiceArg),
              )
            ) =>
          cid should ===(imId)
          choiceArg should ===(encode(MyMain.InheritedOnly()))
      }
    }
  }

  "key" should {
    "make an exercise-by-key command" in {
      inside((KeyedNumber.key(alice).exerciseIncrement(alice, 42)).command.command) {
        case rpccmd.Command.Command.ExerciseByKey(
              rpccmd.ExerciseByKeyCommand(Some(tid), Some(k), "Increment", Some(choiceArg))
            ) =>
          import com.daml.ledger.client.binding.Value.encode
          tid should ===(KeyedNumber.id)
          k should ===(encode(alice))
          choiceArg should ===(encode(Increment(42)))
      }
    }
  }
}
