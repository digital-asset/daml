// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen

import com.daml.sample.{MyMain, MyMainIface}
import MyMain.{KeyedNumber, Increment, SimpleListExample}
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.{commands => rpccmd}
import com.daml.ledger.client.binding.{Primitive => P}
import com.daml.ledger.client.binding.Value.encode

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

    "include template ID and interface ID" in {
      inside(
        MyMain
          .InterfaceMixer(alice)
          .createAnd
          .toInterface[MyMainIface.IfaceFromAnotherMod]
          .exerciseFromAnotherMod(alice, 42)
          .command
          .command
      ) {
        case rpccmd.Command.Command
              .CreateAndExercise(
                rpccmd.CreateAndExerciseCommand(
                  Some(tpId),
                  Some(payload),
                  "FromAnotherMod",
                  Some(choiceArg),
                )
              ) =>
          tpId should ===(MyMain.InterfaceMixer.id)
          payload should ===(MyMain.InterfaceMixer(alice).arguments)
          choiceArg should ===(encode(MyMainIface.FromAnotherMod(42)))
      }
    }
  }

  "exercise" should {
    val imId: P.ContractId[MyMain.InterfaceMixer] = P.ContractId("fakeimid")
    val itmId: P.ContractId[MyMainIface.IfaceFromAnotherMod] = P.ContractId("fakeitmid")
    val DirectTemplateId = ApiTypes.TemplateId unwrap MyMain.InterfaceMixer.id
    val FAMTemplateId = ApiTypes.TemplateId unwrap MyMainIface.IfaceFromAnotherMod.id

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

    "invoke on an interface-contract ID" in {
      inside(itmId.exerciseFromAnotherMod(alice, 42).command.command) {
        case rpccmd.Command.Command.Exercise(
              rpccmd.ExerciseCommand(
                Some(FAMTemplateId),
                cid,
                "FromAnotherMod",
                Some(choiceArg),
              )
            ) =>
          cid should ===(itmId)
          choiceArg should ===(encode(MyMainIface.FromAnotherMod(42)))
      }
    }
  }

  "template IDs" should {
    "be present on templates" in {
      val sle = ApiTypes.TemplateId unwrap MyMain.SimpleListExample.id
      sle.moduleName should ===("MyMain")
      sle.entityName should ===("SimpleListExample")
    }

    "be present on interfaces" in {
      val sle = ApiTypes.TemplateId unwrap MyMainIface.IfaceFromAnotherMod.id
      sle.moduleName should ===("MyMainIface")
      sle.entityName should ===("IfaceFromAnotherMod")
    }
  }

  "key" should {
    "make an exercise-by-key command" in {
      inside((KeyedNumber.key(alice).exerciseIncrement(alice, 42)).command.command) {
        case rpccmd.Command.Command.ExerciseByKey(
              rpccmd.ExerciseByKeyCommand(Some(tid), Some(k), "Increment", Some(choiceArg))
            ) =>
          tid should ===(KeyedNumber.id)
          k should ===(encode(alice))
          choiceArg should ===(encode(Increment(42)))
      }
    }

    "pass template ID when exercising interface choice" in {
      inside(
        MyMain.InterfaceMixer
          .key(alice)
          .toInterface[MyMainIface.IfaceFromAnotherMod]
          .exerciseFromAnotherMod(alice, 42)
          .command
          .command
      ) {
        case rpccmd.Command.Command.ExerciseByKey(
              rpccmd.ExerciseByKeyCommand(Some(tid), Some(k), "FromAnotherMod", Some(choiceArg))
            ) =>
          tid should ===(MyMain.InterfaceMixer.id)
          k should ===(encode(alice))
          choiceArg should ===(encode(MyMainIface.FromAnotherMod(42)))
      }
    }
  }
}
