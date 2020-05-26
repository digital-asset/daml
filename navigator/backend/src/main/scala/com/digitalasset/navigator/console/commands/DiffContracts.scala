// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.console._
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.navigator.model
import com.daml.navigator.model.ApiValue
import spray.json.JsValue
import gnieh.diffson.sprayJson._

import scala.util.Try

case object DiffContracts extends SimpleCommand {
  def name: String = "diff_contracts"

  def description: String = "Print diff of two contracts"

  def params: List[Parameter] = List(
    ParameterContractId("id1", "Contract ID"),
    ParameterContractId("id2", "Contract ID")
  )

  def argToJson(arg: ApiValue): JsValue =
    ApiCodecCompressed.apiValueToJsValue(arg)

  def diff(arg1: ApiValue, arg2: ApiValue): PrettyNode = {
    val json1 = argToJson(arg1)
    val json2 = argToJson(arg2)
    val diff = JsonDiff.diff(json1, json2, true)
    if (diff.ops.isEmpty) {
      PrettyPrimitive("(no diff)")
    } else {
      PrettyArray(
        diff.ops
          .map({
            case Add(pointer, value) => s"Added ${pointer.serialize} ($value)"
            case Remove(pointer, None) => s"Removed ${pointer.serialize}"
            case Remove(pointer, Some(old)) => s"Removed ${pointer.serialize} ($old)"
            case Replace(pointer, value, None) => s"Replaced ${pointer.serialize} ($value)"
            case Replace(pointer, value, Some(old)) =>
              s"Replaced ${pointer.serialize} ($old -> $value)"
            case Move(from, path) => s"Moved ${from.serialize} to ${path.serialize}"
            case Copy(from, path) => s"Copied ${from.serialize} to ${path.serialize}"
            case Test(path, value) => s"Tested ${path.serialize} ($value)"
          })
          .map(PrettyPrimitive))
    }
  }

  def diff(c1: model.Contract, c2: model.Contract): PrettyObject = {
    PrettyObject(
      PrettyField("Contract 1", ApiTypes.ContractId.unwrap(c1.id)),
      PrettyField("Contract 2", ApiTypes.ContractId.unwrap(c2.id)),
      PrettyField("Diff", diff(c1.argument, c2.argument))
    )
  }

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      arg1 <- args.headOption ~> "Missing <id1> argument"
      arg2 <- args.drop(1).headOption ~> "Missing <id2> argument"
      ps <- state.getPartyState ~> s"Unknown party ${state.party}"
      types = ps.packageRegistry
      c1 <- ps.ledger.contract(ApiTypes.ContractId(arg1), types) ~> s"Contract '$arg1' not found"
      c2 <- ps.ledger.contract(ApiTypes.ContractId(arg2), types) ~> s"Contract '$arg2' not found"
      diff <- Try(diff(c1, c2)) ~> s"Could not compute the diff"
    } yield {
      (state, Pretty.yaml(diff))
    }
  }

}
