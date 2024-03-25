// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.query

import java.time.Instant

import com.daml.navigator.dotnot.PropertyCursor
import com.daml.navigator.model._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.daml.navigator.query.filter._
import com.daml.ledger.api.refinements.ApiTypes
import scalaz.std.function._
import scalaz.syntax.arrow._
import scalaz.syntax.tag._

class FilterSpec extends AnyFlatSpec with Matchers {
  import com.daml.navigator.{DamlConstants => C}

  val choices = List(
    Choice(
      ApiTypes.Choice("call"),
      DamlLfTypeCon(DamlLfTypeConName(C.complexRecordId), DamlLfImmArraySeq()),
      C.simpleUnitT,
      false,
    )
  )
  val template = Template(C.complexRecordId, choices, None, Set.empty)
  val contractId = ApiTypes.ContractId("ContractIou")
  val commandId = ApiTypes.CommandId("Cmd")
  val workflowId = ApiTypes.WorkflowId("Workflow")

  val contract =
    Contract(contractId, template, C.complexRecordV, Some(""), List.empty, List.empty, None)
  val command =
    CreateCommand(commandId, 1, workflowId, Instant.EPOCH, template.id, C.complexRecordV)

  val templateTests = Seq(
    "id" -> template.idString,
    "parameter.fRecord.fA" -> "text",
    "parameter.fText" -> "text",
    "choices.call.parameter.fInt64" -> "int64",
    "choices.call.parameter.fOptionalText" -> "optional",
  ).map((PropertyCursor.fromString _).first)

  for ((cursor, expectedValue) <- templateTests) {
    "templateFilter" should s"match the input $expectedValue in path $cursor for template $template" in {
      templateFilter.run(template, cursor, expectedValue, C.allTypes.get) shouldEqual Right(true)
    }
  }

  val contractTests = Seq(
    "id" -> contractId.unwrap,
    "template.id" -> template.idString,
    "argument.fText" -> "foo",
    "argument.fRecord.fA" -> "foo",
    "argument.fOptionalText" -> "None",
    "argument.fOptionalText.None" -> "",
    "argument.fOptionalUnit" -> "Some",
    "argument.fOptionalUnit.Some" -> "",
    "argument.fOptOptText" -> "Some",
    "argument.fOptOptText.Some" -> "Some",
    "argument.fOptOptText.Some.Some" -> "foo",
  ).map((PropertyCursor.fromString _).first)

  for ((cursor, expectedValue) <- contractTests) {
    "contractFilter" should s"match the input $expectedValue in path $cursor for template $template" in {
      contractFilter.run(contract, cursor, expectedValue, C.allTypes.get) shouldEqual Right(true)
    }
  }

  val commandTests = Seq(
    "id" -> command.id.unwrap,
    "workflowId" -> command.workflowId.unwrap,
    "platformTime" -> command.platformTime.toString,
    "index" -> command.index.toString,
  ).map((PropertyCursor.fromString _).first)

  for ((cursor, expectedValue) <- commandTests) {
    "commandFilter" should s"match the input $expectedValue in path $cursor for command $command" in {
      commandFilter.run(command, cursor, expectedValue, C.allTypes.get) shouldEqual Right(true)
    }
  }
}
