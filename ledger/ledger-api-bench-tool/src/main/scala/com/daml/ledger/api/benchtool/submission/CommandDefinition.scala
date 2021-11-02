// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Foo.{ContractDefinition, Factory, Foo1, Foo2, Foo3}

import scala.util.Try

// Stores all information required to create a contract using a template defined in Foo.daml
case class CommandDefinition(
    templateName: String,
    observerUsed: List[Boolean],
    archive: Boolean,
    payload: String,
) {
  // A simple create command
  def toCommand(
      observerNamesWithIndices: List[(Primitive.Party, Int)]
  ): Try[Primitive.Party => Command] = Try {
    val observers: List[Primitive.Party] = observerNamesWithIndices
      .filter { case (_, index) => observerUsed(index) }
      .map(_._1)
    templateName match {
      case "Foo1" => Foo1(_, observers, payload).create.command
      case "Foo2" => Foo2(_, observers, payload).create.command
      case "Foo3" => Foo3(_, observers, payload).create.command
      case invalid => throw new RuntimeException(s"Invalid template: $invalid")
    }
  }

  // A struct that is used for batched contract creation through a factory contract
  // Note: the mapping from templateId to template needs to be synchronized with the Foo.daml file
  def toDamlStruct: Try[ContractDefinition] = Try {
    val templateId: Primitive.Int64 = templateName match {
      case "Foo1" => 1
      case "Foo2" => 2
      case "Foo3" => 3
      case invalid => throw new RuntimeException(s"Invalid template: $invalid")
    }
    ContractDefinition(
      templateId = templateId,
      observerUsed = Primitive.List(observerUsed: _*),
      archive = archive,
      payload = payload,
    )
  }
}

object CommandDefinition {
  // An exercise on a factory contract that will create the given contracts
  def toBatchCommand(
      factoryCid: Primitive.ContractId[Factory],
      signatory: Primitive.Party,
      observers: List[Primitive.Party],
      definitions: List[CommandDefinition],
  ): Try[Command] = Try {
    val damlDefinitions = definitions.map(_.toDamlStruct).map(_.get)
    factoryCid.exerciseCreate(signatory, observers, damlDefinitions).command
  }
}
