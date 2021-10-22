// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.model.Foo._

import java.nio.charset.StandardCharsets

final class CommandGenerator(
    randomnessProvider: RandomnessProvider,
    descriptor: ContractSetDescriptor,
    party: Party,
) {
  private val distribution = new Distribution(descriptor.instanceDistribution.map(_.weight))
  private lazy val descriptionMapping: Map[Int, ContractSetDescriptor.ContractDescription] =
    descriptor.instanceDistribution.zipWithIndex
      .map(_.swap)
      .toMap

  def next(): Either[String, Party => Command] = {
    val description = descriptionMapping(distribution.index(randomnessProvider.randomDouble()))
    createContractCommand(
      template = description.template,
      observers = List(party),
      payload = randomPayload(description.payloadSizeBytes),
    )
  }

  private def createContractCommand(
      template: String,
      observers: List[Party],
      payload: String,
  ): Either[String, Party => Command] = {
    template match {
      case "Foo1" => Right(Foo1(_, observers, payload).create.command)
      case "Foo2" => Right(Foo2(_, observers, payload).create.command)
      case "Foo3" => Right(Foo3(_, observers, payload).create.command)
      case invalid => Left(s"Invalid template: $invalid")
    }
  }

  private def randomPayload(sizeBytes: Int): String =
    new String(randomnessProvider.randomBytes(sizeBytes), StandardCharsets.UTF_8)
}
