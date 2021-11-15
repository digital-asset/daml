// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.config.WorkflowConfig.SubmissionConfig
import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Foo._

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

final class CommandGenerator(
    randomnessProvider: RandomnessProvider,
    config: SubmissionConfig,
    signatory: Primitive.Party,
    observers: List[Primitive.Party],
) {
  private val distribution = new Distribution(config.instanceDistribution.map(_.weight))
  private val descriptionMapping: Map[Int, SubmissionConfig.ContractDescription] =
    config.instanceDistribution.zipWithIndex
      .map(_.swap)
      .toMap
  private val observersWithIndices: List[(Primitive.Party, Int)] = observers.zipWithIndex

  def next(): Try[Command] =
    (for {
      (description, observers) <- Try((pickDescription(), pickObservers()))
      payload <- Try(randomPayload(description.payloadSizeBytes))
      archive <- Try(pickArchive(description))
      command <- createContractCommand(
        template = description.template,
        observers = observers,
        payload = payload,
        archive = archive,
      )
    } yield command(signatory)).recoverWith { case NonFatal(ex) =>
      Failure(
        CommandGenerator.CommandGeneratorError(
          msg = s"Command generation failed. Details: ${ex.getLocalizedMessage}",
          cause = ex,
        )
      )
    }

  private def pickDescription(): SubmissionConfig.ContractDescription =
    descriptionMapping(distribution.index(randomnessProvider.randomDouble()))

  private def pickObservers(): List[Primitive.Party] =
    observersWithIndices
      .filter { case (_, index) => isObserverUsed(index) }
      .map(_._1)

  private def pickArchive(description: SubmissionConfig.ContractDescription): Boolean =
    randomnessProvider.randomDouble() < description.archiveChance

  private def isObserverUsed(i: Int): Boolean =
    randomnessProvider.randomNatural(math.pow(10.0, i.toDouble).toInt) == 0

  private def createContractCommand(
      template: String,
      observers: List[Primitive.Party],
      payload: String,
      archive: Boolean,
  ): Try[Primitive.Party => Command] =
    (template, archive) match {
      case ("Foo1", false) => Success(Foo1(_, observers, payload).create.command)
      case ("Foo2", false) => Success(Foo2(_, observers, payload).create.command)
      case ("Foo3", false) => Success(Foo3(_, observers, payload).create.command)
      case ("Foo1", true) =>
        Success(Foo1(_, observers, payload).createAnd.exerciseArchive(signatory).command)
      case ("Foo2", true) =>
        Success(Foo2(_, observers, payload).createAnd.exerciseArchive(signatory).command)
      case ("Foo3", true) =>
        Success(Foo3(_, observers, payload).createAnd.exerciseArchive(signatory).command)
      case invalid => Failure(new RuntimeException(s"Invalid template: $invalid"))
    }

  private def randomPayload(sizeBytes: Int): String =
    new String(randomnessProvider.randomBytes(sizeBytes), StandardCharsets.UTF_8)

}

object CommandGenerator {
  case class CommandGeneratorError(msg: String, cause: Throwable)
      extends RuntimeException(msg, cause)
}
