// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Foo.Factory

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

final class CommandGenerator(
    randomnessProvider: RandomnessProvider,
    descriptor: ContractSetDescriptor,
    signatory: Primitive.Party,
    observers: List[Primitive.Party],
) {
  private val distribution = new Distribution(descriptor.instanceDistribution.map(_.weight))
  private val descriptionMapping: Map[Int, ContractSetDescriptor.ContractDescription] =
    descriptor.instanceDistribution.zipWithIndex
      .map(_.swap)
      .toMap
  private val observersWithIndices: List[(Primitive.Party, Int)] = observers.zipWithIndex

  def nextSingle(): Try[Command] =
    (for {
      defn <- nextDefinition()
      cmd <- defn.toCommand(observersWithIndices)
    } yield cmd(signatory)).recoverWith { case NonFatal(ex) =>
      Failure(
        CommandGenerator.CommandGeneratorError(
          msg = s"Command generation failed. Details: ${ex.getLocalizedMessage}",
          cause = ex,
        )
      )
    }

  def nextBatch(
      factoryCid: String,
      submissionBatchSize: Int,
  ): Try[Command] =
    (for {
      damlDefinitions <- Try((1 to submissionBatchSize).map(_ => nextDefinition().get).toList)
      typedCid <- Try(Primitive.ContractId[Factory](factoryCid))
      command <- CommandDefinition.toBatchCommand(typedCid, signatory, observers, damlDefinitions)
    } yield command).recoverWith { case NonFatal(ex) =>
      Failure(
        CommandGenerator.CommandGeneratorError(
          msg = s"Batch command generation failed. Details: ${ex.getLocalizedMessage}",
          cause = ex,
        )
      )
    }

  private def nextDefinition(): Try[CommandDefinition] =
    for {
      description <- Try(pickDescription())
      observers <- Try(pickObservers())
      payload <- Try(randomPayload(description.payloadSizeBytes))
      archive <- Try(pickArchive(description))
    } yield CommandDefinition(
      templateName = description.template,
      observerUsed = observers,
      archive = archive,
      payload = payload,
    )

  private def pickDescription(): ContractSetDescriptor.ContractDescription =
    descriptionMapping(distribution.index(randomnessProvider.randomDouble()))

  private def pickObservers(): List[Boolean] =
    observersWithIndices
      .map { case (_, index) => isObserverUsed(index) }

  private def isObserverUsed(i: Int): Boolean =
    randomnessProvider.randomNatural(math.pow(10.0, i.toDouble).toInt) == 0

  private def randomPayload(sizeBytes: Int): String =
    new String(randomnessProvider.randomBytes(sizeBytes), StandardCharsets.UTF_8)

  private def pickArchive(description: ContractSetDescriptor.ContractDescription): Boolean =
    randomnessProvider.randomDouble() < description.archiveChance
}

object CommandGenerator {
  case class CommandGeneratorError(msg: String, cause: Throwable)
      extends RuntimeException(msg, cause)

  def createFactoryCommand(signatory: Primitive.Party): Command =
    Factory(signatory).create.command
}
