// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig
import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.api.v1.commands.ExerciseByKeyCommand
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Foo._
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong

import com.daml.ledger.client.binding

import scala.util.control.NonFatal
import scala.util.{Failure, Try}

final class FooCommandGenerator(
    randomnessProvider: RandomnessProvider,
    config: FooSubmissionConfig,
    signatory: Primitive.Party,
    allObservers: List[Primitive.Party],
) extends CommandGenerator {
  private val distribution = new Distribution(config.instanceDistribution.map(_.weight))
  private val descriptionMapping: Map[Int, FooSubmissionConfig.ContractDescription] =
    config.instanceDistribution.zipWithIndex
      .map(_.swap)
      .toMap
  private val observersWithUnlikelihood: List[(Primitive.Party, Int)] =
    allObservers.zipWithIndex.toMap.view.mapValues(unlikelihood).toList

  /** @return denominator of a 1/(10**i) likelihood
    */
  private def unlikelihood(i: Int): Int = math.pow(10.0, i.toDouble).toInt

  def next(): Try[Seq[Command]] =
    (for {
      (description, observers) <- Try(
        (pickDescription(), pickObservers())
      )
      createContractPayload <- Try(randomPayload(description.payloadSizeBytes))
      command = createCommands(
        templateDescriptor = FooTemplateDescriptor.forName(description.template),
        signatory = signatory,
        observers = observers,
        payload = createContractPayload,
      )
    } yield command).recoverWith { case NonFatal(ex) =>
      Failure(
        FooCommandGenerator.CommandGeneratorError(
          msg = s"Command generation failed. Details: ${ex.getLocalizedMessage}",
          cause = ex,
        )
      )
    }

  private def pickDescription(): FooSubmissionConfig.ContractDescription =
    descriptionMapping(distribution.index(randomnessProvider.randomDouble()))

  private def pickObservers(): List[Primitive.Party] =
    observersWithUnlikelihood
      .filter { case (_, unlikelihood) => randomDraw(unlikelihood) }
      .map(_._1)

  private def randomDraw(unlikelihood: Int): Boolean =
    randomnessProvider.randomNatural(unlikelihood) == 0

  private def createCommands(
      templateDescriptor: FooTemplateDescriptor,
      signatory: Primitive.Party,
      observers: List[Primitive.Party],
      payload: String,
  ): Seq[Command] = {
    val contractNumber = FooCommandGenerator.nextContractNumber.getAndIncrement()
    val fooKeyId = "foo-" + contractNumber
    val fooContractKey = FooCommandGenerator.makeContractKeyValue(signatory, fooKeyId)
    val createFooCmd = templateDescriptor.name match {
      case "Foo1" => Foo1(signatory, observers, payload, keyId = fooKeyId).create.command
      case "Foo2" => Foo2(signatory, observers, payload, keyId = fooKeyId).create.command
      case "Foo3" => Foo3(signatory, observers, payload, keyId = fooKeyId).create.command
    }
    val nonconsumingExercisePayloads: Seq[String] =
      config.nonConsumingExercises.fold(Seq.empty[String]) { config =>
        var f = config.probability.toInt
        if (randomnessProvider.randomDouble() <= config.probability - f) {
          f += 1
        }
        Seq.fill[String](f)(randomPayload(config.payloadSizeBytes))
      }
    val nonconsumingExercises = nonconsumingExercisePayloads.map { payload =>
      makeExerciseByKeyCommand(
        templateId = templateDescriptor.templateId,
        choiceName = templateDescriptor.nonconsumingChoiceName,
        args = Seq(
          RecordField(
            label = "exercisePayload",
            value = Some(Value(Value.Sum.Text(payload))),
          )
        ),
      )(contractKey = fooContractKey)
    }
    val consumingExerciseO: Option[Command] = config.consumingExercises
      .flatMap(config =>
        if (randomnessProvider.randomDouble() <= config.probability) {
          val payload = randomPayload(config.payloadSizeBytes)
          Some(
            makeExerciseByKeyCommand(
              templateId = templateDescriptor.templateId,
              choiceName = templateDescriptor.consumingChoiceName,
              args = Seq(
                RecordField(
                  label = "exercisePayload",
                  value = Some(Value(Value.Sum.Text(payload))),
                )
              ),
            )(contractKey = fooContractKey)
          )

        } else None
      )
    Seq(createFooCmd) ++ nonconsumingExercises ++ consumingExerciseO.toList
  }

  def makeExerciseByKeyCommand(templateId: Identifier, choiceName: String, args: Seq[RecordField])(
      contractKey: Value
  ): Command = {
    val choiceArgument = Some(
      Value(
        Value.Sum.Record(
          Record(
            None,
            args,
          )
        )
      )
    )
    val c: Command = Command(
      command = Command.Command.ExerciseByKey(
        ExerciseByKeyCommand(
          templateId = Some(templateId),
          contractKey = Some(contractKey),
          choice = choiceName,
          choiceArgument = choiceArgument,
        )
      )
    )
    c
  }

  private def randomPayload(sizeBytes: Int): String =
    FooCommandGenerator.randomPayload(randomnessProvider, sizeBytes)

}

object FooCommandGenerator {

  private[submission] val nextContractNumber = new AtomicLong(0)

  /** @return A DAML tuple of type `(Party, Text)`
    */
  private[submission] def makeContractKeyValue(
      party: binding.Primitive.Party,
      keyId: String,
  ): Value = {
    Value(
      Value.Sum.Record(
        Record(
          None,
          Seq(
            RecordField(
              value = Some(Value(Value.Sum.Party(party.toString)))
            ),
            RecordField(
              value = Some(Value(Value.Sum.Text(keyId)))
            ),
          ),
        )
      )
    )
  }

  case class CommandGeneratorError(msg: String, cause: Throwable)
      extends RuntimeException(msg, cause)

  private[submission] def randomPayload(
      randomnessProvider: RandomnessProvider,
      sizeBytes: Int,
  ): String =
    new String(randomnessProvider.randomBytes(sizeBytes), StandardCharsets.UTF_8)
}
