// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig
import com.daml.ledger.api.v1.commands.{Command, ExerciseByKeyCommand}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Foo._

import java.util.concurrent.atomic.AtomicLong
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

case class FooTemplateDescriptor(
    templateId: Identifier,
    consumingChoiceName: String,
    nonconsumingChoiceName: String,
)

/** NOTE: Keep me in sync with `Foo.daml`
  */
object FooTemplateDescriptor {

  val Foo1: FooTemplateDescriptor = FooTemplateDescriptor(
    templateId = com.daml.ledger.test.model.Foo.Foo1.id.asInstanceOf[Identifier],
    consumingChoiceName = "Foo1_ConsumingChoice",
    nonconsumingChoiceName = "Foo1_NonconsumingChoice",
  )
  val Foo2: FooTemplateDescriptor = FooTemplateDescriptor(
    templateId = com.daml.ledger.test.model.Foo.Foo2.id.asInstanceOf[Identifier],
    consumingChoiceName = "Foo2_ConsumingChoice",
    nonconsumingChoiceName = "Foo2_NonconsumingChoice",
  )
  val Foo3: FooTemplateDescriptor = FooTemplateDescriptor(
    templateId = com.daml.ledger.test.model.Foo.Foo3.id.asInstanceOf[Identifier],
    consumingChoiceName = "Foo3_ConsumingChoice",
    nonconsumingChoiceName = "Foo3_NonconsumingChoice",
  )
}

final class FooCommandGenerator(
    randomnessProvider: RandomnessProvider,
    config: FooSubmissionConfig,
    signatory: Primitive.Party,
    observers: List[Primitive.Party],
) extends CommandGenerator {
  private val distribution = new Distribution(config.instanceDistribution.map(_.weight))
  private val descriptionMapping: Map[Int, FooSubmissionConfig.ContractDescription] =
    config.instanceDistribution.zipWithIndex
      .map(_.swap)
      .toMap
  private val observersWithIndices: List[(Primitive.Party, Int)] = observers.zipWithIndex
  private val nextCommandId = new AtomicLong(0)

  def next(): Try[Seq[Command]] =
    (for {
      (description, observers) <- Try((pickDescription(), pickObservers()))
      createContractPayload <- Try(randomPayload(description.payloadSizeBytes))
      command = createContractCommand(
        templateName = description.template,
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
    observersWithIndices
      .filter { case (_, index) => isObserverUsed(index) }
      .map(_._1)

  private def isObserverUsed(i: Int): Boolean =
    randomnessProvider.randomNatural(math.pow(10.0, i.toDouble).toInt) == 0

  private def createContractCommand(
      templateName: String,
      signatory: Primitive.Party,
      observers: List[Primitive.Party],
      payload: String,
  ): Seq[Command] = {
    val commandId = nextCommandId.getAndIncrement()
    val consumingExercisePayload: Option[String] = config.consumingExercises
      .flatMap(c =>
        Option.when(randomnessProvider.randomDouble() <= c.probability)(c.payloadSizeBytes)
      )
      .map(randomPayload)
    val nonconsumingExercisePayload: Seq[String] =
      config.nonConsumingExercises.fold(Seq.empty[String]) { c =>
        var f = c.probability.toInt
        if (randomnessProvider.randomDouble() <= c.probability - f) {
          f += 1
        }
        Seq.fill[String](f)(randomPayload(c.payloadSizeBytes))
      }
    val (templateDesc, createCmd) = templateName match {
      case "Foo1" =>
        (
          FooTemplateDescriptor.Foo1,
          Foo1(signatory, observers, payload, id = commandId).create.command,
        )
      case "Foo2" =>
        (
          FooTemplateDescriptor.Foo2,
          Foo2(signatory, observers, payload, id = commandId).create.command,
        )
      case "Foo3" =>
        (
          FooTemplateDescriptor.Foo3,
          Foo3(signatory, observers, payload, id = commandId).create.command,
        )
      case invalid => sys.error(s"Invalid template: $invalid")
    }

    val contractKey = Value(
      Value.Sum.Record(
        Record(
          None,
          Seq(
            RecordField(
              value = Some(Value(Value.Sum.Party(signatory.toString)))
            ),
            RecordField(
              value = Some(Value(Value.Sum.Int64(commandId)))
            ),
          ),
        )
      )
    )
    val nonconsumingExercises = nonconsumingExercisePayload.map { payload =>
      createExerciseByKeyCmd(
        templateId = templateDesc.templateId,
        choiceName = templateDesc.nonconsumingChoiceName,
        argValue = payload,
      )(contractKey = contractKey)
    }
    val consumingExerciseO = consumingExercisePayload.fold[Option[Command]](None)(payload =>
      Some(
        createExerciseByKeyCmd(
          templateId = templateDesc.templateId,
          choiceName = templateDesc.consumingChoiceName,
          argValue = payload,
        )(contractKey = contractKey)
      )
    )
    Seq(createCmd) ++ nonconsumingExercises ++ consumingExerciseO.toList
  }

  def createExerciseByKeyCmd(templateId: Identifier, choiceName: String, argValue: String)(
      contractKey: Value
  ): Command = {
    val choiceArgument = Some(
      Value(
        Value.Sum.Record(
          Record(
            None,
            Seq(
              RecordField(
                label = "exercisePayload",
                value = Some(Value(Value.Sum.Text(argValue))),
              )
            ),
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
  case class CommandGeneratorError(msg: String, cause: Throwable)
      extends RuntimeException(msg, cause)

  private[submission] def randomPayload(
      randomnessProvider: RandomnessProvider,
      sizeBytes: Int,
  ): String = {
    randomnessProvider.randomAsciiString(sizeBytes)
  }

}
