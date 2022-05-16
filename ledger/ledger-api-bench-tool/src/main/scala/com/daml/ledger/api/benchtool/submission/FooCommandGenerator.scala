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

/** @param divulgeesToDivulgerKeyMap map whose keys are sorted divulgees lists
  */
final class FooCommandGenerator(
    randomnessProvider: RandomnessProvider,
    config: FooSubmissionConfig,
    signatory: Primitive.Party,
    allObservers: List[Primitive.Party],
    allDivulgees: List[Primitive.Party],
    divulgeesToDivulgerKeyMap: Map[Set[Primitive.Party], Value],
) extends CommandGenerator {
  private val distribution = new Distribution(config.instanceDistribution.map(_.weight))
  private val descriptionMapping: Map[Int, FooSubmissionConfig.ContractDescription] =
    config.instanceDistribution.zipWithIndex
      .map(_.swap)
      .toMap
  private val observersWithUnlikelihood: List[(Primitive.Party, Int)] =
    allObservers.zipWithIndex.toMap.view.mapValues(unlikelihood).toList
  private val divulgeesWithUnlikelihood: List[(Primitive.Party, Int)] =
    allDivulgees.zipWithIndex.toMap.view.mapValues(unlikelihood).toList.sortBy { case (party, _) =>
      party.toString
    }

  /** @return denominator of a 1/(10**i) likelihood
    */
  private def unlikelihood(i: Int): Int = math.pow(10.0, i.toDouble).toInt

  def next(): Try[Seq[Command]] =
    (for {
      (description, observers, divulgees) <- Try(
        (pickDescription(), pickObservers(), pickDivulgees())
      )
      createContractPayload <- Try(randomPayload(description.payloadSizeBytes))
      command = createCommands(
        templateDescriptor = FooTemplateDescriptor.forName(description.template),
        signatory = signatory,
        observers = observers,
        divulgerContractKeyO =
          if (divulgees.isEmpty) None else divulgeesToDivulgerKeyMap.get(divulgees),
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

  private def pickDivulgees(): Set[Primitive.Party] =
    divulgeesWithUnlikelihood.view.collect {
      case (party, unlikelihood) if randomDraw(unlikelihood) => party
    }.toSet

  private def randomDraw(unlikelihood: Int): Boolean =
    randomnessProvider.randomNatural(unlikelihood) == 0

  private def createCommands(
      templateDescriptor: FooTemplateDescriptor,
      signatory: Primitive.Party,
      observers: List[Primitive.Party],
      divulgerContractKeyO: Option[Value],
      payload: String,
  ): Seq[Command] = {
    val contractCounter = FooCommandGenerator.nextContractNumber.getAndIncrement()
    val fooKeyId = "foo-" + contractCounter
    val fooContractKey = FooCommandGenerator.makeContractKeyValue(signatory, fooKeyId)
    val createFooCmd = divulgerContractKeyO match {
      case Some(divulgerContractKey) =>
        makeCreateAndDivulgeFooCommand(
          divulgerContractKey = divulgerContractKey,
          payload = payload,
          fooKeyId = fooKeyId,
          observers = observers,
          templateName = templateDescriptor.name,
        )
      case None =>
        templateDescriptor.name match {
          case "Foo1" => Foo1(signatory, observers, payload, keyId = fooKeyId).create.command
          case "Foo2" => Foo2(signatory, observers, payload, keyId = fooKeyId).create.command
          case "Foo3" => Foo3(signatory, observers, payload, keyId = fooKeyId).create.command
        }
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

  private def makeCreateAndDivulgeFooCommand(
      divulgerContractKey: Value,
      payload: String,
      fooKeyId: String,
      observers: List[Primitive.Party],
      templateName: String,
  ) = {
    makeExerciseByKeyCommand(
      templateId = FooTemplateDescriptor.Divulger_templateId,
      choiceName = FooTemplateDescriptor.Divulger_DivulgeImmediate,
      args = Seq(
        RecordField(
          label = "fooObservers",
          value = Some(
            Value(
              Value.Sum.List(
                com.daml.ledger.api.v1.value.List(
                  observers.map(obs => Value(Value.Sum.Party(obs.toString)))
                )
              )
            )
          ),
        ),
        RecordField(
          label = "fooPayload",
          value = Some(Value(Value.Sum.Text(payload))),
        ),
        RecordField(
          label = "fooKeyId",
          value = Some(Value(Value.Sum.Text(fooKeyId))),
        ),
        RecordField(
          label = "fooTemplateName",
          value = Some(Value(Value.Sum.Text(templateName))),
        ),
      ),
    )(contractKey = divulgerContractKey)
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
