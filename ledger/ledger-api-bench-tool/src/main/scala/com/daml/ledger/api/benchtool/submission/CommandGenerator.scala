// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.config.WorkflowConfig.SubmissionConfig
import com.daml.ledger.api.v1.commands.{Command, ExerciseCommand}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.lf.value.Value.ContractId
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Foo._

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

case class CreateCmdAndContinuations(
    createCommand: Command,
    continuationF: ContractId => Seq[Command] = _ => Seq.empty,
)

case class TemplateDescriptor(
    templateId: Identifier,
    consumingChoiceName: String,
    nonconsumingChoiceName: String,
)

/** NOTE: Keep me in sync with `Foo.daml`
  */
object TemplateDescriptor {

  val ArchiveChoiceName = "Archive"

  val Foo1: TemplateDescriptor = TemplateDescriptor(
    templateId = com.daml.ledger.test.model.Foo.Foo1.id.asInstanceOf[Identifier],
    consumingChoiceName = "Foo1_ConsumingChoice",
    nonconsumingChoiceName = "Foo1_NonconsumingChoice",
  )
  val Foo2: TemplateDescriptor = TemplateDescriptor(
    templateId = com.daml.ledger.test.model.Foo.Foo2.id.asInstanceOf[Identifier],
    consumingChoiceName = "Foo2_ConsumingChoice",
    nonconsumingChoiceName = "Foo2_NonconsumingChoice",
  )
  val Foo3: TemplateDescriptor = TemplateDescriptor(
    templateId = com.daml.ledger.test.model.Foo.Foo3.id.asInstanceOf[Identifier],
    consumingChoiceName = "Foo3_ConsumingChoice",
    nonconsumingChoiceName = "Foo3_NonconsumingChoice",
  )
}

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

  def next(): Try[CreateCmdAndContinuations] =
    (for {
      (description, observers) <- Try((pickDescription(), pickObservers()))
      createContractPayload <- Try(randomPayload(description.payloadSizeBytes))
      archive <- Try(pickArchive(description))
      command = createContractCommand(
        templateName = description.template,
        signatory = signatory,
        observers = observers,
        payload = createContractPayload,
        archive = archive,
      )
    } yield command).recoverWith { case NonFatal(ex) =>
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
      templateName: String,
      signatory: Primitive.Party,
      observers: List[Primitive.Party],
      payload: String,
      archive: Boolean,
  ): CreateCmdAndContinuations = {
    val consumingExercisePayload: Option[String] = config.consumingExercises
      .flatMap(c =>
        Option.when(randomnessProvider.randomDouble() <= c.probability)(c.payloadSizeBytes)
      )
      .map(randomPayload)
    val nonconsumingExercisePayload: Seq[String] =
      config.nonconsumingExercises.fold(Seq.empty[String]) { c =>
        var f = c.probability.toInt
        if (randomnessProvider.randomDouble() <= c.probability - f) {
          f += 1
        }
        Seq.fill[String](f)(randomPayload(c.payloadSizeBytes))
      }
    val (templateDesc, createCmd) = templateName match {
      case "Foo1" => (TemplateDescriptor.Foo1, Foo1(signatory, observers, payload).create.command)
      case "Foo2" => (TemplateDescriptor.Foo2, Foo2(signatory, observers, payload).create.command)
      case "Foo3" => (TemplateDescriptor.Foo3, Foo3(signatory, observers, payload).create.command)
      case invalid => sys.error(s"Invalid template: $invalid")
    }

    CreateCmdAndContinuations(
      createCommand = createCmd,
      continuationF = {
        def createCont(cid: ContractId): Seq[Command] = {
          val nonconsumingExercises = nonconsumingExercisePayload.map { payload =>
            createExerciseCmd(
              templateId = templateDesc.templateId,
              choiceName = templateDesc.nonconsumingChoiceName,
              argValue = payload,
            )(cid)
          }
          val consumingExerciseO = consumingExercisePayload.fold[Option[Command]](None)(payload =>
            Some(
              createExerciseCmd(
                templateId = templateDesc.templateId,
                choiceName = templateDesc.consumingChoiceName,
                argValue = payload,
              )(cid)
            )
          )
          // NOTE: Archive will be generated only if prior consuming exercise is absent.
          val archiveExerciseO = if (archive && consumingExerciseO.isEmpty) {
            Some(
              doCreateArchiveExerciseCmd(
                templateId = templateDesc.templateId,
                cid = cid,
              )
            )
          } else
            None
          nonconsumingExercises ++ consumingExerciseO.toList ++ archiveExerciseO.toList
        }
        createCont
      },
    )
  }

  private def createExerciseCmd(templateId: Identifier, choiceName: String, argValue: String)(
      cid: ContractId
  ): Command =
    doCreateExerciseCmd(
      templateId = templateId,
      choiceName = choiceName,
      argName = "exercisePayload",
      argValue = argValue,
      cid = cid,
    )

  private def doCreateExerciseCmd(
      templateId: Identifier,
      choiceName: String,
      argName: String,
      argValue: String,
      cid: ContractId,
  ): Command = {
    val c: Command = Command(
      command = Command.Command.Exercise(
        value = ExerciseCommand(
          templateId = Some(templateId),
          contractId = cid.coid,
          choice = choiceName,
          choiceArgument = Some(
            Value(
              Value.Sum.Record(
                Record(
                  None,
                  Seq(
                    RecordField(
                      label = argName,
                      value = Some(Value(Value.Sum.Text(argValue))),
                    )
                  ),
                )
              )
            )
          ),
        )
      )
    )
    c
  }

  private def doCreateArchiveExerciseCmd(templateId: Identifier, cid: ContractId): Command = {
    val c: Command = Command(
      command = Command.Command.Exercise(
        value = ExerciseCommand(
          templateId = Some(templateId),
          contractId = cid.coid,
          choice = TemplateDescriptor.ArchiveChoiceName,
          choiceArgument = Some(
            Value(
              Value.Sum.Record(
                Record(
                  None,
                  Seq.empty[RecordField],
                )
              )
            )
          ),
        )
      )
    )
    c
  }

  private def randomPayload(sizeBytes: Int): String =
    new String(randomnessProvider.randomBytes(sizeBytes), StandardCharsets.UTF_8)

}

object CommandGenerator {
  case class CommandGeneratorError(msg: String, cause: Throwable)
      extends RuntimeException(msg, cause)
}
