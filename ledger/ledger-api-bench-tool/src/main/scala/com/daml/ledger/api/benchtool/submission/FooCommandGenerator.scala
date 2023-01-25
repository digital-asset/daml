// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import java.util.concurrent.atomic.AtomicLong

import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig
import com.daml.ledger.api.benchtool.submission.foo.RandomPartySelecting
import com.daml.ledger.api.v1.commands.{Command, ExerciseByKeyCommand}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.client.binding
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.benchtool.Foo._

import scala.util.control.NonFatal
import scala.util.{Failure, Try}

/** @param divulgeesToDivulgerKeyMap map whose keys are sorted divulgees lists
  */
final class FooCommandGenerator(
    config: FooSubmissionConfig,
    allocatedParties: AllocatedParties,
    divulgeesToDivulgerKeyMap: Map[Set[Primitive.Party], Value],
    names: Names,
    partySelecting: RandomPartySelecting,
    contractDescriptionRandomnessProvider: RandomnessProvider,
    payloadRandomnessProvider: RandomnessProvider,
    consumingEventsRandomnessProvider: RandomnessProvider,
    nonConsumingEventsRandomnessProvider: RandomnessProvider,
    applicationIdRandomnessProvider: RandomnessProvider,
) extends CommandGenerator {
  private val contractDescriptions = new Distribution[FooSubmissionConfig.ContractDescription](
    weights = config.instanceDistribution.map(_.weight),
    items = config.instanceDistribution.toIndexedSeq,
  )

  private val applicationIdsDistributionO: Option[Distribution[FooSubmissionConfig.ApplicationId]] =
    Option.when(config.applicationIds.nonEmpty)(
      new Distribution(
        weights = config.applicationIds.map(_.weight),
        items = config.applicationIds.toIndexedSeq,
      )
    )

  override def next(): Try[Seq[Command]] =
    (for {
      (contractDescription, partySelection) <- Try(
        (
          pickContractDescription(),
          partySelecting.nextPartiesForContracts(),
        )
      )
      divulgees = partySelection.divulgees.toSet
      createContractPayload <- Try(randomPayload(contractDescription.payloadSizeBytes))
      command = createCommands(
        templateDescriptor = FooTemplateDescriptor.forName(contractDescription.template),
        signatory = allocatedParties.signatory,
        observers = partySelection.observers,
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

  override def nextApplicationId(): String = {
    applicationIdsDistributionO.fold(
      names.benchtoolApplicationId
    )(applicationIdsDistribution =>
      applicationIdsDistribution
        .choose(applicationIdRandomnessProvider.randomDouble())
        .applicationId
    )
  }

  override def nextExtraCommandSubmitters(): List[Primitive.Party] = {
    partySelecting.nextExtraSubmitter()
  }

  private def pickContractDescription(): FooSubmissionConfig.ContractDescription =
    contractDescriptions.choose(contractDescriptionRandomnessProvider.randomDouble())

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
    // Create events
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
    // Non-consuming events
    val nonconsumingExercises: Seq[Command] = makeNonConsumingExerciseCommands(
      templateDescriptor = templateDescriptor,
      fooContractKey = fooContractKey,
    )
    // Consuming events
    val consumingPayloadO: Option[String] = config.consumingExercises
      .flatMap(config =>
        if (consumingEventsRandomnessProvider.randomDouble() <= config.probability) {
          Some(randomPayload(config.payloadSizeBytes))
        } else None
      )
    val consumingExerciseO: Option[Command] = consumingPayloadO.map { payload =>
      divulgerContractKeyO match {
        case Some(divulgerContractKey) =>
          makeDivulgedConsumeExerciseCommand(
            templateDescriptor = templateDescriptor,
            fooContractKey = fooContractKey,
            payload = payload,
            divulgerContractKey = divulgerContractKey,
          )

        case None =>
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
      }
    }
    Seq(createFooCmd) ++ nonconsumingExercises ++ consumingExerciseO.toList
  }

  private def makeDivulgedConsumeExerciseCommand(
      templateDescriptor: FooTemplateDescriptor,
      fooContractKey: Value,
      payload: String,
      divulgerContractKey: Value,
  ): Command = {
    makeExerciseByKeyCommand(
      templateId = FooTemplateDescriptor.Divulger_templateId,
      choiceName = FooTemplateDescriptor.Divulger_DivulgeConsumingExercise,
      args = Seq(
        RecordField(
          label = "fooTemplateName",
          value = Some(Value(Value.Sum.Text(templateDescriptor.name))),
        ),
        RecordField(
          label = "fooKey",
          value = Some(fooContractKey),
        ),
        RecordField(
          label = "fooConsumingPayload",
          value = Some(Value(Value.Sum.Text(payload))),
        ),
      ),
    )(contractKey = divulgerContractKey)
  }

  private def makeNonConsumingExerciseCommands(
      templateDescriptor: FooTemplateDescriptor,
      fooContractKey: Value,
  ): Seq[Command] = {
    val nonconsumingExercisePayloads: Seq[String] =
      config.nonConsumingExercises.fold(Seq.empty[String]) { config =>
        var f = config.probability.toInt
        if (nonConsumingEventsRandomnessProvider.randomDouble() <= config.probability - f) {
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
    nonconsumingExercises
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
      choiceName = FooTemplateDescriptor.Divulger_DivulgeContractImmediate,
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
    FooCommandGenerator.randomPayload(payloadRandomnessProvider, sizeBytes)

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
  ): String = {
    randomnessProvider.randomAsciiString(sizeBytes)
  }

}
