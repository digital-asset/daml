// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.api.v2.commands.{Command, CreateCommand, ExerciseByKeyCommand}
import com.daml.ledger.api.v2.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig
import com.digitalasset.canton.ledger.api.benchtool.infrastructure.TestDars
import com.digitalasset.canton.ledger.api.benchtool.submission.foo.RandomPartySelecting
import com.digitalasset.daml.lf.data.Ref

import java.util.concurrent.atomic.AtomicLong
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

/** @param divulgeesToDivulgerKeyMap
  *   map whose keys are sorted divulgees lists
  */
final class FooCommandGenerator(
    config: FooSubmissionConfig,
    allocatedParties: AllocatedParties,
    divulgeesToDivulgerKeyMap: Map[Set[Party], Value],
    names: Names,
    partySelecting: RandomPartySelecting,
    randomnessProvider: RandomnessProvider,
) extends SimpleCommandGenerator {

  private val packageRef: Ref.PackageRef = TestDars.benchtoolDarPackageRef

  private val activeContractKeysPool = new ActiveContractKeysPool[Value](randomnessProvider)

  private val contractDescriptions = new Distribution[FooSubmissionConfig.ContractDescription](
    weights = config.instanceDistribution.map(_.weight),
    items = config.instanceDistribution.toIndexedSeq,
  )

  private val userIdsDistributionO: Option[Distribution[FooSubmissionConfig.UserId]] =
    Option.when(config.userIds.nonEmpty)(
      new Distribution(
        weights = config.userIds.map(_.weight),
        items = config.userIds.toIndexedSeq,
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
        templateDescriptor = FooTemplateDescriptor
          .forName(templateName = contractDescription.template, packageId = packageRef.toString),
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

  override def nextUserId(): String =
    userIdsDistributionO.fold(
      names.benchtoolUserId
    )(userIdsDistribution =>
      userIdsDistribution
        .choose(randomnessProvider.randomDouble())
        .userId
    )

  override def nextExtraCommandSubmitters(): List[Party] =
    partySelecting.nextExtraSubmitter()

  private def pickContractDescription(): FooSubmissionConfig.ContractDescription =
    contractDescriptions.choose(randomnessProvider.randomDouble())

  private def createCommands(
      templateDescriptor: FooTemplateDescriptor,
      signatory: Party,
      observers: List[Party],
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
        makeCreateFooCommand(
          payload = payload,
          fooKeyId = fooKeyId,
          signatory = signatory,
          observers = observers,
          templateId = templateDescriptor.templateId,
        )
    }
    if (config.allowNonTransientContracts) {
      activeContractKeysPool.addContractKey(templateDescriptor.name, fooContractKey)
    }
    // Non-consuming events
    val nonconsumingExercises: Seq[Command] = makeNonConsumingExerciseCommands(
      templateDescriptor = templateDescriptor,
      fooContractKey = fooContractKey,
    )
    // Consuming events
    val consumingPayloadO: Option[String] = config.consumingExercises
      .flatMap(config =>
        if (randomnessProvider.randomDouble() <= config.probability) {
          Some(randomPayload(config.payloadSizeBytes))
        } else None
      )
    val consumingExerciseO: Option[Command] = consumingPayloadO.map { payload =>
      val selectedActiveFooContractKey =
        if (config.allowNonTransientContracts) {
          // This can choose at random a key of any the previously generated contracts.
          activeContractKeysPool.getAndRemoveContractKey(templateDescriptor.name)
        } else {
          // This is always the key of the contract created in this batch of commands.
          fooContractKey
        }
      divulgerContractKeyO match {
        case Some(divulgerContractKey) =>
          makeDivulgedConsumeExerciseCommand(
            templateDescriptor = templateDescriptor,
            fooContractKey = selectedActiveFooContractKey,
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
          )(contractKey = selectedActiveFooContractKey)
      }
    }
    Seq(createFooCmd) ++ nonconsumingExercises ++ consumingExerciseO.toList
  }

  private def makeDivulgedConsumeExerciseCommand(
      templateDescriptor: FooTemplateDescriptor,
      fooContractKey: Value,
      payload: String,
      divulgerContractKey: Value,
  ): Command =
    makeExerciseByKeyCommand(
      templateId = FooTemplateDescriptor.divulgerTemplateId(packageId = packageRef.toString),
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

  private def makeNonConsumingExerciseCommands(
      templateDescriptor: FooTemplateDescriptor,
      fooContractKey: Value,
  ): Seq[Command] = {
    val nonconsumingExercisePayloads: Seq[String] =
      config.nonConsumingExercises.fold(Seq.empty[String]) { config =>
        val p = config.probability.toInt
        val f =
          if (randomnessProvider.randomDouble() <= config.probability - p) {
            p + 1
          } else
            p
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

  private def makeCreateFooCommand(
      payload: String,
      fooKeyId: String,
      signatory: Party,
      observers: List[Party],
      templateId: Identifier,
  ) = {
    val createArguments: Option[Record] = Some(
      Record(
        None,
        Seq(
          RecordField(
            label = "signatory",
            value = Some(Value(Value.Sum.Party(signatory.getValue))),
          ),
          RecordField(
            label = "observers",
            value = Some(
              Value(
                Value.Sum.List(
                  com.daml.ledger.api.v2.value.List(
                    observers.map(obs => Value(Value.Sum.Party(obs.getValue)))
                  )
                )
              )
            ),
          ),
          RecordField(
            label = "payload",
            value = Some(Value(Value.Sum.Text(payload))),
          ),
          RecordField(
            label = "keyId",
            value = Some(Value(Value.Sum.Text(fooKeyId))),
          ),
        ),
      )
    )
    val c: Command = Command(
      command = Command.Command.Create(
        CreateCommand(
          templateId = Some(templateId),
          createArguments = createArguments,
        )
      )
    )
    c
  }

  private def makeCreateAndDivulgeFooCommand(
      divulgerContractKey: Value,
      payload: String,
      fooKeyId: String,
      observers: List[Party],
      templateName: String,
  ) =
    makeExerciseByKeyCommand(
      templateId = FooTemplateDescriptor.divulgerTemplateId(packageId = packageRef.toString),
      choiceName = FooTemplateDescriptor.Divulger_DivulgeContractImmediate,
      args = Seq(
        RecordField(
          label = "fooObservers",
          value = Some(
            Value(
              Value.Sum.List(
                com.daml.ledger.api.v2.value.List(
                  observers.map(obs => Value(Value.Sum.Party(obs.getValue)))
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

  /** @return
    *   A DAML tuple of type `(Party, Text)`
    */
  private[submission] def makeContractKeyValue(
      party: Party,
      keyId: String,
  ): Value =
    Value(
      Value.Sum.Record(
        Record(
          None,
          Seq(
            RecordField(
              value = Some(Value(Value.Sum.Party(party.getValue)))
            ),
            RecordField(
              value = Some(Value(Value.Sum.Text(keyId)))
            ),
          ),
        )
      )
    )

  final case class CommandGeneratorError(msg: String, cause: Throwable)
      extends RuntimeException(msg, cause)

  private[submission] def randomPayload(
      randomnessProvider: RandomnessProvider,
      sizeBytes: Int,
  ): String =
    randomnessProvider.randomAsciiString(sizeBytes)

}
