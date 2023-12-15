// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package command

import com.daml.lf.data.Ref._
import com.daml.lf.value.Value
import com.daml.lf.data.{ImmArray, Time}

/** Accepted commands coming from API */
sealed abstract class ApiCommand extends Product with Serializable {
  def typeRef: TypeConRef
}

object ApiCommand {

  /** Command for creating a contract
    *
    * @param templateRef the template that the contract is instantiating
    * @param argument    value passed to the template
    */
  final case class Create(templateRef: TypeConRef, argument: Value) extends ApiCommand {
    override def typeRef: TypeConRef = templateRef
  }

  object Create {
    def apply(templateId: TypeConName, argument: Value): Create =
      Create(templateId.toRef, argument)
  }

  /** Command for exercising a choice on an existing contract
    *
    * @param typeRef    template or interface where the choice is defined
    * @param contractId contract on which the choice is exercised
    * @param choiceId   TypeConName choice
    * @param argument   value passed for the choice
    */
  final case class Exercise(
      typeRef: TypeConRef,
      contractId: Value.ContractId,
      choiceId: ChoiceName,
      argument: Value,
  ) extends ApiCommand

  object Exercise {
    def apply(
        typeId: TypeConName,
        contractId: Value.ContractId,
        choiceId: ChoiceName,
        argument: Value,
    ): Exercise =
      Exercise(typeId.toRef, contractId, choiceId, argument)
  }

  /** Command for exercising a choice on an existing contract specified by its key
    *
    * @param templateRef template where the choice is defined
    * @param contractKey key of the contract on which the choice is exercised
    * @param choiceId    TypeConName choice
    * @param argument    value passed for the choice
    */
  final case class ExerciseByKey(
      templateRef: TypeConRef,
      contractKey: Value,
      choiceId: ChoiceName,
      argument: Value,
  ) extends ApiCommand {
    override def typeRef: TypeConRef = templateRef
  }

  object ExerciseByKey {
    def apply(
        templateId: TypeConName,
        contractKey: Value,
        choiceId: ChoiceName,
        argument: Value,
    ): ExerciseByKey =
      ExerciseByKey(templateId.toRef, contractKey, choiceId, argument)
  }

  /** Command for creating a contract and exercising a choice
    * on that existing contract within the same transaction
    *
    * @param templateRef     template of the contract
    * @param createArgument value passed to the template
    * @param choiceId       TypeConName choice
    * @param choiceArgument value passed for the choice
    */
  final case class CreateAndExercise(
      templateRef: TypeConRef,
      createArgument: Value,
      choiceId: ChoiceName,
      choiceArgument: Value,
  ) extends ApiCommand {
    override def typeRef: TypeConRef = templateRef
  }

  object CreateAndExercise {
    def apply(
        templateId: TypeConName,
        createArgument: Value,
        choiceId: ChoiceName,
        choiceArgument: Value,
    ): CreateAndExercise =
      CreateAndExercise(templateId.toRef, createArgument, choiceId, choiceArgument)
  }
}

/** Commands input adapted from ledger-api
  *
  * @param commands            a batch of commands to be interpreted/executed
  * @param ledgerEffectiveTime approximate time the commands to be effective,
  *                            interpretation will take this instant
  * @param commandsReference   id passed only for error reporting
  */
case class ApiCommands(
    commands: ImmArray[ApiCommand],
    ledgerEffectiveTime: Time.Timestamp,
    commandsReference: String,
)

/** An additional contract that is used to resolve contract id and contract key lookups during interpretation.
  *
  * @param templateId   identifier of the template of disclosed contract
  * @param contractId   the contract id of the disclosed contract
  * @param argument     the payload of the disclosed contract
  * @param keyHash        hash of the contract key, if present
  */
final case class DisclosedContract(
    templateId: Identifier,
    contractId: Value.ContractId,
    argument: Value,
    keyHash: Option[crypto.Hash],
)
