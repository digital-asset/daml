// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package command

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.data.{ImmArray, Time}

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

  /** Command for exercising a choice on an existing contract
    *
    * @param typeRef    template or interface where the choice is defined
    * @param contractId contract on which the choice is exercised
    * @param choiceId   TypeConId choice
    * @param argument   value passed for the choice
    */
  final case class Exercise(
      typeRef: TypeConRef,
      contractId: Value.ContractId,
      choiceId: ChoiceName,
      argument: Value,
  ) extends ApiCommand

  /** Command for exercising a choice on an existing contract specified by its key
    *
    * @param templateRef template where the choice is defined
    * @param contractKey key of the contract on which the choice is exercised
    * @param choiceId    TypeConId choice
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

  /** Command for creating a contract and exercising a choice
    * on that existing contract within the same transaction
    *
    * @param templateRef     template of the contract
    * @param createArgument value passed to the template
    * @param choiceId       TypeConId choice
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

/** A contract key passed in over the ledger API command submission */
final case class ApiContractKey(
    templateRef: TypeConRef,
    contractKey: Value,
)
