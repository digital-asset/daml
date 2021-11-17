// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package command

import com.daml.lf.data.Ref._
import com.daml.lf.value.Value
import com.daml.lf.data.{ImmArray, Time}

// ---------------------------
// commands accepted by engine
// ---------------------------
sealed abstract class Command extends Product with Serializable {
  val templateId: Identifier
}

// ---------------------------------
// Accepted commands coming from API
// ---------------------------------
sealed abstract class ApiCommand extends Command

/** Command for creating a contract
  *
  *  @param templateId identifier of the template that the contract is instantiating
  *  @param argument value passed to the template
  */
final case class CreateCommand(templateId: Identifier, argument: Value) extends ApiCommand

/** Create template contract, by interface */
final case class CreateByInterfaceCommand(
    interfaceId: Identifier,
    templateId: Identifier,
    argument: Value,
) extends Command

/** Command for exercising a choice on an existing contract
  *
  *  @param templateId identifier of the original contract
  *  @param contractId contract on which the choice is exercised
  *  @param choiceId identifier choice
  *  @param argument value passed for the choice
  */
final case class ExerciseCommand(
    templateId: Identifier,
    contractId: Value.ContractId,
    choiceId: ChoiceName,
    argument: Value,
) extends ApiCommand

/** Exercise a template choice, not by interface. */
final case class ExerciseTemplateCommand(
    templateId: Identifier,
    contractId: Value.ContractId,
    choiceId: ChoiceName,
    argument: Value,
) extends Command

/** Exercise a template choice, by interface. */
final case class ExerciseByInterfaceCommand(
    interfaceId: Identifier,
    templateId: Identifier,
    contractId: Value.ContractId,
    choiceId: ChoiceName,
    argument: Value,
) extends Command

/** Command for exercising a choice on an existing contract specified by its key
  *
  *  @param templateId identifier of the original contract
  *  @param contractKey key of the contract on which the choice is exercised
  *  @param choiceId identifier choice
  *  @param argument value passed for the choice
  */
final case class ExerciseByKeyCommand(
    templateId: Identifier,
    contractKey: Value,
    choiceId: ChoiceName,
    argument: Value,
) extends ApiCommand

/** Command for creating a contract and exercising a choice
  * on that existing contract within the same transaction
  *
  *  @param templateId identifier of the original contract
  *  @param createArgument value passed to the template
  *  @param choiceId identifier choice
  *  @param choiceArgument value passed for the choice
  */
final case class CreateAndExerciseCommand(
    templateId: Identifier,
    createArgument: Value,
    choiceId: ChoiceName,
    choiceArgument: Value,
) extends ApiCommand

/** Fetch a template, not by interface */
final case class FetchCommand(
    templateId: Identifier,
    coid: Value.ContractId,
) extends Command

/** Fetch a template, by interface */
final case class FetchByInterfaceCommand(
    interfaceId: Identifier,
    templateId: Identifier,
    coid: Value.ContractId,
) extends Command

final case class FetchByKeyCommand(
    templateId: Identifier,
    key: Value,
) extends Command

final case class LookupByKeyCommand(
    templateId: Identifier,
    contractKey: Value,
) extends Command

/** Commands input adapted from ledger-api
  *
  *  @param commands a batch of commands to be interpreted/executed
  *  @param ledgerEffectiveTime approximate time the commands to be effective,
  *    interpretation will take this instant
  *  @param commandsReference id passed only for error reporting
  */
case class Commands(
    commands: ImmArray[ApiCommand],
    ledgerEffectiveTime: Time.Timestamp,
    commandsReference: String,
)
