// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.command

import com.digitalasset.daml.lf.data.Ref.{Identifier, SimpleString}
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.data.{ImmArray, Time}

// ---------------------------------
// Accepted commands coming from API
// ---------------------------------
sealed trait Command extends Product with Serializable

/** Command for creating a contract
  *
  *  @param templateId identifier of the template that the contract is instantiating
  *  @param argument value passed to the template
  */
final case class CreateCommand(templateId: Identifier, argument: VersionedValue[AbsoluteContractId])
    extends Command

/** Command for exercising a choice on an existing contract
  *
  *  @param templateId identifier of the original contract
  *  @param contractId contract on which the choice is exercised
  *  @param choiceId identifier choice
  *  @param submitter party submitting the choice
  *  @param argument value passed for the choice
  */
final case class ExerciseCommand(
    templateId: Identifier,
    contractId: String,
    choiceId: String,
    submitter: SimpleString,
    argument: VersionedValue[AbsoluteContractId])
    extends Command

/** Command for creating a contract and exercising a choice
  * on that existing contract within the same transaction
  *
  *  @param templateId identifier of the original contract
  *  @param createArgument value passed to the template
  *  @param choiceId identifier choice
  *  @param choiceArgument value passed for the choice
  *  @param submitter party submitting the choice
  */
final case class CreateAndExerciseCommand(
    templateId: Identifier,
    createArgument: VersionedValue[AbsoluteContractId],
    choiceId: String,
    choiceArgument: VersionedValue[AbsoluteContractId],
    submitter: SimpleString)
    extends Command

/** Commands input adapted from ledger-api
  *
  *  @param commands a batch of commands to be interpreted/executed
  *  @param ledgerEffectiveTime approximate time the commands to be effective,
  *    interpretation will take this instant
  *  @param commandsReference id passed only for error reporting
  */
case class Commands(
    commands: ImmArray[Command],
    ledgerEffectiveTime: Time.Timestamp,
    commandsReference: String)
