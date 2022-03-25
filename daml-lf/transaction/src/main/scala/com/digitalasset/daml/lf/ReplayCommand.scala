// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package command

import com.daml.lf.data.Ref._
import com.daml.lf.value.Value

/** Accepted commands for replay */
sealed abstract class ReplayCommand extends Product with Serializable {
  val templateId: Identifier
}

object ReplayCommand {

  /** Create template contract, by template */
  final case class CreateByTemplate(
      templateId: Identifier,
      argument: Value,
  ) extends ReplayCommand

  /** Create template contract, by interface */
  final case class CreateByInterface(
      interfaceId: Identifier,
      templateId: Identifier,
      argument: Value,
  ) extends ReplayCommand

  /** Exercise a template choice, by template Id or interface Id. */
  // https://github.com/digital-asset/daml/issues/12051
  //   This command is temporary, it will be drop in 2.2
  final case class LenientExercise(
      templateId: Identifier,
      contractId: Value.ContractId,
      choiceId: ChoiceName,
      argument: Value,
  ) extends ReplayCommand

  /** Exercise a template choice, not by interface. */
  final case class ExerciseTemplate(
      templateId: Identifier,
      contractId: Value.ContractId,
      choiceId: ChoiceName,
      argument: Value,
  ) extends ReplayCommand

  /** Exercise a template choice, by interface. */
  final case class ExerciseByInterface(
      interfaceId: Identifier,
      templateId: Identifier,
      contractId: Value.ContractId,
      choiceId: ChoiceName,
      argument: Value,
  ) extends ReplayCommand

  /** Command for exercising a choice on an existing contract specified by its key
    *
    * @param templateId  identifier of the original contract
    * @param contractKey key of the contract on which the choice is exercised
    * @param choiceId    identifier choice
    * @param argument    value passed for the choice
    */
  final case class ExerciseTemplateByKey(
      templateId: Identifier,
      contractKey: Value,
      choiceId: ChoiceName,
      argument: Value,
  ) extends ReplayCommand

  /** Fetch a template, not by interface */
  final case class FetchTemplate(
      templateId: Identifier,
      coid: Value.ContractId,
  ) extends ReplayCommand

  /** Fetch a template, by interface */
  final case class FetchByInterface(
      interfaceId: Identifier,
      templateId: Identifier,
      coid: Value.ContractId,
  ) extends ReplayCommand

  final case class FetchTemplateByKey(
      templateId: Identifier,
      key: Value,
  ) extends ReplayCommand

  final case class LookupTemplateByKey(
      templateId: Identifier,
      contractKey: Value,
  ) extends ReplayCommand
}
