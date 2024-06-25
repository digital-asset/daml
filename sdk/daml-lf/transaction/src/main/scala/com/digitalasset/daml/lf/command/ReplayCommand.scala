// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package command

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.value.Value

/** Accepted commands for replay */
sealed abstract class ReplayCommand extends Product with Serializable {
  val templateId: TypeConName
}

object ReplayCommand {

  /** Create template contract, by template */
  final case class Create(
      templateId: Identifier,
      argument: Value,
  ) extends ReplayCommand

  /** Exercise a template choice, by template Id or interface Id. */
  final case class Exercise(
      templateId: TypeConName,
      interfaceId: Option[TypeConName],
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
  final case class ExerciseByKey(
      templateId: Identifier,
      contractKey: Value,
      choiceId: ChoiceName,
      argument: Value,
  ) extends ReplayCommand

  /** Fetch a template, not by interface */
  final case class Fetch(
      templateId: Identifier,
      coid: Value.ContractId,
  ) extends ReplayCommand

  final case class FetchByKey(
      templateId: Identifier,
      key: Value,
  ) extends ReplayCommand

  final case class LookupByKey(
      templateId: Identifier,
      contractKey: Value,
  ) extends ReplayCommand
}
