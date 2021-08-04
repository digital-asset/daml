// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.data.Ref.{ChoiceName, Identifier}
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value.ContractId

// ---------------------
// Preprocessed commands
// ---------------------
sealed abstract class Command extends Product with Serializable {
  val templateId: Identifier
}

object Command {

  final case class Create(
      templateId: Identifier,
      argument: SValue,
      coids: Set[ContractId.V1],
  ) extends Command

  final case class Exercise(
      templateId: Identifier,
      contractId: SContractId,
      choiceId: ChoiceName,
      argument: SValue,
      coids: Set[ContractId.V1],
  ) extends Command

  final case class ExerciseByKey(
      templateId: Identifier,
      contractKey: SValue,
      choiceId: ChoiceName,
      argument: SValue,
      coids: Set[ContractId.V1],
  ) extends Command

  final case class Fetch(
      templateId: Identifier,
      coid: SContractId,
  ) extends Command

  final case class FetchByKey(
      templateId: Identifier,
      key: SValue,
  ) extends Command

  final case class CreateAndExercise(
      templateId: Identifier,
      createArgument: SValue,
      choiceId: ChoiceName,
      choiceArgument: SValue,
      coid: Set[ContractId.V1],
  ) extends Command

  final case class LookupByKey(
      templateId: Identifier,
      contractKey: SValue,
  ) extends Command

}
