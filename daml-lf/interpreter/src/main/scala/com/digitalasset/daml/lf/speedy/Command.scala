// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.data.Ref.{ChoiceName, Identifier}
import com.digitalasset.daml.lf.speedy.SValue._

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
  ) extends Command

  final case class Exercise(
      templateId: Identifier,
      contractId: SContractId,
      choiceId: ChoiceName,
      argument: SValue,
  ) extends Command

  final case class ExerciseByKey(
      templateId: Identifier,
      contractKey: SValue,
      choiceId: ChoiceName,
      argument: SValue,
  ) extends Command

  final case class Fetch(
      templateId: Identifier,
      coid: SContractId,
  ) extends Command

  final case class CreateAndExercise(
      templateId: Identifier,
      createArgument: SValue,
      choiceId: ChoiceName,
      choiceArgument: SValue,
  ) extends Command

}
