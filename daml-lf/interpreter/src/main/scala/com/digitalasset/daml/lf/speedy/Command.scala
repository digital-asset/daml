// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{ChoiceName, Identifier}
import com.daml.lf.speedy.SValue._

// ---------------------
// Preprocessed commands
// ---------------------
sealed abstract class Command extends Product with Serializable {
  // The set of root template ids from which all dependencies can be calculated.
  // This is a singleton set for actions but can contain multiple elements
  // for rollback commands.
  val templateIds: Set[Identifier]
}

// Commands acting on a specific template. This roughly
// corresponds to what the ledger model calls an action.
sealed abstract class TemplateCommand extends Command {
  val templateId: Identifier
  override val templateIds = Set(templateId)
}

object Command {
  // This does not correspond to a Ledger API command but will be used for projections.
  // Note that this is not the same as a try/catch. Instead this will always create a rollback
  // node regardless of whether an exception is thrown or not
  // (we do not know if our projection included the throw).
  final case class Rollback(
      children: ImmArray[Command]
  ) extends Command {
    override val templateIds =
      children.foldLeft[Set[Identifier]](Set.empty)((acc, child) => acc union child.templateIds)
  }

  final case class Create(
      templateId: Identifier,
      argument: SValue,
  ) extends TemplateCommand

  final case class Exercise(
      templateId: Identifier,
      contractId: SContractId,
      choiceId: ChoiceName,
      argument: SValue,
  ) extends TemplateCommand

  final case class ExerciseByKey(
      templateId: Identifier,
      contractKey: SValue,
      choiceId: ChoiceName,
      argument: SValue,
  ) extends TemplateCommand

  final case class Fetch(
      templateId: Identifier,
      coid: SContractId,
  ) extends TemplateCommand

  final case class FetchByKey(
      templateId: Identifier,
      key: SValue,
  ) extends TemplateCommand

  final case class CreateAndExercise(
      templateId: Identifier,
      createArgument: SValue,
      choiceId: ChoiceName,
      choiceArgument: SValue,
  ) extends TemplateCommand

  final case class LookupByKey(
      templateId: Identifier,
      contractKey: SValue,
  ) extends TemplateCommand

}
