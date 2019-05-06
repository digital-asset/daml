// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.DefinitionRef
import com.digitalasset.daml.lf.speedy.SValue._

// ---------------------
// Preprocessed commands
// ---------------------
sealed abstract class Command extends Product with Serializable

object Command {

  final case class Create(
      templateId: DefinitionRef,
      argument: SValue
  ) extends Command

  final case class Exercise(
      templateId: DefinitionRef,
      contractId: SContractId,
      choiceId: String,
      submitter: ImmArray[SParty],
      argument: SValue
  ) extends Command

  final case class Fetch(
      templateId: DefinitionRef,
      coid: SContractId
  ) extends Command

  final case class CreateAndExercise(
      templateId: DefinitionRef,
      createArgument: SValue,
      choiceId: String,
      choiceArgument: SValue,
      submitter: ImmArray[SParty]
  ) extends Command

}
