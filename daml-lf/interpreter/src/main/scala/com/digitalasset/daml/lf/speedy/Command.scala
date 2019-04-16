// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.speedy.SValue._

// ---------------------
// Preprocessed commands
// ---------------------
sealed abstract class Command extends Product with Serializable

object Command {

  final case class Create(
      templateId: Identifier,
      argument: SValue
  ) extends Command

  final case class Exercise(
      templateId: Identifier,
      contractId: SContractId,
      choiceId: String,
      submitter: ImmArray[SParty],
      argument: SValue
  ) extends Command

  final case class Fetch(
      templateId: Identifier,
      coid: SContractId
  ) extends Command

}
