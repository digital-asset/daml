// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.data.Ref.{ChoiceName, Identifier}
import com.daml.lf.speedy.SValue._

// ---------------------
// Preprocessed commands
// ---------------------
sealed abstract class Command extends Product with Serializable {
  val templateId: Identifier
}

object Command {

  /** Create a template, not by interface */
  final case class Create(
      templateId: Identifier,
      argument: SValue,
  ) extends Command

  /** Create a template, by interface */
  final case class CreateByInterface(
      interfaceId: Identifier,
      templateId: Identifier,
      argument: SValue,
  ) extends Command

  /** Exercise a template choice, not by interface */
  final case class Exercise(
      templateId: Identifier,
      contractId: SContractId,
      choiceId: ChoiceName,
      argument: SValue,
  ) extends Command

  /** Exercise a template choice, by interface */
  final case class ExerciseByInterface(
      interfaceId: Identifier,
      templateId: Identifier,
      contractId: SContractId,
      choiceId: ChoiceName,
      argument: SValue,
  ) extends Command

  /** Exercise an interface choice */
  final case class ExerciseInterface(
      interfaceId: Identifier,
      contractId: SContractId,
      choiceId: ChoiceName,
      argument: SValue,
  ) extends Command {
    // TODO https://github.com/digital-asset/daml/issues/11342
    //   The actual template id isn't known until run time.
    //   The interface id is the best we've got.
    val templateId = interfaceId
  }

  final case class ExerciseByKey(
      templateId: Identifier,
      contractKey: SValue,
      choiceId: ChoiceName,
      argument: SValue,
  ) extends Command

  /** Fetch a template, not by interface */
  final case class Fetch(
      templateId: Identifier,
      coid: SContractId,
  ) extends Command

  /** Fetch a template, by interface */
  final case class FetchByInterface(
      interfaceId: Identifier,
      templateId: Identifier,
      coid: SContractId,
  ) extends Command

  /** Fetch an interface */
  final case class FetchInterface(
      interfaceId: Identifier,
      coid: SContractId,
  ) extends Command {
    // TODO https://github.com/digital-asset/daml/issues/11342
    //   Same as above.
    val templateId = interfaceId
  }

  final case class FetchByKey(
      templateId: Identifier,
      key: SValue,
  ) extends Command

  final case class CreateAndExercise(
      templateId: Identifier,
      createArgument: SValue,
      choiceId: ChoiceName,
      choiceArgument: SValue,
  ) extends Command

  final case class LookupByKey(
      templateId: Identifier,
      contractKey: SValue,
  ) extends Command

}
