// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.data.Ref.{ChoiceName, Identifier}
import com.daml.lf.speedy.SValue._

// ---------------------
// Preprocessed commands
// ---------------------
private[lf] sealed abstract class Command extends Product with Serializable

private[lf] object Command {

  /** Create a template, not by interface */
  final case class Create(
      templateId: Identifier,
      argument: SValue,
  ) extends Command

  /** Exercise a template choice, not by interface */
  final case class ExerciseTemplate(
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

  /** Exercise an interface choice. This is used for exercising an interface
    * on the ledger api, where the template id is unknown.
    */
  final case class ExerciseInterface(
      interfaceId: Identifier,
      contractId: SContractId,
      choiceId: ChoiceName,
      argument: SValue,
  ) extends Command

  final case class ExerciseByInheritedInterface(
      requiredIface: Identifier,
      requiringIface: Identifier,
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
