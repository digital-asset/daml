// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.data.Ref._
import com.daml.lf.value.Value

// ---------------------------
// commands accepted by engine
// ---------------------------
sealed abstract class Action extends Product with Serializable {
  val templateId: TypeConName
}

object Action {

  /** Action for creating a contract
    *
    *  @param templateId identifier of the template that the contract is instantiating
    *  @param argument value passed to the template
    */
  final case class Create(templateId: Identifier, argument: Value[Value.ContractId]) extends Action

  /** Action for exercising a choice on an existing contract
    *
    *  @param templateId identifier of the original contract
    *  @param contractId contract on which the choice is exercised
    *  @param choiceId identifier choice
    *  @param argument value passed for the choice
    */
  final case class Exercise(
      templateId: Identifier,
      contractId: Value.ContractId,
      choiceId: ChoiceName,
      argument: Value[Value.ContractId],
  ) extends Action

  /** Action for exercising a choice on an existing contract specified by its key
    *
    *  @param templateId identifier of the original contract
    *  @param contractKey key of the contract on which the choice is exercised
    *  @param choiceId identifier choice
    *  @param argument value passed for the choice
    */
  final case class ExerciseByKey(
      templateId: Identifier,
      contractKey: Value[Value.ContractId],
      choiceId: ChoiceName,
      argument: Value[Value.ContractId],
  ) extends Action

  /** Action for fetching a an existing contract
    *
    *  @param templateId identifier of the original contract
    *  @param coid contract to fetch
    */
  final case class Fetch(
      templateId: Identifier,
      coid: Value.ContractId,
  ) extends Action

  /** Action for fetching a an existing contract
    *
    *  @param templateId identifier of the original contract
    *  @param key key of the contract on to fecth
    */
  final case class FetchByKey(
      templateId: Identifier,
      key: Value[Value.ContractId],
  ) extends Action

  /** Action for looking if a contract key is active
    *
    *  @param templateId identifier of the original contract
    *  @param key key of the contract on to lookup
    */
  final case class LookupByKey(
      templateId: Identifier,
      key: Value[Value.ContractId],
  ) extends Action

}
