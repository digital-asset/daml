// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.refinements

import com.daml.ledger.api.v1.value.{Identifier, Record, Value}
import scalaz.{@@, Tag}

object ApiTypes {

  sealed trait TransactionIdTag
  type TransactionId = String @@ TransactionIdTag
  val TransactionId = Tag.of[TransactionIdTag]

  sealed trait CommandIdTag
  type CommandId = String @@ CommandIdTag
  val CommandId = Tag.of[CommandIdTag]

  sealed trait WorkflowIdTag
  type WorkflowId = String @@ WorkflowIdTag
  val WorkflowId = Tag.of[WorkflowIdTag]

  sealed trait EventIdTag
  type EventId = String @@ EventIdTag
  val EventId = Tag.of[EventIdTag]

  sealed trait TemplateIdTag
  type TemplateId = Identifier @@ TemplateIdTag
  val TemplateId = Tag.of[TemplateIdTag]

  sealed trait InterfaceIdTag
  type InterfaceId = Identifier @@ InterfaceIdTag
  val InterfaceId = Tag.of[InterfaceIdTag]

  sealed trait ApplicationIdTag
  type ApplicationId = String @@ ApplicationIdTag
  val ApplicationId = Tag.of[ApplicationIdTag]

  sealed trait LedgerIdTag
  type LedgerId = String @@ LedgerIdTag
  val LedgerId = Tag.of[LedgerIdTag]

  sealed trait ContractIdTag
  type ContractId = String @@ ContractIdTag
  val ContractId = Tag.of[ContractIdTag]

  sealed trait ChoiceTag
  type Choice = String @@ ChoiceTag
  val Choice = Tag.of[ChoiceTag]

  sealed trait CreateArgumentsTag
  type CreateArguments = Record @@ CreateArgumentsTag
  val CreateArguments = Tag.of[CreateArgumentsTag]

  sealed trait ChoiceArgumentTag
  type ChoiceArgument = Value @@ ChoiceArgumentTag
  val ChoiceArgument = Tag.of[ChoiceArgumentTag]

  sealed trait PartyTag
  type Party = String @@ PartyTag
  val Party = Tag.of[PartyTag]

}
