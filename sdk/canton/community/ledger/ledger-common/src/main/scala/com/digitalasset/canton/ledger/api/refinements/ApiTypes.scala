// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.refinements

import com.daml.ledger.api.v2.value.Identifier
import scalaz.{@@, Tag}

object ApiTypes {

  sealed trait UpdateIdTag
  type UpdateId = String @@ UpdateIdTag
  val UpdateId = Tag.of[UpdateIdTag]

  sealed trait CommandIdTag
  type CommandId = String @@ CommandIdTag
  val CommandId = Tag.of[CommandIdTag]

  sealed trait WorkflowIdTag
  type WorkflowId = String @@ WorkflowIdTag
  val WorkflowId = Tag.of[WorkflowIdTag]

  sealed trait TemplateIdTag
  type TemplateId = Identifier @@ TemplateIdTag
  val TemplateId = Tag.of[TemplateIdTag]

  sealed trait InterfaceIdTag
  type InterfaceId = Identifier @@ InterfaceIdTag
  val InterfaceId = Tag.of[InterfaceIdTag]

  sealed trait ApplicationIdTag
  type ApplicationId = String @@ ApplicationIdTag
  val ApplicationId = Tag.of[ApplicationIdTag]

  sealed trait ContractIdTag
  type ContractId = String @@ ContractIdTag
  val ContractId = Tag.of[ContractIdTag]

  sealed trait ChoiceTag
  type Choice = String @@ ChoiceTag
  val Choice = Tag.of[ChoiceTag]

  sealed trait PartyTag
  type Party = String @@ PartyTag
  val Party = Tag.of[PartyTag]

}
