// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import com.digitalasset.ledger.api.refinements.ApiTypes._

import scala.collection.immutable

sealed trait DomainEvent {

  /** The id of the event */
  def eventId: EventId

  /** The id of the target contract */
  def contractId: ContractId

  /** The template ID of the target contract */
  def templateId: TemplateId

  /** Which parties are notified of the events */
  def witnessParties: immutable.Seq[Party]
}

final case class DomainCreatedEvent(
    eventId: EventId,
    contractId: ContractId,
    templateId: TemplateId,
    witnessParties: immutable.Seq[Party],
    createArguments: CreateArguments,
    contractData: Contract.OfAny)
    extends DomainEvent

final case class DomainArchivedEvent(
    eventId: EventId,
    contractId: ContractId,
    templateId: TemplateId,
    witnessParties: immutable.Seq[Party])
    extends DomainEvent
