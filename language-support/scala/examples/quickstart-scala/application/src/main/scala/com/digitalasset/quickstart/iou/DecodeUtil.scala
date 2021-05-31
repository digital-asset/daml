// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou

import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.{value => V}
import com.daml.ledger.client.binding.{Contract, Template, ValueDecoder, Primitive => P}

object DecodeUtil {
  def decodeAllCreated[A <: Template[A]: ValueDecoder](transaction: Transaction): Seq[Contract[A]] =
    for {
      event <- transaction.events
      created <- event.event.created.toList
      a <- decodeCreated(created).toList
    } yield a

  def decodeCreated[A <: Template[A]: ValueDecoder](transaction: Transaction): Option[Contract[A]] =
    for {
      event <- transaction.events.headOption: Option[Event]
      created <- event.event.created: Option[CreatedEvent]
      a <- decodeCreated(created)
    } yield a

  def decodeCreated[A <: Template[A]: ValueDecoder](event: CreatedEvent): Option[Contract[A]] = {
    val decoder = implicitly[ValueDecoder[A]]
    for {
      record <- event.createArguments: Option[V.Record]
      a <- decoder.read(V.Value.Sum.Record(record)): Option[A]
    } yield Contract(
      P.ContractId(event.contractId),
      a,
      event.agreementText,
      event.signatories,
      event.observers,
      event.contractKey,
    )
  }

  def decodeArchived[A <: Template[A]](transaction: Transaction): Option[P.ContractId[A]] = {
    for {
      event <- transaction.events.headOption: Option[Event]
      archived <- event.event.archived: Option[ArchivedEvent]
    } yield P.ContractId(archived.contractId)
  }
}
