// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.api.v1.value as V
import com.daml.ledger.api.v2.transaction.{
  Transaction as TransactionV2,
  TransactionTree as TransactionTreeV2,
}
import com.daml.ledger.client.binding.{Contract, Primitive as P, Template, TemplateCompanion}

object DecodeUtil {
  def decodeAllCreated[T](
      companion: TemplateCompanion[T]
  )(transaction: Transaction): Seq[Contract[T]] =
    decodeAllCreatedFromEvents(companion)(transaction.events)

  def decodeAllCreatedV2[T](
      companion: TemplateCompanion[T]
  )(transaction: TransactionV2): Seq[Contract[T]] =
    decodeAllCreatedFromEvents(companion)(transaction.events)

  def decodeAllCreatedFromEvents[T](
      companion: TemplateCompanion[T]
  )(events: Seq[Event]): Seq[Contract[T]] =
    for {
      event <- events
      created <- event.event.created.toList
      a <- decodeCreated(companion)(created).toList
    } yield a

  def decodeAllArchived[T](
      companion: TemplateCompanion[T]
  )(transaction: Transaction): Seq[P.ContractId[T]] =
    decodeAllArchivedFromEvents(companion)(transaction.events)

  def decodeAllArchivedV2[T](
      companion: TemplateCompanion[T]
  )(transaction: TransactionV2): Seq[P.ContractId[T]] =
    decodeAllArchivedFromEvents(companion)(transaction.events)

  def decodeAllArchivedFromEvents[T](
      companion: TemplateCompanion[T]
  )(events: Seq[Event]): Seq[P.ContractId[T]] =
    for {
      event <- events
      archive <- event.event.archived.toList
      decoded <- decodeArchived(companion)(archive).toList
    } yield decoded

  private def widenToWith[T](value: T)(implicit ev: T <:< Template[T]): T with Template[T] = {
    type W[+A] = A with T
    implicit val liftedEv: W[T] <:< W[Template[T]] = ev.liftCo[W]
    liftedEv.apply(value)
  }

  def decodeCreated[T](
      companion: TemplateCompanion[T]
  )(event: CreatedEvent): Option[Contract[T]] = {
    for {
      record <- event.createArguments: Option[V.Record]
      if event.templateId.exists(templateMatches(companion.id))
      value <- companion.fromNamedArguments(record): Option[T]
      tValue = widenToWith[T](value)(companion.describesTemplate)
    } yield Contract(
      P.ContractId(event.contractId),
      tValue,
      event.agreementText,
      event.signatories,
      event.observers,
      event.contractKey,
    )
  }

  private def templateMatches[A](expected: P.TemplateId[A])(actual: Identifier): Boolean =
    ApiTypes.TemplateId.unwrap(expected) == actual

  def decodeArchived[T](
      companion: TemplateCompanion[T]
  )(event: ArchivedEvent): Option[P.ContractId[T]] =
    Option(event)
      .filter(_.templateId.exists(templateMatches(companion.id)))
      .map(_.contractId)
      .map(P.ContractId.apply)

  def decodeAllCreatedTree[T](
      companion: TemplateCompanion[T]
  )(transaction: TransactionTree): Seq[Contract[T]] =
    decodeAllCreatedTreeFromTreeEvents(companion)(transaction.eventsById)

  def decodeAllArchivedTree[T](
      companion: TemplateCompanion[T]
  )(transaction: TransactionTree): Seq[P.ContractId[T]] =
    decodeAllArchivedTreeFromTreeEvents(companion)(transaction.eventsById)

  def decodeAllCreatedTreeV2[T](
      companion: TemplateCompanion[T]
  )(transaction: TransactionTreeV2): Seq[Contract[T]] =
    decodeAllCreatedTreeFromTreeEvents(companion)(transaction.eventsById)

  def decodeAllArchivedTreeV2[T](
      companion: TemplateCompanion[T]
  )(transaction: TransactionTreeV2): Seq[P.ContractId[T]] =
    decodeAllArchivedTreeFromTreeEvents(companion)(transaction.eventsById)

  def decodeAllCreatedTreeFromTreeEvents[T](
      companion: TemplateCompanion[T]
  )(eventsById: Map[String, TreeEvent]): Seq[Contract[T]] =
    for {
      event <- eventsById.values.toList
      created <- event.kind.created.toList
      a <- decodeCreated(companion)(created).toList
    } yield a

  def decodeAllArchivedTreeFromTreeEvents[T](
      companion: TemplateCompanion[T]
  )(eventsById: Map[String, TreeEvent]): Seq[P.ContractId[T]] =
    for {
      event <- eventsById.values.toList
      archive <- event.kind.exercised.toList.filter(e =>
        e.consuming && e.templateId.fold(false)(templateMatches(companion.id)(_))
      )
    } yield P.ContractId(archive.contractId)

}
