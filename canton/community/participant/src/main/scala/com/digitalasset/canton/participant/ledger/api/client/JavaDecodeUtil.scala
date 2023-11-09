// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.ledger.api.v2.TransactionOuterClass.Transaction as JavaTransactionV2
import com.daml.ledger.javaapi.data.codegen.{
  Contract,
  ContractCompanion,
  ContractId,
  InterfaceCompanion,
}
import com.daml.ledger.javaapi.data.{
  ArchivedEvent,
  CreatedEvent as JavaCreatedEvent,
  Event,
  Transaction as JavaTransaction,
  TransactionTree,
  TreeEvent,
}

import scala.jdk.CollectionConverters.*

object JavaDecodeUtil {
  def decodeCreated[TC](
      companion: ContractCompanion[TC, ?, ?]
  )(event: JavaCreatedEvent): Option[TC] =
    if (event.getTemplateId == companion.TEMPLATE_ID) {
      Some(companion.fromCreatedEvent(event))
    } else None

  def decodeCreated[Id, View](
      companion: InterfaceCompanion[?, Id, View]
  )(event: JavaCreatedEvent): Option[Contract[Id, View]] =
    if (event.getInterfaceViews.containsKey(companion.TEMPLATE_ID)) {
      Some(companion.fromCreatedEvent(event))
    } else None

  def flatToCreated(transaction: JavaTransaction): Seq[JavaCreatedEvent] =
    transaction.getEvents.iterator.asScala.collect { case e: JavaCreatedEvent => e }.toSeq

  def decodeAllCreated[TC](
      companion: ContractCompanion[TC, ?, ?]
  )(transaction: JavaTransaction): Seq[TC] =
    decodeAllCreatedFromEvents(companion)(transaction.getEvents.asScala.toSeq)

  def decodeAllCreatedV2[TC](
      companion: ContractCompanion[TC, ?, ?]
  )(transaction: JavaTransactionV2): Seq[TC] =
    decodeAllCreatedFromEvents(companion)(
      transaction.getEventsList.asScala.toSeq.map(Event.fromProtoEvent)
    )

  def decodeAllCreatedFromEvents[TC](
      companion: ContractCompanion[TC, ?, ?]
  )(events: Seq[Event]): Seq[TC] =
    for {
      event <- events
      eventP = event.toProtoEvent
      created <- if (eventP.hasCreated) Seq(eventP.getCreated) else Seq()
      a <- decodeCreated(companion)(JavaCreatedEvent.fromProto(created)).toList
    } yield a

  def decodeAllArchived[T](
      companion: ContractCompanion[?, ?, T]
  )(transaction: JavaTransaction): Seq[ContractId[T]] =
    decodeAllArchivedFromEvents(companion)(transaction.getEvents.asScala.toSeq)

  def decodeAllArchivedFromEvents[T](
      companion: ContractCompanion[?, ?, T]
  )(events: Seq[Event]): Seq[ContractId[T]] =
    for {
      event <- events
      eventP = event.toProtoEvent
      if eventP.hasArchived
      archive = ArchivedEvent.fromProto(eventP.getArchived)
      decoded <- decodeArchived(companion)(archive).toList
    } yield decoded

  def decodeArchived[T](
      companion: ContractCompanion[?, ?, T]
  )(event: ArchivedEvent): Option[ContractId[T]] =
    Option(event)
      .filter(_.getTemplateId == companion.TEMPLATE_ID)
      .map(_.getContractId)
      .map(new ContractId[T](_))

  private def treeToCreated(transaction: TransactionTree): Seq[JavaCreatedEvent] =
    transaction.getEventsById.asScala.valuesIterator.collect { case e: JavaCreatedEvent => e }.toSeq

  def decodeAllCreatedTree[TC](
      companion: ContractCompanion[TC, ?, ?]
  )(transaction: TransactionTree): Seq[TC] =
    for {
      created <- treeToCreated(transaction)
      a <- decodeCreated(companion)(created).toList
    } yield a

  def decodeAllArchivedTree[TCid](
      companion: ContractCompanion[?, TCid, ?]
  )(transaction: TransactionTree): Seq[TCid] =
    decodeAllArchivedTreeFromTreeEvents(companion)(transaction.getEventsById.asScala.toMap)

  def decodeAllArchivedTreeFromTreeEvents[TCid](
      companion: ContractCompanion[?, TCid, ?]
  )(eventsById: Map[String, TreeEvent]): Seq[TCid] =
    for {
      event <- eventsById.values.toList
      archive = event.toProtoTreeEvent.getExercised
      if archive.getConsuming && archive.getTemplateId == companion.TEMPLATE_ID.toProto
    } yield companion.toContractId(new ContractId(archive.getContractId))

}
