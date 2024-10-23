// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.ledger.javaapi.data.codegen.{
  Contract,
  ContractCompanion,
  ContractId,
  ContractTypeCompanion,
  InterfaceCompanion,
}
import com.daml.ledger.javaapi.data.{
  ArchivedEvent,
  CreatedEvent as JavaCreatedEvent,
  Event,
  Identifier,
  Transaction as JavaTransaction,
  TransactionTree,
  TreeEvent,
}
import com.daml.lf.data.Ref

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

object JavaDecodeUtil {

  private def matchesTemplate[TC](
      companion: ContractTypeCompanion[TC, ?, ?, ?],
      packageName: Option[String],
      templateId: Identifier,
  ): Boolean = {
    val (got, want) = packageName match {
      case Some(name) => {
        val pkgRefForName = Ref.PackageRef.Name(Ref.PackageName.assertFromString(name)).toString
        (
          new Identifier(pkgRefForName, templateId.getModuleName, templateId.getEntityName),
          companion.TEMPLATE_ID,
        )
      }
      case None =>
        // No package name was populated on this event, so we must match against the package id.
        (templateId, companion.TEMPLATE_ID_WITH_PACKAGE_ID)
    }
    got == want
  }

  def decodeCreated[TC](
      companion: ContractCompanion[TC, ?, ?]
  )(event: JavaCreatedEvent): Option[TC] =
    if (matchesTemplate(companion, event.getPackageName.toScala, event.getTemplateId)) {
      Some(companion.fromCreatedEvent(event))
    } else None

  def decodeCreated[Id, View](
      companion: InterfaceCompanion[?, Id, View]
  )(event: JavaCreatedEvent): Option[Contract[Id, View]] =
    if (
      event.getInterfaceViews.keySet.asScala.exists(
        matchesTemplate(companion, event.getPackageName.toScala, _)
      )
    ) {
      Some(companion.fromCreatedEvent(event))
    } else None

  def decodeAllCreated[TC](
      companion: ContractCompanion[TC, ?, ?]
  )(transaction: JavaTransaction): Seq[TC] =
    decodeAllCreatedFromEvents(companion)(transaction.getEvents.asScala.toSeq)

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
      .filter(e => matchesTemplate(companion, e.getPackageName.toScala, e.getTemplateId))
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
      packageName = if (archive.hasPackageName) Some(archive.getPackageName.toString) else None
      if archive.getConsuming && matchesTemplate(
        companion,
        packageName,
        Identifier.fromProto(archive.getTemplateId),
      )
    } yield companion.toContractId(new ContractId(archive.getContractId))

}
