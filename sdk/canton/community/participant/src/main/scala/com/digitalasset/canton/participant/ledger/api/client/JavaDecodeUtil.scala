// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.ledger.javaapi.data.codegen.{
  Contract,
  ContractCompanion,
  ContractId,
  InterfaceCompanion,
}
import com.daml.ledger.javaapi.data.{
  ArchivedEvent,
  CreatedEvent as JavaCreatedEvent,
  DisclosedContract,
  Event,
  Identifier,
  Transaction as JavaTransaction,
  TransactionTree,
  TreeEvent,
}
import com.digitalasset.daml.lf.data.Ref

import scala.jdk.CollectionConverters.*

/** Java event decoders
  *
  * If you use scalapb GRPC bindings, then you need to map the events to Java Proto using:
  * JavaCreatedEvent.fromProto(ScalaCreatedEvent.toJavaProto(scalaProtoEvent))
  * javaapi.data.Transaction.fromProto(Transaction.toJavaProto(scalaTx))
  */
object JavaDecodeUtil {

  private def matchesTemplate(
      gotId: Identifier,
      gotPackageName: String,
      wantId: Identifier,
  ): Boolean = {
    val pkgNameRef = Ref.PackageRef.Name(Ref.PackageName.assertFromString(gotPackageName))
    val gotIdPkgName = new Identifier(pkgNameRef.toString, gotId.getModuleName, gotId.getEntityName)
    gotIdPkgName == wantId
  }

  def decodeCreated[TC](
      companion: ContractCompanion[TC, ?, ?]
  )(event: JavaCreatedEvent): Option[TC] =
    if (matchesTemplate(event.getTemplateId, event.getPackageName, companion.TEMPLATE_ID)) {
      Some(companion.fromCreatedEvent(event))
    } else None

  def decodeCreated[Id, View](
      companion: InterfaceCompanion[?, Id, View]
  )(event: JavaCreatedEvent): Option[Contract[Id, View]] =
    if (
      event.getInterfaceViews.keySet.asScala.exists(
        matchesTemplate(_, event.getPackageName, companion.TEMPLATE_ID)
      )
    ) {
      Some(companion.fromCreatedEvent(event))
    } else None

  def flatToCreated(transaction: JavaTransaction): Seq[JavaCreatedEvent] =
    transaction.getEvents.iterator.asScala.collect { case e: JavaCreatedEvent => e }.toSeq

  def decodeAllCreated[TC](
      companion: ContractCompanion[TC, ?, ?]
  )(transaction: JavaTransaction): Seq[TC] =
    decodeAllCreatedFromEvents(companion)(
      transaction.getEvents.iterator.asScala.toSeq
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
      .filter(e => matchesTemplate(e.getTemplateId, e.getPackageName, companion.TEMPLATE_ID))
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
    decodeAllArchivedTreeFromTreeEvents(companion)(transaction.getEventsById.asScala.toMap.map {
      case (nodeId, event) => (nodeId.toInt, event)
    })

  def decodeAllArchivedTreeFromTreeEvents[TCid](
      companion: ContractCompanion[?, TCid, ?]
  )(eventsById: Map[Int, TreeEvent]): Seq[TCid] =
    for {
      event <- eventsById.values.toList
      archive = event.toProtoTreeEvent.getExercised
      if archive.getConsuming && matchesTemplate(
        Identifier.fromProto(archive.getTemplateId),
        archive.getPackageName,
        companion.TEMPLATE_ID,
      )
    } yield companion.toContractId(new ContractId(archive.getContractId))

  def decodeDisclosedContracts(
      transaction: JavaTransaction
  ): Seq[DisclosedContract] =
    toDisclosedContracts(
      synchronizerId = transaction.getSynchronizerId,
      creates = transaction.getEvents.asScala.collect { case createdEvent: JavaCreatedEvent =>
        createdEvent
      }.toSeq,
    )

  private def toDisclosedContract(
      synchronizerId: String,
      create: JavaCreatedEvent,
  ): DisclosedContract = {
    val createdEventBlob = create.getCreatedEventBlob
    if (createdEventBlob.isEmpty)
      throw new IllegalArgumentException(
        s"Cannot decode a disclosed contract from a create event with an empty created event blob for contract-id ${create.getContractId}"
      )
    else
      new DisclosedContract(
        create.getTemplateId,
        create.getContractId,
        createdEventBlob,
        synchronizerId,
      )
  }

  private def toDisclosedContracts(
      synchronizerId: String,
      creates: Seq[JavaCreatedEvent],
  ): Seq[DisclosedContract] =
    creates.map(toDisclosedContract(synchronizerId, _))
}
