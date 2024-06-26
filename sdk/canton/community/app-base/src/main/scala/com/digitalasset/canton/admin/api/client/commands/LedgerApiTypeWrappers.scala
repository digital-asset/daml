// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.reassignment.{AssignedEvent, UnassignedEvent}
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse.ContractEntry
import com.daml.ledger.api.v2.state_service.{
  GetActiveContractsResponse,
  IncompleteAssigned,
  IncompleteUnassigned,
}
import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.daml.lf.data.Time
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.{LfPackageName, LfPackageVersion}
import com.google.protobuf.timestamp.Timestamp

/** Wrapper class to make scalapb LedgerApi classes more convenient to access
  */
object LedgerApiTypeWrappers {
  final case class WrappedContractEntry(entry: GetActiveContractsResponse.ContractEntry) {
    lazy val event: CreatedEvent = (entry match {
      case ContractEntry.Empty => throw new RuntimeException("Found empty contract entry")
      case ContractEntry.ActiveContract(value) => value.createdEvent
      case ContractEntry.IncompleteUnassigned(value) =>
        value.createdEvent
      case ContractEntry.IncompleteAssigned(value) =>
        value.assignedEvent
          .getOrElse(throw new RuntimeException("Found empty assigned event"))
          .createdEvent
    }).getOrElse(throw new RuntimeException("Found empty created event"))

    def arguments: Map[String, Any] =
      event.createArguments.toList.flatMap(_.fields).flatMap(flatten(Seq(), _)).toMap

    def reassignmentCounter: Long = entry match {
      case ContractEntry.Empty => throw new RuntimeException("Found empty contract entry")
      case ContractEntry.ActiveContract(value) => value.reassignmentCounter
      case ContractEntry.IncompleteUnassigned(value) =>
        value.unassignedEvent
          .getOrElse(throw new RuntimeException("Found empty unassigned event"))
          .reassignmentCounter
      case ContractEntry.IncompleteAssigned(value) =>
        value.assignedEvent
          .getOrElse(throw new RuntimeException("Found empty assigned event"))
          .reassignmentCounter
    }

    def contractId: String = event.contractId

    def domainId: Option[String] = entry match {
      case ContractEntry.ActiveContract(value) => Some(value.domainId)
      case _ => None
    }
    def templateId: TemplateId = TemplateId.fromIdentifier(
      event.templateId
        .getOrElse(throw new RuntimeException("Found empty template id"))
    )
  }

  final case class WrappedIncompleteUnassigned(entry: IncompleteUnassigned) {
    private def event: UnassignedEvent = entry.unassignedEvent.getOrElse(
      throw new RuntimeException("Found empty unassigned event")
    )

    lazy val createdEvent: CreatedEvent = entry.createdEvent
      .getOrElse(throw new RuntimeException("Found empty created event"))

    def reassignmentCounter: Long = event.reassignmentCounter
    def contractId: String = createdEvent.contractId
    def unassignId: String = event.unassignId
    def assignmentExclusivity: Option[Timestamp] = event.assignmentExclusivity

    def source: DomainId = DomainId.tryFromString(event.source)
    def target: DomainId = DomainId.tryFromString(event.target)
  }

  final case class WrappedIncompleteAssigned(entry: IncompleteAssigned) {
    private def event: AssignedEvent = entry.assignedEvent.getOrElse(
      throw new RuntimeException("Found empty unassigned event")
    )

    lazy val createdEvent: CreatedEvent = event.createdEvent
      .getOrElse(throw new RuntimeException("Found empty created event"))

    def reassignmentCounter: Long = event.reassignmentCounter
    def contractId: String = createdEvent.contractId

    def source: DomainId = DomainId.tryFromString(event.source)
    def target: DomainId = DomainId.tryFromString(event.target)

  }

  /*
    Provide a few utilities methods on CreatedEvent.
    Notes:
   * We don't use an `implicit class` because it makes the use of pretty
       instances difficult (e.g. for `ledger_api.acs.of_all`).

   * Also, the name of some methods of `WrappedCreatedEvent`, such as `templateId`,
       collides with one of the underlying event.
   */
  final case class WrappedCreatedEvent(event: CreatedEvent) {

    private def corrupt: String = s"corrupt event ${event.eventId} / ${event.contractId}"

    def templateId: TemplateId = {
      TemplateId.fromIdentifier(
        event.templateId.getOrElse(
          throw new IllegalArgumentException(
            s"Template Id not specified for event ${event.eventId} / ${event.contractId}"
          )
        )
      )
    }

    def packageId: String = {
      event.templateId.map(_.packageId).getOrElse(corrupt)
    }

    def arguments: Map[String, Any] =
      event.createArguments.toList.flatMap(_.fields).flatMap(flatten(Seq(), _)).toMap
  }

  private def flatten(prefix: Seq[String], field: RecordField): Seq[(String, Any)] = {
    def extract(args: Value.Sum): Seq[(String, Any)] =
      args match {
        case x: Value.Sum.Record => x.value.fields.flatMap(flatten(prefix :+ field.label, _))
        case x: Value.Sum.Variant => x.value.value.toList.map(_.sum).flatMap(extract)
        case x => Seq(((prefix :+ field.label).mkString("."), x.value))
      }

    field.value.map(_.sum).toList.flatMap(extract)
  }

  /** Holder of "core" contract defining fields (particularly those relevant for importing contracts) */
  final case class ContractData(
      templateId: TemplateId,
      packageName: LfPackageName,
      packageVersion: Option[LfPackageVersion],
      createArguments: Record,
      // track signatories and observers for use as auth validation by daml engine
      signatories: Set[String],
      observers: Set[String],
      inheritedContractId: LfContractId,
      contractSalt: Option[Salt],
      ledgerCreateTime: Option[Time.Timestamp],
  )

}
