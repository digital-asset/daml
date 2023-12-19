// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse.ContractEntry
import com.daml.lf.data.Time
import com.daml.lf.transaction.TransactionCoder
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.protocol.{DriverContractMetadata, LfContractId}

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
    def templateId: TemplateId = TemplateId.fromIdentifier(
      event.templateId
        .getOrElse(throw new RuntimeException("Found empty template id"))
    )
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

    def toContractData: ContractData = {
      val templateId = TemplateId.fromIdentifier(
        event.templateId.getOrElse(throw new IllegalArgumentException("Template Id not specified"))
      )
      val createArguments =
        event.createArguments.getOrElse(
          throw new IllegalArgumentException("Create Arguments not specified")
        )
      val lfContractId =
        LfContractId
          .fromString(event.contractId)
          .getOrElse(
            throw new IllegalArgumentException(s"Illegal Contract Id: ${event.contractId}")
          )

      val contractSaltO = for {
        fatInstance <- TransactionCoder.decodeFatContractInstance(event.createdEventBlob).toOption
        parsed = DriverContractMetadata.fromByteString(fatInstance.cantonData.toByteString)
      } yield parsed.fold[Salt](
        err =>
          throw new IllegalArgumentException(
            s"Could not deserialize driver contract metadata: ${err.message}"
          ),
        _.salt,
      )

      val ledgerCreateTimeO =
        event.createdAt.map(TimestampConversion.toLf(_, TimestampConversion.ConversionMode.Exact))

      ContractData(
        templateId = templateId,
        createArguments = createArguments,
        signatories = event.signatories.toSet,
        observers = event.observers.toSet,
        inheritedContractId = lfContractId,
        contractSalt = contractSaltO,
        ledgerCreateTime = ledgerCreateTimeO,
      )
    }
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
      createArguments: Record,
      // track signatories and observers for use as auth validation by daml engine
      signatories: Set[String],
      observers: Set[String],
      inheritedContractId: LfContractId,
      contractSalt: Option[Salt],
      ledgerCreateTime: Option[Time.Timestamp],
  )

}
