// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model

import java.time.Instant

import com.daml.lf.data.{Ref => DamlLfRef}
import com.daml.ledger.api.refinements.ApiTypes
import scalaz.{@@, Tag}

// ------------------------------------------------------------------------------------------------
// Node
// ------------------------------------------------------------------------------------------------

/** Identifiable entity. */
sealed trait Node[T] {
  def id: T
  def idString: String
}

sealed trait TaggedNode[IdTag] extends Node[String @@ IdTag] {
  def id: String @@ IdTag
  override def idString: String = Tag.unwrap(id)
}

sealed trait DamlLfNode extends Node[DamlLfIdentifier] {
  def id: DamlLfIdentifier
  override def idString: String = id.asOpaqueString
}

// ------------------------------------------------------------------------------------------------
// Commands
// ------------------------------------------------------------------------------------------------

// Status of a command - waiting, failed, succeeded, or unknown
sealed trait CommandStatus {
  def isCompleted: Boolean
}

// Waiting for command completion
final case class CommandStatusWaiting() extends CommandStatus {
  def isCompleted = false
}

// Completed with an error
final case class CommandStatusError(code: String, details: String) extends CommandStatus {
  def isCompleted = true
}

// Completed successfully
final case class CommandStatusSuccess(tx: Transaction) extends CommandStatus {
  def isCompleted = true
}

// Command status tracking failed, status unknown
final case class CommandStatusUnknown() extends CommandStatus {
  def isCompleted = false
}

sealed trait Command extends TaggedNode[ApiTypes.CommandIdTag] {

  /** Order in which the command was submitted. */
  def index: Long
  def workflowId: ApiTypes.WorkflowId
  def platformTime: Instant
}

final case class CreateCommand(
    id: ApiTypes.CommandId,
    index: Long,
    workflowId: ApiTypes.WorkflowId,
    platformTime: Instant,
    template: DamlLfIdentifier,
    argument: ApiRecord,
) extends Command

/** @param template
  *     The template of the given contract. Not required for the ledger API, but we keep
  *     this denormalized information so that it's easier to serialize/deserialize the
  *     choice argument.
  */
final case class ExerciseCommand(
    id: ApiTypes.CommandId,
    index: Long,
    workflowId: ApiTypes.WorkflowId,
    platformTime: Instant,
    contract: ApiTypes.ContractId,
    template: DamlLfIdentifier,
    interfaceId: Option[DamlLfIdentifier],
    choice: ApiTypes.Choice,
    argument: ApiValue,
) extends Command

case class Error(code: String, details: String, parameters: String)
final case class Result(id: ApiTypes.CommandId, errorOrTx: Either[Error, Transaction])
    extends TaggedNode[ApiTypes.CommandIdTag]

// ------------------------------------------------------------------------------------------------
// Daml Package
// ------------------------------------------------------------------------------------------------

case class DamlLfPackage(
    id: DamlLfRef.PackageId,
    typeDefs: Map[DamlLfIdentifier, DamlLfDefDataType],
    templates: Map[DamlLfIdentifier, Template],
    interfaces: Map[DamlLfIdentifier, Interface],
)

/** A boxed DefDataType that also includes the ID of the type.
  * This is useful for the GraphQL schema.
  */
final case class DamlLfDefDataTypeBoxed(id: DamlLfIdentifier, value: DamlLfDefDataType)
    extends DamlLfNode

// ------------------------------------------------------------------------------------------------
// Transactions
// ------------------------------------------------------------------------------------------------

/** Transaction. */
final case class Transaction(
    id: ApiTypes.TransactionId,
    commandId: Option[ApiTypes.CommandId],
    effectiveAt: Instant,
    offset: String,
    events: List[Event],
) extends TaggedNode[ApiTypes.TransactionIdTag]

// ------------------------------------------------------------------------------------------------
// Events
// ------------------------------------------------------------------------------------------------

sealed trait Event extends TaggedNode[ApiTypes.EventIdTag] {
  def workflowId: ApiTypes.WorkflowId

  /** Id of the parent event in the transaction tree. */
  def parentId: Option[ApiTypes.EventId]

  /** Id of the transaction tree containing this event. */
  def transactionId: ApiTypes.TransactionId

  /** Determines which parties are notified of this event. */
  def witnessParties: List[ApiTypes.Party]
}

final case class ContractCreated(
    id: ApiTypes.EventId,
    parentId: Option[ApiTypes.EventId],
    transactionId: ApiTypes.TransactionId,
    witnessParties: List[ApiTypes.Party],
    workflowId: ApiTypes.WorkflowId,
    contractId: ApiTypes.ContractId,
    templateId: DamlLfIdentifier,
    argument: ApiRecord,
    agreementText: Option[String],
    signatories: List[ApiTypes.Party],
    observers: List[ApiTypes.Party],
    key: Option[ApiValue],
) extends Event

final case class ChoiceExercised(
    id: ApiTypes.EventId,
    parentId: Option[ApiTypes.EventId],
    transactionId: ApiTypes.TransactionId,
    witnessParties: List[ApiTypes.Party],
    workflowId: ApiTypes.WorkflowId,
    contractId: ApiTypes.ContractId,
    templateId: DamlLfIdentifier,
    choice: ApiTypes.Choice,
    argument: ApiValue,
    actingParties: List[ApiTypes.Party],
    consuming: Boolean,
) extends Event

// ------------------------------------------------------------------------------------------------
// Contract
// ------------------------------------------------------------------------------------------------

/** Active contract. */
final case class Contract(
    id: ApiTypes.ContractId,
    template: Template,
    argument: ApiRecord,
    agreementText: Option[String],
    signatories: List[ApiTypes.Party],
    observers: List[ApiTypes.Party],
    key: Option[ApiValue],
) extends TaggedNode[ApiTypes.ContractIdTag]

// ------------------------------------------------------------------------------------------------
// Template
// ------------------------------------------------------------------------------------------------

/** Template for instantiating contracts. */
final case class Template(
    id: DamlLfIdentifier,
    choices: List[Choice],
    key: Option[DamlLfType],
    implementedInterfaces: Set[DamlLfIdentifier],
) extends DamlLfNode {
  def topLevelDecl: String = id.qualifiedName.toString()
  def parameter: DamlLfTypeCon = DamlLfTypeCon(DamlLfTypeConName(id), DamlLfImmArraySeq())
}

/** Interfaces. */
final case class Interface(
    id: DamlLfIdentifier,
    choices: List[Choice],
) extends DamlLfNode

/** Template choice. */
case class Choice(
    name: ApiTypes.Choice,
    parameter: DamlLfType,
    returnType: DamlLfType,
    consuming: Boolean,
    inheritedInterface: Option[DamlLfIdentifier] = None,
)
