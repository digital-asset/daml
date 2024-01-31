// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse as GetActiveContractsResponseV1
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionStreamRequest as CompletionStreamRequestV1,
  CompletionStreamResponse as CompletionStreamResponseV1,
}
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest as SubmitAndWaitRequestV1
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest as SubmitRequestV1
import com.daml.ledger.api.v1.commands.Commands as CommandsV1
import com.daml.ledger.api.v1.completion.Completion as CompletionV1
import com.daml.ledger.api.v1.event_query_service.GetEventsByContractIdResponse as GetEventsByContractIdResponseV1
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.{
  Transaction as TransactionV1,
  TransactionTree as TransactionTreeV1,
}
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse as GetFlatTransactionResponseV1,
  GetTransactionByEventIdRequest as GetTransactionByEventIdRequestV1,
  GetTransactionByIdRequest as GetTransactionByIdRequestV1,
  GetTransactionResponse as GetTransactionResponseV1,
  GetTransactionTreesResponse as GetTransactionTreesResponseV1,
  GetTransactionsResponse as GetTransactionsResponseV1,
}
import com.daml.ledger.api.v2.command_completion_service.{
  CompletionStreamRequest as CompletionStreamRequestV2,
  CompletionStreamResponse as CompletionStreamResponseV2,
}
import com.daml.ledger.api.v2.command_service.SubmitAndWaitRequest as SubmitAndWaitRequestV2
import com.daml.ledger.api.v2.command_submission_service.SubmitRequest as SubmitRequestV2
import com.daml.ledger.api.v2.commands.Commands as CommandsV2
import com.daml.ledger.api.v2.completion.Completion as CompletionV2
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse as GetEventsByContractIdResponseV2
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse as GetActiveContractsResponseV2
import com.daml.ledger.api.v2.transaction.{
  Transaction as TransactionV2,
  TransactionTree as TransactionTreeV2,
}
import com.daml.ledger.api.v2.update_service.{
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionResponse as GetTransactionResponseV2,
  GetTransactionTreeResponse as GetTransactionTreeResponseV2,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}

object ApiConversions {

  def toV1(participantOffset: ParticipantOffset): LedgerOffset =
    participantOffset.value match {
      case ParticipantOffset.Value.Empty =>
        LedgerOffset.of(LedgerOffset.Value.Empty)
      case ParticipantOffset.Value.Absolute(absoluteString) =>
        LedgerOffset.of(LedgerOffset.Value.Absolute(absoluteString))
      case ParticipantOffset.Value.Boundary(
            ParticipantOffset.ParticipantBoundary.PARTICIPANT_BEGIN
          ) =>
        LedgerOffset.of(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
      case ParticipantOffset.Value.Boundary(
            ParticipantOffset.ParticipantBoundary.PARTICIPANT_END
          ) =>
        LedgerOffset.of(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))
      case ParticipantOffset.Value.Boundary(
            ParticipantOffset.ParticipantBoundary.Unrecognized(value)
          ) =>
        LedgerOffset.of(
          LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.Unrecognized(value))
        )
    }

  def toV2(ledgerOffset: LedgerOffset): ParticipantOffset = ledgerOffset.value match {
    case LedgerOffset.Value.Empty => ParticipantOffset.of(ParticipantOffset.Value.Empty)
    case LedgerOffset.Value.Absolute(value) =>
      ParticipantOffset.of(ParticipantOffset.Value.Absolute(value))
    case LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN) =>
      ParticipantOffset.of(
        ParticipantOffset.Value.Boundary(ParticipantOffset.ParticipantBoundary.PARTICIPANT_BEGIN)
      )
    case LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END) =>
      ParticipantOffset.of(
        ParticipantOffset.Value.Boundary(ParticipantOffset.ParticipantBoundary.PARTICIPANT_END)
      )
    case LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.Unrecognized(value)) => {
      ParticipantOffset.of(
        ParticipantOffset.Value.Boundary(
          ParticipantOffset.ParticipantBoundary.Unrecognized(value)
        )
      )
    }
  }
  def toV1(
      getTransactionByEventIdRequest: GetTransactionByEventIdRequest
  ): GetTransactionByEventIdRequestV1 =
    GetTransactionByEventIdRequestV1(
      ledgerId = "",
      eventId = getTransactionByEventIdRequest.eventId,
      requestingParties = getTransactionByEventIdRequest.requestingParties,
    )

  def toV1(getTransactionByIdRequest: GetTransactionByIdRequest): GetTransactionByIdRequestV1 =
    GetTransactionByIdRequestV1(
      ledgerId = "",
      transactionId = getTransactionByIdRequest.updateId,
      requestingParties = getTransactionByIdRequest.requestingParties,
    )

  def toV1(completionStreamRequest: CompletionStreamRequestV2): CompletionStreamRequestV1 =
    CompletionStreamRequestV1(
      ledgerId = "",
      applicationId = completionStreamRequest.applicationId,
      parties = completionStreamRequest.parties,
      offset = completionStreamRequest.beginExclusive.map(toV1),
    )

  def toV1(completion: CompletionV2): CompletionV1 =
    CompletionV1(
      commandId = completion.commandId,
      status = completion.status,
      transactionId = completion.updateId,
      applicationId = completion.applicationId,
      actAs = completion.actAs,
      submissionId = completion.submissionId,
      deduplicationPeriod = completion.deduplicationPeriod match {
        case CompletionV2.DeduplicationPeriod.Empty =>
          CompletionV1.DeduplicationPeriod.Empty
        case CompletionV2.DeduplicationPeriod.DeduplicationOffset(offset) =>
          CompletionV1.DeduplicationPeriod.DeduplicationOffset(offset)
        case CompletionV2.DeduplicationPeriod.DeduplicationDuration(duration) =>
          CompletionV1.DeduplicationPeriod.DeduplicationDuration(duration)
      },
      traceContext = completion.traceContext,
    )

  def toV1(completionStreamResponse: CompletionStreamResponseV2): CompletionStreamResponseV1 =
    CompletionStreamResponseV1(
      checkpoint = completionStreamResponse.checkpoint,
      completions = completionStreamResponse.completion.toList.map(toV1),
    )

  def toV1(transaction: TransactionV2): TransactionV1 =
    TransactionV1(
      transactionId = transaction.updateId,
      commandId = transaction.commandId,
      workflowId = transaction.workflowId,
      effectiveAt = transaction.effectiveAt,
      events = transaction.events,
      offset = transaction.offset,
      traceContext = transaction.traceContext,
    )

  def toV1(transactionTree: TransactionTreeV2): TransactionTreeV1 =
    TransactionTreeV1(
      transactionId = transactionTree.updateId,
      commandId = transactionTree.commandId,
      workflowId = transactionTree.workflowId,
      effectiveAt = transactionTree.effectiveAt,
      offset = transactionTree.offset,
      rootEventIds = transactionTree.rootEventIds,
      eventsById = transactionTree.eventsById,
      traceContext = transactionTree.traceContext,
    )

  def toV1(getUpdatesResponse: GetUpdatesResponse): Seq[GetTransactionsResponseV1] =
    getUpdatesResponse.update match {
      case GetUpdatesResponse.Update.Transaction(txV2) =>
        Seq(GetTransactionsResponseV1(Seq(toV1(txV2))))
      case _ => Nil
    }

  def toV1(getUpdateTreesResponse: GetUpdateTreesResponse): Seq[GetTransactionTreesResponseV1] =
    getUpdateTreesResponse.update match {
      case GetUpdateTreesResponse.Update.TransactionTree(txV2) =>
        Seq(GetTransactionTreesResponseV1(Seq(toV1(txV2))))
      case _ => Nil
    }

  def toV1(getTransactionResponse: GetTransactionResponseV2): GetFlatTransactionResponseV1 =
    GetFlatTransactionResponseV1(getTransactionResponse.transaction.map(toV1))

  def toV1(
      getTransactionTreeResponse: GetTransactionTreeResponseV2
  ): GetTransactionResponseV1 =
    GetTransactionResponseV1(getTransactionTreeResponse.transaction.map(toV1))

  def toV1(commands: CommandsV2): CommandsV1 =
    CommandsV1(
      ledgerId = "",
      workflowId = commands.workflowId,
      applicationId = commands.applicationId,
      commandId = commands.commandId,
      party = commands.party,
      commands = commands.commands,
      deduplicationPeriod = commands.deduplicationPeriod match {
        case CommandsV2.DeduplicationPeriod.Empty =>
          CommandsV1.DeduplicationPeriod.Empty
        case CommandsV2.DeduplicationPeriod.DeduplicationDuration(value) =>
          CommandsV1.DeduplicationPeriod.DeduplicationDuration(value)
        case CommandsV2.DeduplicationPeriod.DeduplicationOffset(value) =>
          CommandsV1.DeduplicationPeriod.DeduplicationOffset(value)
      },
      minLedgerTimeAbs = commands.minLedgerTimeAbs,
      minLedgerTimeRel = commands.minLedgerTimeRel,
      actAs = commands.actAs,
      readAs = commands.readAs,
      submissionId = commands.submissionId,
      disclosedContracts = commands.disclosedContracts,
      packageIdSelectionPreference = commands.packageIdSelectionPreference,
    )

  def toV1(getActiveContractsResponse: GetActiveContractsResponseV2): GetActiveContractsResponseV1 =
    GetActiveContractsResponseV1(
      offset = getActiveContractsResponse.offset,
      workflowId = getActiveContractsResponse.workflowId,
      activeContracts = getActiveContractsResponse.contractEntry match {
        case GetActiveContractsResponseV2.ContractEntry.ActiveContract(activeContract) =>
          activeContract.createdEvent.toList
        case GetActiveContractsResponseV2.ContractEntry.Empty =>
          Nil // for the last element with the offset
        case _ =>
          throw new IllegalStateException(
            "This should not happen as for V1 serving there should be no incomplete reassignments populated"
          )
      },
    )

  def toV1(
      getEventsByContractIdResponse: GetEventsByContractIdResponseV2
  ): GetEventsByContractIdResponseV1 =
    GetEventsByContractIdResponseV1(
      createEvent = getEventsByContractIdResponse.created.flatMap(_.createdEvent),
      archiveEvent = getEventsByContractIdResponse.archived.flatMap(_.archivedEvent),
    )

  def toV1(submitRequest: SubmitRequestV2): SubmitRequestV1 =
    SubmitRequestV1(submitRequest.commands.map(toV1))

  def toV1(submitAndWaitRequest: SubmitAndWaitRequestV2): SubmitAndWaitRequestV1 =
    SubmitAndWaitRequestV1(submitAndWaitRequest.commands.map(toV1))
}
