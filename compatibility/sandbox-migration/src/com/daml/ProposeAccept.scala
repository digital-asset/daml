// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.event.{CreatedEvent, Event}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.value.{Identifier, Record}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.platform.participant.util.ValueConversions._

import scala.concurrent.{ExecutionContext, Future}

object ProposeAccept {

  private def toEventResult(e: Event): ProposeAccept.EventResult = {
    e.event match {
      case Event.Event.Created(created) =>
        ProposeAccept.CreatedResult(
          entityName = created.getTemplateId.entityName,
          moduleName = created.getTemplateId.moduleName,
          contractId = created.contractId,
          argument = created.getCreateArguments,
        )
      case Event.Event.Archived(archived) =>
        ProposeAccept.ArchivedResult(
          entityName = archived.getTemplateId.entityName,
          moduleName = archived.getTemplateId.moduleName,
          archived.contractId,
        )
      case Event.Event.Empty =>
        throw new RuntimeException("Invalid empty event")
    }
  }

  object ContractResult {
    def fromCreateEvent(event: CreatedEvent): ContractResult =
      ContractResult(event.contractId, event.createArguments.get)
  }
  final case class ContractResult(_1: String, _2: Record)

  object TransactionResult {
    def fromTransaction(transaction: Transaction): TransactionResult =
      TransactionResult(transaction.transactionId, transaction.events.map(toEventResult))
  }
  final case class TransactionResult(
      transactionId: String,
      events: Seq[EventResult],
  )

  final case class Result(
      oldProposeDeals: Seq[ContractResult],
      newProposeDeals: Seq[ContractResult],
      oldDeals: Seq[ContractResult],
      newDeals: Seq[ContractResult],
      oldTransactions: Seq[TransactionResult],
      newTransactions: Seq[TransactionResult],
  )

  sealed abstract class EventResult
  final case class CreatedResult(
      entityName: String,
      moduleName: String,
      contractId: String,
      argument: Record,
  ) extends EventResult
  final case class ArchivedResult(
      entityName: String,
      moduleName: String,
      contractId: String,
  ) extends EventResult

}

final class ProposeAccept(host: String, port: Int, applicationId: String, packageId: String)(
    implicit ec: ExecutionContext,
    esf: ExecutionSequencerFactory,
    mat: Materializer) {

  private val ProposeDeal =
    Application.Template(Identifier(packageId, "ProposeAccept", "ProposeDeal"))
  private val Accept = Application.Choice(ProposeDeal, "Accept")

  private val Deal =
    Application.Template(Identifier(packageId, "ProposeAccept", "Deal"))
  private val UnilateralArchive = Application.Choice(Deal, "UnilateralArchive")

  private val clientConfig =
    LedgerClientConfiguration(
      applicationId,
      LedgerIdRequirement("", enabled = false),
      CommandClientConfiguration.default,
      None,
    )

  private def createProposal(
      proposer: Application.Party,
      accepter: Application.Party,
      note: String,
  ): Future[Application.Contract] =
    proposer.create(
      ProposeDeal,
      "proposer" -> proposer.name.asParty,
      "accepter" -> accepter.name.asParty,
      "note" -> note.asText)

  private def acceptProposal(
      accepter: Application.Party,
      proposal: Application.Contract,
  ): Future[Application.Contract] =
    accepter
      .exercise(Accept, proposal)
      .map(value => Application.Contract(value.getContractId))

  private def unilateralArchive(
      proposer: Application.Party,
      deal: Application.Contract,
  ): Future[Unit] =
    proposer.exercise(UnilateralArchive, deal).map(_ => ())

  def run(proposerName: String, accepterName: String, note: String,
  ): Future[ProposeAccept.Result] = {
    for {
      client <- LedgerClient.singleHost(host, port, clientConfig)
      proposer = new Application.Party(proposerName, client, applicationId)
      accepter = new Application.Party(accepterName, client, applicationId)
      oldTransactions <- proposer.transactions(Seq(ProposeDeal, Deal))
      oldProposals <- proposer.activeContracts(ProposeDeal)
      oldAccepted <- proposer.activeContracts(Deal)
      proposal0 <- createProposal(proposer, accepter, note)
      proposal1 <- createProposal(proposer, accepter, note)
      deal1 <- acceptProposal(accepter, proposal0)
      _ <- acceptProposal(accepter, proposal1)
      _ <- unilateralArchive(proposer, deal1)
      newProposals <- proposer.activeContracts(ProposeDeal)
      newAccepted <- proposer.activeContracts(Deal)
      newTransactions <- proposer.transactions(Seq(ProposeDeal, Deal))
    } yield
      ProposeAccept.Result(
        oldProposals.map(ProposeAccept.ContractResult.fromCreateEvent),
        newProposals.map(ProposeAccept.ContractResult.fromCreateEvent),
        oldAccepted.map(ProposeAccept.ContractResult.fromCreateEvent),
        newAccepted.map(ProposeAccept.ContractResult.fromCreateEvent),
        oldTransactions.map(ProposeAccept.TransactionResult.fromTransaction),
        newTransactions.map(ProposeAccept.TransactionResult.fromTransaction),
      )
  }

}
