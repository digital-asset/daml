// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerIdRequirement,
}
import com.daml.platform.participant.util.ValueConversions._
import spray.json.RootJsonFormat
import JsonProtocol._

import scala.concurrent.{ExecutionContext, Future}

object ProposeAccept {

  private final case class Result(
      oldProposeDeals: Seq[Application.ContractResult],
      newProposeDeals: Seq[Application.ContractResult],
      oldDeals: Seq[Application.ContractResult],
      newDeals: Seq[Application.ContractResult],
      oldTransactions: Seq[Application.TransactionResult],
      newTransactions: Seq[Application.TransactionResult],
  )

  implicit private val resultFormat: RootJsonFormat[ProposeAccept.Result] =
    jsonFormat6(ProposeAccept.Result.apply)

  private final class Model(packageId: String)(implicit ec: ExecutionContext) {

    val ProposeDeal: Application.Template =
      Application.Template(Identifier(packageId, "ProposeAccept", "ProposeDeal"))
    val Accept: Application.Choice = Application.Choice(ProposeDeal, "Accept")

    val Deal: Application.Template =
      Application.Template(Identifier(packageId, "ProposeAccept", "Deal"))
    val UnilateralArchive: Application.Choice = Application.Choice(Deal, "UnilateralArchive")

    def createProposal(
        proposer: Application.Party,
        accepter: Application.Party,
        note: String,
    ): Future[Application.Contract] =
      proposer.create(
        ProposeDeal,
        "proposer" -> proposer.name.asParty,
        "accepter" -> accepter.name.asParty,
        "note" -> note.asText,
      )

    def acceptProposal(
        accepter: Application.Party,
        proposal: Application.Contract,
    ): Future[Application.Contract] =
      accepter
        .exercise(Accept, proposal)
        .map(value => Application.Contract(value.getContractId))

    def unilateralArchive(
        proposer: Application.Party,
        deal: Application.Contract,
    ): Future[Unit] =
      proposer.exercise(UnilateralArchive, deal).map(_ => ())
  }

  val ApplicationId: String = "propose-accept"

}

final class ProposeAccept(
    proposerName: String,
    accepterName: String,
    note: String,
) extends MigrationStep.Test {

  private val clientConfig =
    LedgerClientConfiguration(
      ProposeAccept.ApplicationId,
      LedgerIdRequirement.none,
      CommandClientConfiguration.default,
      None,
    )

  override def execute(packageId: String, config: Config.Test)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] =
    for {
      client <- LedgerClient.singleHost(
        config.host,
        config.port,
        clientConfig,
        LedgerClientChannelConfiguration.InsecureDefaults,
      )
      proposer = new Application.Party(proposerName, client, ProposeAccept.ApplicationId)
      accepter = new Application.Party(accepterName, client, ProposeAccept.ApplicationId)
      model = new ProposeAccept.Model(packageId)
      oldTransactions <- proposer.transactions(Seq(model.ProposeDeal, model.Deal))
      oldProposals <- proposer.activeContracts(model.ProposeDeal)
      oldAccepted <- proposer.activeContracts(model.Deal)
      proposal0 <- model.createProposal(proposer, accepter, note)
      proposal1 <- model.createProposal(proposer, accepter, note)
      deal1 <- model.acceptProposal(accepter, proposal0)
      _ <- model.acceptProposal(accepter, proposal1)
      _ <- model.unilateralArchive(proposer, deal1)
      newProposals <- proposer.activeContracts(model.ProposeDeal)
      newAccepted <- proposer.activeContracts(model.Deal)
      newTransactions <- proposer.transactions(Seq(model.ProposeDeal, model.Deal))
    } yield saveAsJson(
      config.outputFile,
      ProposeAccept
        .Result(
          oldProposals.map(Application.ContractResult.fromCreateEvent),
          newProposals.map(Application.ContractResult.fromCreateEvent),
          oldAccepted.map(Application.ContractResult.fromCreateEvent),
          newAccepted.map(Application.ContractResult.fromCreateEvent),
          oldTransactions.map(Application.TransactionResult.fromTransaction),
          newTransactions.map(Application.TransactionResult.fromTransaction),
        ),
    )

}
