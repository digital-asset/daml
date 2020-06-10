// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.platform.participant.util.ValueConversions._
import JsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContext, Future}

object KeyTransfer {

  private final case class Result(
      oldKeptAssets: Seq[Application.ContractResult],
      newKeptAssets: Seq[Application.ContractResult],
      oldTransferredAssets: Seq[Application.ContractResult],
      newTransferredAssets: Seq[Application.ContractResult],
      oldTransactions: Seq[Application.TransactionResult],
      newTransactions: Seq[Application.TransactionResult],
  )

  implicit private val resultFormat: RootJsonFormat[Result] =
    jsonFormat6(Result.apply)

  private final class Model(packageId: String)(implicit ec: ExecutionContext) {

    val Asset: Application.Template =
      Application.Template(Identifier(packageId, "KeyTransfer", "Asset"))
    val Transfer: Application.Choice = Application.Choice(Asset, "AssetTransfer")
    val Archive: Application.Choice = Application.Choice(Asset, "Archive")

    def createAsset(
        owner: Application.Party,
        receiver: Application.Party,
        name: String,
    ): Future[Application.Contract] =
      owner.create(
        Asset,
        "owner" -> owner.name.asParty,
        "receiver" -> receiver.name.asParty,
        "name" -> name.asText,
      )

    def transfer(
        asset: Application.Contract,
        from: Application.Party,
        to: Application.Party,
    ): Future[Application.Contract] =
      to.exercise(Transfer, asset).map(v => Application.Contract(v.getContractId))

    def archive(asset: Application.Contract, as: Application.Party): Future[Unit] =
      as.exercise(Archive, asset).map(_ => ())
  }

  val ApplicationId: String = "key-transfer"

}

final class KeyTransfer(
    ownerName: String,
    receiverName: String,
    suffix: String,
) extends MigrationStep.Test {

  private val clientConfig =
    LedgerClientConfiguration(
      KeyTransfer.ApplicationId,
      LedgerIdRequirement("", enabled = false),
      CommandClientConfiguration.default,
      None,
    )

  override def execute(packageId: String, config: Config.Test)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] =
    for {
      client <- LedgerClient.singleHost(config.host, config.port, clientConfig)
      owner = new Application.Party(ownerName, client, KeyTransfer.ApplicationId)
      receiver = new Application.Party(receiverName, client, KeyTransfer.ApplicationId)
      model = new KeyTransfer.Model(packageId)
      oldTransactions <- owner.transactions(Seq(model.Asset))
      oldKeptAssets <- owner.activeContracts(model.Asset)
      oldTransferredAssets <- receiver.activeContracts(model.Asset)
      _ <- model.createAsset(owner, receiver, s"keep-$suffix")
      toArchive <- model.createAsset(owner, receiver, s"archive-$suffix")
      _ <- model.archive(asset = toArchive, as = owner)
      toTransfer <- model.createAsset(owner, receiver, s"transfer-$suffix")
      _ <- model.transfer(asset = toTransfer, from = owner, to = receiver)
      newTransactions <- owner.transactions(Seq(model.Asset))
      newKeptAssets <- owner.activeContracts(model.Asset)
      newTransferredAssets <- receiver.activeContracts(model.Asset)
    } yield {
      saveAsJson(
        config.outputFile,
        KeyTransfer
          .Result(
            oldKeptAssets.map(Application.ContractResult.fromCreateEvent),
            newKeptAssets.map(Application.ContractResult.fromCreateEvent),
            oldTransferredAssets.map(Application.ContractResult.fromCreateEvent),
            newTransferredAssets.map(Application.ContractResult.fromCreateEvent),
            oldTransactions.map(Application.TransactionResult.fromTransaction),
            newTransactions.map(Application.TransactionResult.fromTransaction),
          ),
      )
    }

}
