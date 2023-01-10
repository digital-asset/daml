// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import JsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContext, Future}

object KeyTransfer {

  private final case class Result(
      oldAssets: Seq[Application.ContractResult],
      newAssets: Seq[Application.ContractResult],
      oldTransactions: Seq[Application.TransactionResult],
      newTransactions: Seq[Application.TransactionResult],
  )

  implicit private val resultFormat: RootJsonFormat[Result] =
    jsonFormat4(Result.apply)

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
      owner = new Application.Party(ownerName, client, KeyTransfer.ApplicationId)
      receiver = new Application.Party(receiverName, client, KeyTransfer.ApplicationId)
      model = new KeyTransfer.Model(packageId)
      oldTransactions <- owner.transactions(Seq(model.Asset))
      oldAssets <- owner.activeContracts(model.Asset)
      _ <- model.createAsset(owner, receiver, s"keep-$suffix")
      toArchive <- model.createAsset(owner, receiver, s"archive-$suffix")
      _ <- model.archive(asset = toArchive, as = owner)
      toTransfer <- model.createAsset(owner, receiver, s"transfer-$suffix")
      _ <- model.transfer(asset = toTransfer, to = receiver)
      newTransactions <- owner.transactions(Seq(model.Asset))
      newAssets <- owner.activeContracts(model.Asset)
    } yield {
      saveAsJson(
        config.outputFile,
        KeyTransfer
          .Result(
            oldAssets.map(Application.ContractResult.fromCreateEvent),
            newAssets.map(Application.ContractResult.fromCreateEvent),
            oldTransactions.map(Application.TransactionResult.fromTransaction),
            newTransactions.map(Application.TransactionResult.fromTransaction),
          ),
      )
    }

}
