// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import akka.stream.Materializer
import com.daml.platform.participant.util.ValueConversions._
import com.daml.JsonProtocol._
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerIdRequirement,
}
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContext, Future}

object Divulgence {

  private final case class Result(
      oldAssets: Seq[Application.ContractResult],
      newAssets: Seq[Application.ContractResult],
      oldDivulgedAssets: Seq[Application.ContractResult],
      newDivulgedAssets: Seq[Application.ContractResult],
  )

  implicit private val resultFormat: RootJsonFormat[Result] =
    jsonFormat4(Result.apply)

  private final class Model(packageId: String)(implicit ec: ExecutionContext) {

    val Asset: Application.Template =
      Application.Template(Identifier(packageId, "Divulgence", "Asset"))
    val Fetch: Application.Choice = Application.Choice(Asset, "Fetch")

    val AssetDivulgence: Application.Template =
      Application.Template(Identifier(packageId, "Divulgence", "AssetDivulgence"))
    val Divulge: Application.Choice = Application.Choice(AssetDivulgence, "Divulge")

    def createAsset(
        owner: Application.Party,
        tag: String,
    ): Future[Application.Contract] =
      owner.create(
        Asset,
        "owner" -> owner.name.asParty,
        "tag" -> tag.asText,
      )

    def createAssetDivulgence(
        assetOwner: Application.Party,
        divulgee: Application.Party,
    ): Future[Application.Contract] =
      divulgee.create(
        AssetDivulgence,
        "divulgee" -> divulgee.name.asParty,
        "assetOwner" -> assetOwner.name.asParty,
      )

    def divulge(
        asset: Application.Contract,
        assetDivulgence: Application.Contract,
        assetOwner: Application.Party,
    ): Future[Unit] =
      assetOwner
        .exercise(Divulge, assetDivulgence, "divulgedAsset" -> asset.identifier.asContractId)
        .map(_ => ())

    def fetchDivulgedAsset(
        asset: Application.Contract,
        divulgee: Application.Party,
    ): Future[Application.ContractResult] =
      divulgee
        .exercise(Fetch, asset, "divulgee" -> divulgee.name.asParty)
        .map(argument => Application.ContractResult(asset.identifier, argument.getRecord))

  }

  val ApplicationId: String = "divulgence"

}

final class Divulgence(
    ownerName: String,
    divulgeeName: String,
    suffix: String,
) extends MigrationStep.Test {

  private val clientConfig =
    LedgerClientConfiguration(
      Divulgence.ApplicationId,
      LedgerIdRequirement.none,
      CommandClientConfiguration.default,
      None,
    )

  private val divulged: PartialFunction[CreatedEvent, Application.Contract] = {
    case created: CreatedEvent
        if created.createArguments.exists(
          _.fields
            .exists(r => r.label == "tag" && r.value.exists(_.getText.startsWith("divulging-")))
        ) =>
      Application.Contract(created.contractId)
  }

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
      divulgee = new Application.Party(divulgeeName, client, KeyTransfer.ApplicationId)
      model = new Divulgence.Model(packageId)
      divulgence <- model.createAssetDivulgence(owner, divulgee)
      oldAssets <- owner.activeContracts(model.Asset)
      oldDivulgeeAssets <- Future.traverse(oldAssets.collect(divulged))(
        model.fetchDivulgedAsset(_, divulgee)
      )
      asset <- model.createAsset(owner, s"divulging-$suffix")
      _ <- model.divulge(asset, divulgence, owner)
      _ <- model.createAsset(owner, s"private-$suffix")
      newAssets <- owner.activeContracts(model.Asset)
      newDivulgeeAssets <- Future.traverse(newAssets.collect(divulged))(
        model.fetchDivulgedAsset(_, divulgee)
      )
    } yield {
      saveAsJson(
        config.outputFile,
        Divulgence
          .Result(
            oldAssets.map(Application.ContractResult.fromCreateEvent),
            newAssets.map(Application.ContractResult.fromCreateEvent),
            oldDivulgeeAssets,
            newDivulgeeAssets,
          ),
      )
    }

}
