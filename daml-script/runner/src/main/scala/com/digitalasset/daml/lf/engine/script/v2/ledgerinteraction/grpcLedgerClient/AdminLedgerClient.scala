// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Temporary stand-in for the real admin api clients defined in canton. Needed only for upgrades testing
// We should intend to replace this as soon as possible
package com.daml.lf.engine.script.v2.ledgerinteraction
package grpcLedgerClient

import com.digitalasset.canton.ledger.client.LedgerCallCredentials.authenticatingStub
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import com.digitalasset.canton.ledger.client.GrpcChannel
import com.digitalasset.canton.admin.participant.{v30 => admin_package_service}
import com.digitalasset.canton.topology.admin.{v30 => admin_topology_service}
import com.digitalasset.canton.protocol.v30.Enums.TopologyChangeOp
import com.google.protobuf.ByteString
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.AbstractStub
import java.io.{Closeable, File, FileInputStream}
import scala.concurrent.{ExecutionContext, Future}

final class AdminLedgerClient private (
    val channel: Channel,
    token: Option[String],
)(implicit ec: ExecutionContext)
    extends Closeable {

  // Follow community/app-base/src/main/scala/com/digitalasset/canton/console/commands/TopologyAdministration.scala:1149
  // Shows how to do a list request
  // Try filtering for just Adds, assuming a Remove cancels an Add.
  // If it doesn't, change the filter to all and fold them

  private val packageServiceStub =
    AdminLedgerClient.stub(admin_package_service.PackageServiceGrpc.stub(channel), token)

  private val topologyServiceStub =
    AdminLedgerClient.stub(
      admin_topology_service.TopologyManagerReadServiceGrpc.stub(channel),
      token,
    )

  def vetDarByHash(darHash: String): Future[Unit] =
    packageServiceStub.vetDar(admin_package_service.VetDarRequest(darHash, true)).map(_ => ())

  def unvetDarByHash(darHash: String): Future[Unit] =
    packageServiceStub.unvetDar(admin_package_service.UnvetDarRequest(darHash)).map(_ => ())

  // Gets all (first 1000) dar names and hashes
  def listDars(): Future[Seq[(String, String)]] =
    packageServiceStub
      .listDars(admin_package_service.ListDarsRequest(1000))
      .map { res =>
        if (res.dars.length == 1000)
          println(
            "Warning: AdminLedgerClient.listDars gave the maximum number of results, some may have been truncated."
          )
        res.dars.map(darDesc => (darDesc.name, darDesc.hash))
      }

  def findDarHash(name: String): Future[String] =
    listDars().map(_.collectFirst { case (`name`, v) => v }
      .getOrElse(throw new IllegalArgumentException("Couldn't find DAR name: " + name)))

  def vetDar(name: String): Future[Unit] =
    findDarHash(name).flatMap(vetDarByHash)

  def unvetDar(name: String): Future[Unit] =
    findDarHash(name).flatMap(unvetDarByHash)

  def uploadDar(file: File): Future[Either[String, String]] =
    packageServiceStub
      .uploadDar(
        admin_package_service.UploadDarRequest(
          data = ByteString.readFrom(new FileInputStream(file)),
          filename = file.getName,
          vetAllPackages = true,
          synchronizeVetting = true,
        )
      )
      .map { response =>
        import admin_package_service.UploadDarResponse
        response.value match {
          case UploadDarResponse.Value.Success(UploadDarResponse.Success(hash)) => Right(hash)
          case UploadDarResponse.Value.Failure(UploadDarResponse.Failure(msg)) => Left(msg)
          case UploadDarResponse.Value.Empty => Left("unexpected empty response")
        }
      }

  // Map from participantName (in the form PAR::name::hash) to list of packages
  def listVettedPackages(): Future[Map[String, Seq[String]]] = {
    topologyServiceStub
      .listVettedPackages(
        admin_topology_service.ListVettedPackagesRequest(
          baseQuery = Some(
            admin_topology_service.BaseQuery(
              filterStore = None,
              proposals = false,
              operation = TopologyChangeOp.TOPOLOGY_CHANGE_OP_REPLACE_UNSPECIFIED,
              filterOperation = false,
              timeQuery = admin_topology_service.BaseQuery.TimeQuery
                .HeadState(com.google.protobuf.empty.Empty()),
              filterSignedKey = "",
              protocolVersion = None,
            )
          ),
          filterParticipant = "",
        )
      )
      .map { resp =>
        Map.from(resp.results.map { res => (res.item.get.participant, res.item.get.packageIds) })
      }
  }

  override def close(): Unit = GrpcChannel.close(channel)
}

object AdminLedgerClient {
  private[grpcLedgerClient] def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(authenticatingStub(stub, _))

  /** A convenient shortcut to build a [[AdminLedgerClient]], use [[fromBuilder]] for a more
    * flexible alternative.
    */
  def singleHost(
      hostIp: String,
      port: Int,
      token: Option[String] = None,
      channelConfig: LedgerClientChannelConfiguration,
  )(implicit
      ec: ExecutionContext
  ): AdminLedgerClient =
    fromBuilder(channelConfig.builderFor(hostIp, port), token)

  def fromBuilder(
      builder: NettyChannelBuilder,
      token: Option[String] = None,
  )(implicit ec: ExecutionContext): AdminLedgerClient =
    new AdminLedgerClient(
      GrpcChannel.withShutdownHook(builder),
      token,
    )
}
