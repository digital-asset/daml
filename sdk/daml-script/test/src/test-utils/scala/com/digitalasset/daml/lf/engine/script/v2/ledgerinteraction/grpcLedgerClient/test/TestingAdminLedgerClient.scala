// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Temporary stand-in for the real admin api clients defined in canton. Needed only for upgrades testing
// We should intend to replace this as soon as possible
package com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction
package grpcLedgerClient
package test

import com.digitalasset.canton.admin.participant.{v30 => admin_package_service}
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import com.digitalasset.canton.ledger.client.GrpcChannel
import com.digitalasset.canton.protocol.v30.Enums.TopologyChangeOp
import com.digitalasset.canton.topology.admin.{v30 => admin_topology_service}
import com.google.protobuf.ByteString
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import java.io.{File, FileInputStream}
import scala.concurrent.{ExecutionContext, Future}

class TestingAdminLedgerClient(
    channel: Channel,
    token: Option[String],
)(implicit ec: ExecutionContext)
    extends AdminLedgerClient(channel, token) {

  private val topologyServiceStub =
    AdminLedgerClient.stub(
      admin_topology_service.TopologyManagerReadServiceGrpc.stub(channel),
      token,
    )

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
              operation = TopologyChangeOp.TOPOLOGY_CHANGE_OP_UNSPECIFIED,
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
        Map.from(resp.results.map { res => (res.item.get.participantUid, res.item.get.packageIds) })
      }
  }
}

object TestingAdminLedgerClient {

  /** A convenient shortcut to build a [[TestingAdminLedgerClient]], use [[fromBuilder]] for a more
    * flexible alternative.
    */
  def singleHost(
      hostIp: String,
      port: Int,
      token: Option[String] = None,
      channelConfig: LedgerClientChannelConfiguration,
  )(implicit
      ec: ExecutionContext
  ): TestingAdminLedgerClient =
    fromBuilder(channelConfig.builderFor(hostIp, port), token)

  def fromBuilder(
      builder: NettyChannelBuilder,
      token: Option[String] = None,
  )(implicit ec: ExecutionContext): TestingAdminLedgerClient =
    new TestingAdminLedgerClient(
      GrpcChannel.withShutdownHook(builder),
      token,
    )
}
