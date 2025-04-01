// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Temporary stand-in for the real admin api clients defined in canton. Needed only for upgrades testing
// We should intend to replace this as soon as possible
package com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction
package grpcLedgerClient

import com.daml.grpc.AuthCallCredentials
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import com.digitalasset.canton.ledger.client.GrpcChannel
import com.digitalasset.canton.admin.participant.{v30 => admin_participant}
import com.digitalasset.canton.topology.admin.v30.ForceFlag
import com.digitalasset.canton.topology.admin.{v30 => admin_topology}
import com.digitalasset.canton.protocol.{v30 => protocol}
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.AbstractStub

import java.io.Closeable
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.{ExecutionContext, Future}

class AdminLedgerClient private[grpcLedgerClient] (
    val channel: Channel,
    token: Option[String],
    participantId: String,
)(implicit ec: ExecutionContext)
    extends Closeable {

  Files.write(
    Paths.get("/tmp/participant_id"),
    s"PARTICIPANT ID: ${participantId}".getBytes(StandardCharsets.UTF_8),
    StandardOpenOption.APPEND,
    StandardOpenOption.CREATE,
  )
  print(s"PARTICIPANT ID: ${participantId}")

  // Follow community/app-base/src/main/scala/com/digitalasset/canton/console/commands/TopologyAdministration.scala:1149
  // Shows how to do a list request
  // Try filtering for just Adds, assuming a Remove cancels an Add.
  // If it doesn't, change the filter to all and fold them

  private[grpcLedgerClient] val packageServiceStub =
    AdminLedgerClient.stub(admin_participant.PackageServiceGrpc.stub(channel), token)

  private[grpcLedgerClient] val topologyReadServiceStub =
    AdminLedgerClient.stub(
      admin_topology.TopologyManagerReadServiceGrpc.stub(channel),
      token,
    )

  private[grpcLedgerClient] val topologyWriteServiceStub =
    AdminLedgerClient.stub(
      admin_topology.TopologyManagerWriteServiceGrpc.stub(channel),
      token,
    )

  def vetDarByHash(darHash: String): Future[Unit] =
    packageServiceStub.vetDar(admin_participant.VetDarRequest(darHash, true)).map(_ => ())

  def unvetDarByHash(darHash: String): Future[Unit] = for {
    vettedPackages <- listVettedPackages()
    newPackages = vettedPackages(participantId).filter(_ != darHash)
    _ = print(makeAuthorizeRequest(participantId, newPackages))
    _ <- topologyWriteServiceStub.authorize(makeAuthorizeRequest(participantId, newPackages))
  } yield ()

  private[this] def makeAuthorizeRequest(
      participantId: String,
      vettedPackageIds: Seq[String],
  ): admin_topology.AuthorizeRequest =
    admin_topology.AuthorizeRequest(
      admin_topology.AuthorizeRequest.Type.Proposal(
        admin_topology.AuthorizeRequest.Proposal(
          protocol.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE,
          0, // will be picked by the participant
          Some(
            protocol.TopologyMapping(
              protocol.TopologyMapping.Mapping.VettedPackages(
                protocol.VettedPackages(
                  participantId,
                  Seq.empty,
                  vettedPackageIds.map(pkgId =>
                    protocol.VettedPackages.VettedPackage(
                      pkgId,
                      None,
                      None,
                    )
                  ),
                )
              )
            )
          ),
        )
      ),
      mustFullyAuthorize = true,
      forceChanges = Seq(ForceFlag.FORCE_FLAG_ALLOW_UNVET_PACKAGE),
      signedBy = Seq.empty,
      store = Some(
        admin_topology.StoreId(
          admin_topology.StoreId.Store.Authorized(admin_topology.StoreId.Authorized())
        )
      ),
      waitToBecomeEffective = None,
    )

  // Gets all (first 1000) dar names and hashes
  def listDars(): Future[Seq[(String, String)]] =
    packageServiceStub
      .listDars(
        admin_participant.ListDarsRequest(1000, "")
      ) // Empty filterName is the default value
      .map { res =>
        if (res.dars.length == 1000)
          println(
            "Warning: AdminLedgerClient.listDars gave the maximum number of results, some may have been truncated."
          )
        res.dars.map(darDesc => (darDesc.name + "-" + darDesc.version, darDesc.main))
      }

  def findDarHash(name: String): Future[String] =
    listDars().map(_.collectFirst { case (`name`, v) => v }
      .getOrElse(throw new IllegalArgumentException("Couldn't find DAR name: " + name)))

  def vetDar(name: String): Future[Unit] =
    findDarHash(name).flatMap(vetDarByHash)

  def unvetDar(name: String): Future[Unit] =
    findDarHash(name).flatMap(unvetDarByHash)

  def listVettedPackages(): Future[Map[String, Seq[String]]] = {
    val res = topologyReadServiceStub
      .listVettedPackages(
        admin_topology.ListVettedPackagesRequest(
          baseQuery = Some(
            admin_topology.BaseQuery(
              store = Some(
                admin_topology.StoreId(
                  admin_topology.StoreId.Store.Authorized(admin_topology.StoreId.Authorized())
                )
              ),
              proposals = false,
              operation = protocol.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_UNSPECIFIED,
              timeQuery = admin_topology.BaseQuery.TimeQuery
                .HeadState(com.google.protobuf.empty.Empty()),
              filterSignedKey = "",
              protocolVersion = None,
            )
          ),
          filterParticipant = "",
        )
      )
      .map(x => { println(x); x })
      .map(
        _.results
          .groupMapReduce(_.item.get.participantUid)(_.item.get.packages.map(_.packageId))(_ ++ _)
      )
    res
  }

  override def close(): Unit = GrpcChannel.close(channel)
}

object AdminLedgerClient {
  private[grpcLedgerClient] def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(AuthCallCredentials.authorizingStub(stub, _))

  /** A convenient shortcut to build a [[AdminLedgerClient]], use [[fromBuilder]] for a more
    * flexible alternative.
    */
  def singleHost(
      hostIp: String,
      port: Int,
      token: Option[String] = None,
      channelConfig: LedgerClientChannelConfiguration,
      participantId: String,
  )(implicit
      ec: ExecutionContext
  ): AdminLedgerClient =
    fromBuilder(channelConfig.builderFor(hostIp, port), token, participantId)

  def fromBuilder(
      builder: NettyChannelBuilder,
      token: Option[String] = None,
      participantId: String,
  )(implicit ec: ExecutionContext): AdminLedgerClient =
    new AdminLedgerClient(
      GrpcChannel.withShutdownHook(builder),
      token,
      participantId,
    )
}
