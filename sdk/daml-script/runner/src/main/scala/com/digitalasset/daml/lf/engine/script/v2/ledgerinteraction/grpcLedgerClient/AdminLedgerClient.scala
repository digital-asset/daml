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
import com.digitalasset.daml.lf.data.Ref.{PackageName, PackageVersion}
import com.google.protobuf.ByteString
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.AbstractStub

import java.io.{Closeable, File, FileInputStream}
import scala.concurrent.{ExecutionContext, Future}

class AdminLedgerClient private[grpcLedgerClient] (
    val channel: Channel,
    token: Option[String],
    val participantUid: String,
)(implicit ec: ExecutionContext)
    extends Closeable {

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

  private[grpcLedgerClient] val synchronizerConnectivityStub =
    AdminLedgerClient.stub(
      admin_participant.SynchronizerConnectivityServiceGrpc.stub(channel),
      token,
    )

  def listVettedPackages(): Future[Map[String, Seq[protocol.VettedPackages.VettedPackage]]] =
    topologyReadServiceStub
      .listVettedPackages(makeListVettedPackagesRequest())
      .map(_.results.view.map(res => (res.item.get.participantUid -> res.item.get.packages)).toMap)

  private[this] def makeListVettedPackagesRequest() =
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

  def vetPackagesById(packageIds: Iterable[String]): Future[Unit] = {
    for {
      vettedPackages <- listVettedPackages()
      newVettedPackages = packageIds.map(pkgId =>
        protocol.VettedPackages.VettedPackage(pkgId, None, None)
      ) ++ vettedPackages(participantUid)
      _ <- topologyWriteServiceStub.authorize(
        makeAuthorizeRequest(participantUid, newVettedPackages)
      )
    } yield ()
  }

  def unvetPackagesById(packageIds: Iterable[String]): Future[Unit] = {
    val packageIdsSet = packageIds.toSet
    for {
      vettedPackages <- listVettedPackages()
      newVettedPackages = vettedPackages(participantUid).filterNot(pkg =>
        packageIdsSet.contains(pkg.packageId)
      )
      _ <- topologyWriteServiceStub.authorize(
        makeAuthorizeRequest(participantUid, newVettedPackages)
      )
    } yield ()
  }

  private[this] def makeAuthorizeRequest(
      participantId: String,
      vettedPackages: Iterable[protocol.VettedPackages.VettedPackage],
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
                  vettedPackages.toSeq,
                )
              )
            )
          ),
        )
      ),
      mustFullyAuthorize = true,
      forceChanges = Seq(
        ForceFlag.FORCE_FLAG_ALLOW_UNVET_PACKAGE,
        ForceFlag.FORCE_FLAG_ALLOW_UNVET_PACKAGE_WITH_ACTIVE_CONTRACTS,
      ),
      signedBy = Seq.empty,
      store = Some(
        admin_topology.StoreId(
          admin_topology.StoreId.Store.Authorized(admin_topology.StoreId.Authorized())
        )
      ),
      waitToBecomeEffective = None,
    )

  def unvetPackages(packages: Iterable[ScriptLedgerClient.ReadablePackageId]): Future[Unit] = for {
    packageMap <- getPackageMap()
    _ <- unvetPackagesById(
      packages
        .map(pkg =>
          packageMap.getOrElse(
            pkg,
            throw new IllegalArgumentException(s"Package $pkg not found on participant"),
          )
        )
    )
  } yield ()

  def vetPackages(packages: Iterable[ScriptLedgerClient.ReadablePackageId]): Future[Unit] = for {
    packageMap <- getPackageMap()
    _ <- vetPackagesById(
      packages
        .map(pkg =>
          packageMap.getOrElse(
            pkg,
            throw new IllegalArgumentException(s"Package $pkg not found on participant"),
          )
        )
    )
  } yield ()

  private[this] def getPackageMap(): Future[Map[ScriptLedgerClient.ReadablePackageId, String]] =
    for {
      mainPkgIds <- listMainPackageIds()
      darContentsResps <- Future.traverse(mainPkgIds)(pkgId =>
        packageServiceStub.getDarContents(admin_participant.GetDarContentsRequest(pkgId))
      )
    } yield {
      darContentsResps.view
        .flatMap(_.packages)
        .map(pkgDesc => {
          def invalidPackageDesc =
            throw new IllegalStateException(s"Invalid package description: $pkgDesc")
          val pname = PackageName.fromString(pkgDesc.name).getOrElse(invalidPackageDesc)
          val pversion = PackageVersion.fromString(pkgDesc.version).getOrElse(invalidPackageDesc)
          (ScriptLedgerClient.ReadablePackageId(pname, pversion), pkgDesc.packageId)
        })
        .toMap
    }

  /** Lists the main package IDs of up to 1000 dars hosted on the participant.
    */
  private[this] def listMainPackageIds(): Future[Seq[String]] =
    packageServiceStub
      .listDars(
        admin_participant.ListDarsRequest(1000, "")
      ) // Empty filterName is the default value
      .map { res =>
        if (res.dars.length == 1000)
          println(
            "Warning: AdminLedgerClient.listDars gave the maximum number of results, some may have been truncated."
          )
        res.dars.map(_.main)
      }

  def uploadDar(file: File): Future[Either[String, String]] =
    packageServiceStub
      .uploadDar(
        admin_participant.UploadDarRequest(
          dars = Seq(
            admin_participant.UploadDarRequest.UploadDarData(
              ByteString.readFrom(new FileInputStream(file)),
              description = Some(file.getName),
              expectedMainPackageId = None, // empty string is the default expected_main_package_id
            )
          ),
          vetAllPackages = true,
          synchronizeVetting = true,
        )
      )
      .map { response =>
        import admin_participant.UploadDarResponse
        response match {
          case UploadDarResponse(hash) => Right(hash.head)
        }
      }

  def proposePartyReplication(partyId: String, toParticipantUid: String): Future[Unit] = {
    for {
      synchronizerId <- getSynchronizerId
      hostingParticipants <- listHostingParticipants(partyId, synchronizerId)
      _ <- topologyWriteServiceStub.authorize(
        makePartyReplicationAuthorizeRequest(
          hostingParticipants,
          partyId,
          toParticipantUid,
          synchronizerId,
        )
      )
    } yield ()
  }

  private[this] def getSynchronizerId: Future[String] =
    synchronizerConnectivityStub
      .listConnectedSynchronizers(admin_participant.ListConnectedSynchronizersRequest())
      .map(_.connectedSynchronizers.head.synchronizerId)

  private[this] def listHostingParticipants(
      partyId: String,
      synchronizerId: String,
  ): Future[Seq[protocol.PartyToParticipant.HostingParticipant]] =
    topologyReadServiceStub
      .listPartyToParticipant(makeListPartyToParticipantRequest(synchronizerId))
      .map(
        _.results.view
          .collectFirst {
            case result if result.item.get.party == partyId =>
              result.item.get.participants
          }
          .getOrElse(Seq.empty)
      )

  private[this] def makeListPartyToParticipantRequest(
      synchronizerId: String
  ): admin_topology.ListPartyToParticipantRequest =
    admin_topology.ListPartyToParticipantRequest(
      baseQuery = Some(
        admin_topology.BaseQuery(
          store = Some(
            admin_topology.StoreId(
              admin_topology.StoreId.Store.Synchronizer(
                admin_topology.StoreId.Synchronizer(synchronizerId)
              )
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
      filterParty = "",
      filterParticipant = "",
    )

  private[this] def makePartyReplicationAuthorizeRequest(
      currentHostingParticipants: Seq[protocol.PartyToParticipant.HostingParticipant],
      partyId: String,
      participantId: String,
      synchronizerId: String,
  ): admin_topology.AuthorizeRequest = {
    val newEntry = protocol.PartyToParticipant.HostingParticipant(
      participantId,
      protocol.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
    )
    admin_topology.AuthorizeRequest(
      admin_topology.AuthorizeRequest.Type.Proposal(
        admin_topology.AuthorizeRequest.Proposal(
          protocol.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE,
          0, // will be picked by the participant
          Some(
            protocol.TopologyMapping(
              protocol.TopologyMapping.Mapping.PartyToParticipant(
                protocol.PartyToParticipant(
                  partyId,
                  1,
                  newEntry +: currentHostingParticipants,
                )
              )
            )
          ),
        )
      ),
      mustFullyAuthorize = false,
      forceChanges = Seq.empty,
      signedBy = Seq.empty,
      store = Some(
        admin_topology.StoreId(
          admin_topology.StoreId.Store.Synchronizer(
            admin_topology.StoreId.Synchronizer(synchronizerId)
          )
        )
      ),
      waitToBecomeEffective = Some(com.google.protobuf.duration.Duration(1, 0)),
    )
  }

  override def close(): Unit = GrpcChannel.close(channel)
}

object AdminLedgerClient {
  private[grpcLedgerClient] def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(AuthCallCredentials.authorizingStub(stub, _))

  /** Retrieves the identifier of the participant hosted at hostIp:port and calls [[singleHost]]
    * with the result.
    */
  def singleHostWithUnknownParticipantId(
      hostIp: String,
      port: Int,
      token: Option[String] = None,
      channelConfig: LedgerClientChannelConfiguration,
  )(implicit ec: ExecutionContext): Future[AdminLedgerClient] = {
    for {
      participantId <- {
        val identityServiceClient = IdentityServiceClient
          .singleHost(hostIp, port, token, channelConfig)
        val future = identityServiceClient.getId()
        val _ = future.onComplete(_ => identityServiceClient.close())
        future
      }
    } yield AdminLedgerClient
      .singleHost(
        hostIp,
        port,
        token,
        channelConfig,
        participantId.getOrElse(
          throw new IllegalStateException("unexpected uninitialized participant")
        ),
      )
  }

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
