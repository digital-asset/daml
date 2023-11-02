// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Temporary stand-in for the real admin api clients defined in canton. Needed only for upgrades testing
// We should intend to replace this as soon as possible
package com.daml.lf.engine.script.v2.ledgerinteraction
package grpcLedgerClient

import com.daml.ledger.api.auth.client.LedgerCallCredentials.authenticatingStub
import com.daml.ledger.client.configuration.LedgerClientChannelConfiguration
import com.daml.ledger.client.GrpcChannel
import com.digitalasset.canton.participant.admin.{v0 => admin_package_service}
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.AbstractStub
import java.io.Closeable
import scala.concurrent.{ExecutionContext, Future}

final class AdminLedgerClient private (
    val channel: Channel,
    token: Option[String],
)(implicit ec: ExecutionContext)
    extends Closeable {

  private val packageServiceStub =
    AdminLedgerClient.stub(admin_package_service.PackageServiceGrpc.stub(channel), token)

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
