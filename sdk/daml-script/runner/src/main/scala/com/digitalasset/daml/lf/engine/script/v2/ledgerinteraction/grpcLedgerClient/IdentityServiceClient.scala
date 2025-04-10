// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Temporary stand-in for the real admin api clients defined in canton. Needed only for upgrades testing
// We should intend to replace this as soon as possible
package com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction
package grpcLedgerClient

import com.daml.grpc.AuthCallCredentials
import com.digitalasset.canton.ledger.client.GrpcChannel
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.AbstractStub
import com.digitalasset.canton.topology.admin.{v30 => admin_topology_service}

import java.io.Closeable
import scala.concurrent.{ExecutionContext, Future}

class IdentityServiceClient private[grpcLedgerClient] (
    val channel: Channel,
    token: Option[String],
)(implicit ec: ExecutionContext)
    extends Closeable {

  private[grpcLedgerClient] val identityServiceStub =
    IdentityServiceClient.stub(
      admin_topology_service.IdentityInitializationServiceGrpc.stub(channel),
      token,
    )

  def getId(): Future[Option[String]] =
    identityServiceStub
      .getId(admin_topology_service.GetIdRequest())
      .map(res => if (res.initialized) Some(res.uniqueIdentifier) else None)

  override def close(): Unit = GrpcChannel.close(channel)
}

object IdentityServiceClient {
  private[grpcLedgerClient] def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(AuthCallCredentials.authorizingStub(stub, _))

  /** A convenient shortcut to build a [[IdentityServiceClient]], use [[fromBuilder]] for a more
    * flexible alternative.
    */
  def singleHost(
      hostIp: String,
      port: Int,
      token: Option[String] = None,
      channelConfig: LedgerClientChannelConfiguration,
  )(implicit
      ec: ExecutionContext
  ): IdentityServiceClient =
    fromBuilder(channelConfig.builderFor(hostIp, port), token)

  def fromBuilder(
      builder: NettyChannelBuilder,
      token: Option[String] = None,
  )(implicit ec: ExecutionContext): IdentityServiceClient =
    new IdentityServiceClient(
      GrpcChannel.withShutdownHook(builder),
      token,
    )
}
