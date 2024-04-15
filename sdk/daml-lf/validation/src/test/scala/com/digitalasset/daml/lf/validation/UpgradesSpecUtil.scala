// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Temporary stand-in for the real admin api clients defined in canton. Needed only for upgrades testing
// We should intend to replace this as soon as possible

package com.daml.lf.validation

import com.daml.ledger.api.auth.client.LedgerCallCredentials.authenticatingStub
//import com.digitalasset.canton.ledger.client.LedgerCallCredentials.authenticatingStub
import com.daml.ledger.client.GrpcChannel
import com.digitalasset.canton.participant.admin.{v0 => admin_package_service}
import io.grpc.Channel
import io.grpc.stub.AbstractStub
import java.io.Closeable
import scala.concurrent.{ExecutionContext, Future}
import com.google.protobuf.ByteString
import com.daml.ports.Port
import com.daml.integrationtest.CantonConfig

private[validation] final class AdminLedgerClient(
    val channel: Channel,
    token: Option[String],
)(implicit ec: ExecutionContext)
    extends Closeable {

  private val packageServiceStub =
    AdminLedgerClient.stub(admin_package_service.PackageServiceGrpc.stub(channel), token)

  def uploadDar(bytes: ByteString, filename: String): Future[Unit] = {
    packageServiceStub
      .uploadDar(
        admin_package_service.UploadDarRequest(
          bytes,
          filename,
          false,
          false,
        )
      )
      .map(_ => ())
  }

  override def close(): Unit = GrpcChannel.close(channel)
}

object AdminLedgerClient {
  def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(authenticatingStub(stub, _))

  def singleHost(
      port: Port,
      cantonConfig: CantonConfig,
  )(implicit
      ec: ExecutionContext
  ): AdminLedgerClient =
    new AdminLedgerClient(
      GrpcChannel.withShutdownHook(cantonConfig.channelBuilder(port)),
      cantonConfig.adminToken,
    )
}
