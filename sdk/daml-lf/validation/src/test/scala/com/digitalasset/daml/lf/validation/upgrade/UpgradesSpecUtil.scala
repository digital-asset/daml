// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Temporary stand-in for the real admin api clients defined in canton. Needed only for upgrades testing
// We should intend to replace this as soon as possible

package com.digitalasset.daml.lf.validation
package upgrade

import com.daml.grpc.AuthCallCredentials
import com.digitalasset.canton.ledger.client.GrpcChannel
import com.digitalasset.canton.admin.participant.{v30 => admin_package_service}
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

  def validateDar(bytes: ByteString, filename: String): Future[Unit] = {
    packageServiceStub
      .validateDar(
        admin_package_service.ValidateDarRequest(
          bytes,
          filename,
        )
      )
      .map(_ => ())
  }

  def uploadDar(bytes: ByteString, filename: String): Future[Unit] = {
    packageServiceStub
      .uploadDar(
        admin_package_service.UploadDarRequest(
          Seq(
            admin_package_service.UploadDarRequest.UploadDarData(
              bytes,
              Some(filename),
              None, // empty string is the default expected_main_package_id
            )
          ),
          true,
          true,
        )
      )
      .map(_ => ())
  }

  override def close(): Unit = GrpcChannel.close(channel)
}

object AdminLedgerClient {
  def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(AuthCallCredentials.authorizingStub(stub, _))

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
