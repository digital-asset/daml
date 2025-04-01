// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Temporary stand-in for the real admin api clients defined in canton. Needed only for upgrades testing
// We should intend to replace this as soon as possible
package com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction
package grpcLedgerClient
package test

import com.digitalasset.canton.admin.participant.{v30 => admin_package_service}
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import com.digitalasset.canton.ledger.client.GrpcChannel
import com.google.protobuf.ByteString
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import java.io.{File, FileInputStream}
import scala.concurrent.{ExecutionContext, Future}

class TestingAdminLedgerClient(
    channel: Channel,
    token: Option[String],
    participantId: String,
)(implicit ec: ExecutionContext)
    extends AdminLedgerClient(channel, token, participantId) {

  def uploadDar(file: File): Future[Either[String, String]] =
    packageServiceStub
      .uploadDar(
        admin_package_service.UploadDarRequest(
          dars = Seq(
            admin_package_service.UploadDarRequest.UploadDarData(
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
        import admin_package_service.UploadDarResponse
        response match {
          case UploadDarResponse(hash) => Right(hash.head)
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
      participantId: String,
  )(implicit
      ec: ExecutionContext
  ): TestingAdminLedgerClient =
    fromBuilder(channelConfig.builderFor(hostIp, port), token, participantId)

  def fromBuilder(
      builder: NettyChannelBuilder,
      token: Option[String] = None,
      participantId: String,
  )(implicit ec: ExecutionContext): TestingAdminLedgerClient =
    new TestingAdminLedgerClient(
      GrpcChannel.withShutdownHook(builder),
      token,
      participantId,
    )
}
