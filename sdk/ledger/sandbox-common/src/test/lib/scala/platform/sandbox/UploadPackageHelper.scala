// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.client.services.admin.PackageManagementClient
import com.daml.ledger.client.withoutledgerid.LedgerClient
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest
import com.daml.ports.Port
import com.google.protobuf

import java.io.File
import java.nio.file.Files
import scala.concurrent.{ExecutionContext, Future}

object UploadPackageHelper extends SandboxRequiringAuthorizationFuns {

  def adminLedgerClient(port: Port, config: Config, jwtSecret: String)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
  ): LedgerClient = {
    adminLedgerClient(port, config, Some(toHeader(adminTokenStandardJWT, secret = jwtSecret)))
  }

  def adminLedgerClient(port: Port, config: Config, token: Option[String])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
  ): LedgerClient = {
    val participant = config.participants(SandboxOnXForTest.ParticipantId)
    val sslContext = participant.apiServer.tls.flatMap(_.client())
    val clientConfig = LedgerClientConfiguration(
      applicationId = "admin-client",
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      token = token,
    )
    LedgerClient.singleHost(
      hostIp = "localhost",
      port = port.value,
      configuration = clientConfig,
      channelConfig = LedgerClientChannelConfiguration(sslContext),
    )
  }

  private def uploadDarFiles(
      client: PackageManagementClient,
      files: List[File],
  )(implicit
      ec: ExecutionContext
  ): Future[List[Unit]] =
    if (files.isEmpty) Future.successful(List())
    else
      Future.sequence(files.map(uploadDarFile(client)))

  private def uploadDarFile(client: PackageManagementClient)(file: File): Future[Unit] =
    client.uploadDarFile(
      protobuf.ByteString.copyFrom(Files.readAllBytes(file.toPath))
    )

  def uploadDarFiles(
      client: LedgerClient,
      files: List[File],
  )(implicit
      ec: ExecutionContext
  ): Future[List[Unit]] =
    uploadDarFiles(client.packageManagementClient, files)

}
