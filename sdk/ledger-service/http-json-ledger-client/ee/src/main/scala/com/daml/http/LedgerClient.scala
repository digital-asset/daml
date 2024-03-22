// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.file.{Files, Path}
import java.security.{KeyFactory, PrivateKey}
import java.security.cert.{CertificateFactory, X509Certificate}
import java.security.spec.PKCS8EncodedKeySpec

import com.daml.ledger.client.configuration.LedgerClientChannelConfiguration
import com.daml.nonrepudiation.client.SigningInterceptor
import io.grpc.netty.NettyChannelBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object LedgerClient extends LedgerClientBase {

  private def loadCertificate(
      path: Path
  ): Try[X509Certificate] = {
    val newInputStream = Try(Files.newInputStream(path))
    val certificate =
      for {
        input <- newInputStream
        factory <- Try(CertificateFactory.getInstance("X.509"))
        certificate <- Try(factory.generateCertificate(input).asInstanceOf[X509Certificate])
      } yield certificate
    newInputStream.foreach(_.close())
    certificate
  }

  private def loadPrivateKey(
      path: Path,
      algorithm: String,
  ): Try[PrivateKey] =
    for {
      bytes <- Try(Files.readAllBytes(path))
      keySpec <- Try(new PKCS8EncodedKeySpec(bytes))
      factory <- Try(KeyFactory.getInstance(algorithm))
      key <- Try(factory.generatePrivate(keySpec))
    } yield key

  def channelBuilder(
      ledgerHost: String,
      ledgerPort: Int,
      clientChannelConfig: LedgerClientChannelConfiguration,
      nonRepudiationConfig: nonrepudiation.Configuration.Cli,
  )(implicit executionContext: ExecutionContext): Future[NettyChannelBuilder] = {
    val base = clientChannelConfig.builderFor(ledgerHost, ledgerPort)
    Future
      .fromTry(nonRepudiationConfig.validated)
      .map(_.fold(base) { config =>
        val channelWithInterceptor =
          for {
            certificate <- loadCertificate(config.certificateFile)
            key <- loadPrivateKey(config.privateKeyFile, config.privateKeyAlgorithm)
          } yield base.intercept(SigningInterceptor.signCommands(key, certificate))
        channelWithInterceptor.get
      })
  }
}
