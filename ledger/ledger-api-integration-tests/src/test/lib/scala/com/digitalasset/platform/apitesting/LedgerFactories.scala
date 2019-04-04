// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.nio.file.Path

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.PlatformApplications
import com.digitalasset.platform.damllf.PackageParser

import scala.util.control.NonFatal

object LedgerFactories {

  private def packageIdFromString(str: String): Either[Throwable, Ref.PackageId] =
    Ref.PackageId.fromString(str) match {
      case Left(e) => Left(new IllegalStateException(e))
      case Right(x) => Right(x)
    }

  private def getPackageId(path: Path): Either[Throwable, Ref.PackageId] = {
    val inputStream: InputStream = new BufferedInputStream(new FileInputStream(path.toFile))
    try {
      if (path.toFile.getName.endsWith(".dalf")) {
        PackageParser.getPackageIdFromDalf(inputStream)
      } else
        PackageParser.getPackageIdFromDar(inputStream).flatMap(packageIdFromString)
    } catch {
      case NonFatal(t) => throw new RuntimeException(s"Couldn't parse ${path}", t)
    }
  }

  private def getPackageIdOrThrow(path: Path): Ref.PackageId =
    getPackageId(path).fold(t => throw t, identity)

  def createSandboxResource(config: PlatformApplications.Config)(
      implicit esf: ExecutionSequencerFactory): Resource[LedgerContext.SingleChannelContext] = {
    val packageIds = config.darFiles.map(getPackageIdOrThrow)
    SandboxServerResource(PlatformApplications.sandboxApplication(config)).map {
      case PlatformChannels(channel) =>
        LedgerContext.SingleChannelContext(channel, config.ledgerId, packageIds)
    }
  }

  def createRemoteServerResource(config: PlatformApplications.Config, host: String, port: Int)(
      implicit esf: ExecutionSequencerFactory): Resource[LedgerContext.SingleChannelContext] = {

    val packageIds = config.darFiles.map(getPackageIdOrThrow)
    RemoteServerResource(host, port).map {
      case PlatformChannels(channel) =>
        LedgerContext.SingleChannelContext(channel, None, packageIds)
    }
  }
}
