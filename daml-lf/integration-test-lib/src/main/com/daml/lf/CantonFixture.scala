// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package integrationtest

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, OwnedResource, SuiteResource}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.data.Ref
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import org.scalatest.Suite

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

import java.nio.file.{Files, Path, Paths}

@scala.annotation.nowarn("msg=match may not be exhaustive")
object CantonFixture {

  final case class CompiledDar(
      mainPkg: Ref.PackageId,
      compiledPackages: PureCompiledPackages,
  )

  def readDar(
      path: Path,
      compilerConfig: speedy.Compiler.Config = speedy.Compiler.Config.Dev,
  ): CompiledDar = {
    val dar = archive.DarDecoder.assertReadArchiveFromFile(path.toFile)
    val pkgs = PureCompiledPackages.assertBuild(dar.all.toMap, compilerConfig)
    CompiledDar(dar.main._1, pkgs)
  }

  private[integrationtest] lazy val List(serverCrt, serverPem, caCrt, clientCrt, clientPem) =
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").map { src =>
      Paths.get(rlocation("test-common/test-certificates/" + src))
    }

  private val counter = new java.util.concurrent.atomic.AtomicLong()

  def freshLong() = counter.getAndIncrement()

  def freshName(prefix: String): String = {
    assert(!prefix.contains('_'))
    prefix + "__" + freshLong().toString
  }

  def freshUserId() = Ref.UserId.assertFromString(freshName("user"))

}

trait CantonFixture extends SuiteResource[Vector[Port]] with AkkaBeforeAndAfterAll {
  self: Suite =>

  override protected def afterAll(): Unit = {
    cantonCleanUp()
    super.afterAll()
  }

  final override protected lazy val suiteResource: OwnedResource[ResourceContext, Vector[Port]] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, Vector[Port]](
      CantonRunner.run(config, cantonTmpDir),
      acquisitionTimeout = 2.minute,
      releaseTimeout = 2.minute,
    )
  }

  protected def authSecret: Option[String]
  protected def darFiles: List[Path]
  protected def devMode: Boolean
  protected def nParticipants: Int
  protected def timeProviderType: TimeProviderType
  protected def tlsEnable: Boolean
  protected def applicationId: ApplicationId

  lazy val tlsConfig =
    if (tlsEnable)
      Some(
        CantonConfig.Tls(
          Paths.get(rlocation("test-common/test-certificates/server.crt")),
          Paths.get(rlocation("test-common/test-certificates/server.pem")),
          Paths.get(rlocation("test-common/test-certificates/ca.crt")),
          Paths.get(rlocation("test-common/test-certificates/client.crt")),
          Paths.get(rlocation("test-common/test-certificates/client.pem")),
        )
      )
    else
      None

  lazy val config = CantonConfig(
    darFiles = darFiles,
    authSecret = authSecret,
    devMode = devMode,
    nParticipants = nParticipants,
    timeProviderType = timeProviderType,
    tlsConfig = tlsConfig,
    applicationId = applicationId,
    debug = cantonFixtureDebugMode,
  )

  // This flag setup some behavior to ease debugging tests.
  //  If `true`
  //   - temporary file are not deleted (this requires "--test_tmpdir=/tmp/" or similar for bazel builds)
  //   - some debug info are logged.
  protected lazy val cantonFixtureDebugMode = false

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  protected def info(msg: String): Unit =
    if (cantonFixtureDebugMode) logger.info(msg)

  protected val cantonTmpDir = Files.createTempDirectory("CantonFixture")

  protected def cantonCleanUp(): Unit =
    if (cantonFixtureDebugMode)
      info(s"The temporary files are located in ${cantonTmpDir}")
    else
      com.daml.fs.Utils.deleteRecursively(cantonTmpDir)

  final protected def defaultLedgerClient(
      token: Option[String] = None,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext): Future[LedgerClient] =
    config.ledgerClient(suiteResource.value.head, token, maxInboundMessageSize)

  final protected def ledgerClients(
      token: Option[String] = None,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext): Future[Vector[LedgerClient]] =
    Future.traverse(suiteResource.value)(config.ledgerClient(_, token, maxInboundMessageSize))

}
