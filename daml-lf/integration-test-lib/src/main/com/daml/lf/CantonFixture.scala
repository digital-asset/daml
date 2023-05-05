// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package integrationtest

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  OwnedResource,
  SuiteResource,
  SuiteResourceManagementAroundAll,
}
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

trait CantonFixture
    extends SuiteResource[Vector[Port]]
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundAll {
  self: Suite =>

  protected lazy val authSecret: Option[String] = Option.empty
  protected lazy val darFiles: List[Path] = List.empty
  protected lazy val devMode: Boolean = false
  protected lazy val nParticipants: Int = 1
  protected lazy val timeProviderType: TimeProviderType = TimeProviderType.WallClock
  protected lazy val tlsEnable: Boolean = false

  // This flag setup some behavior to ease debugging tests.
  //  If `true`
  //   - temporary file are not deleted (this requires "--test_tmpdir=/tmp/" or similar for bazel builds)
  //   - some debug info are logged.
  protected val cantonFixtureDebugMode = false

  final protected val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  if (cantonFixtureDebugMode)
    logger.asInstanceOf[ch.qos.logback.classic.Logger].setLevel(ch.qos.logback.classic.Level.INFO)

  override protected def afterAll(): Unit = {
    cantonCleanUp()
    super.afterAll()
  }

  final override protected lazy val suiteResource: OwnedResource[ResourceContext, Vector[Port]] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, Vector[Port]](
      CantonRunner.run(config, cantonTmpDir, logger),
      acquisitionTimeout = 2.minute,
      releaseTimeout = 2.minute,
    )
  }

  final protected lazy val applicationId: ApplicationId = ApplicationId(getClass.getName)

  lazy val config = CantonConfig(
    applicationId = applicationId,
    darFiles = darFiles,
    authSecret = authSecret,
    devMode = devMode,
    nParticipants = nParticipants,
    timeProviderType = timeProviderType,
    tlsEnable = tlsEnable,
    debug = cantonFixtureDebugMode,
  )

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
