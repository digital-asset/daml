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
import com.daml.ledger.client.withoutledgerid.{LedgerClient => LedgerClientWithoutId}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import org.scalatest.Suite

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

import java.nio.file.{Path, Paths, Files}
import java.util.UUID

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

  def freshName(prefix: String): String = {
    assert(!prefix.contains('_'))
    prefix + "__" + UUID.randomUUID()
  }

  def freshUserId(): Ref.UserId = Ref.UserId.assertFromString(freshName("user"))

  def freshParty(): Ref.Party = Ref.Party.assertFromString(freshName("Party"))

}

trait CantonFixtureWithResource[A]
    extends SuiteResource[(Vector[Port], A)]
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundAll {
  self: Suite =>

  protected lazy val authSecret: Option[String] = Option.empty
  protected lazy val darFiles: List[Path] = List.empty
  protected lazy val devMode: Boolean = false
  protected lazy val nParticipants: Int = 1
  protected lazy val timeProviderType: TimeProviderType = TimeProviderType.WallClock
  protected lazy val tlsEnable: Boolean = false
  protected lazy val enableDisclosedContracts: Boolean = false
  protected lazy val applicationId: ApplicationId = ApplicationId(getClass.getName)

  // This flag setup some behavior to ease debugging tests.
  //  If `true`
  //   - temporary file are not deleted (this requires "--test_tmpdir=/tmp/" or similar for bazel builds)
  //   - some debug info are logged.
  protected val cantonFixtureDebugMode = false

  final protected val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  if (cantonFixtureDebugMode)
    logger
      .asInstanceOf[ch.qos.logback.classic.Logger]
      .setLevel(ch.qos.logback.classic.Level.INFO)

  override protected def afterAll(): Unit = {
    cantonCleanUp()
    super.afterAll()
  }

  protected def makeAdditionalResource(ports: Vector[Port]): ResourceOwner[A]

  final override protected lazy val suiteResource
      : OwnedResource[ResourceContext, (Vector[Port], A)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (Vector[Port], A)](
      for {
        ports <- CantonRunner.run(config, cantonTmpDir, logger)
        additional <- makeAdditionalResource(ports)
      } yield (ports, additional),
      acquisitionTimeout = 2.minute,
      releaseTimeout = 2.minute,
    )
  }

  lazy val config = CantonConfig(
    applicationId = applicationId,
    darFiles = darFiles,
    authSecret = authSecret,
    devMode = devMode,
    nParticipants = nParticipants,
    timeProviderType = timeProviderType,
    tlsEnable = tlsEnable,
    debug = cantonFixtureDebugMode,
    enableDisclosedContracts = enableDisclosedContracts,
  )

  protected def info(msg: String): Unit =
    if (cantonFixtureDebugMode) logger.info(msg)

  protected val cantonTmpDir = Files.createTempDirectory("CantonFixture")

  protected def cantonCleanUp(): Unit =
    if (cantonFixtureDebugMode)
      info(s"The temporary files are located in ${cantonTmpDir}")
    else
      com.daml.fs.Utils.deleteRecursively(cantonTmpDir)

  final protected def ports: Vector[Port] = suiteResource.value._1
  final protected def additional: A = suiteResource.value._2

  final protected def defaultLedgerClient(
      token: Option[String] = None,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext): Future[LedgerClient] =
    config.ledgerClient(ports.head, token, maxInboundMessageSize)

  final protected def defaultLedgerClientWithoutId(
      token: Option[String] = None,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext): LedgerClientWithoutId =
    config.ledgerClientWithoutId(ports.head, token, maxInboundMessageSize)

  final protected def ledgerClients(
      token: Option[String] = None,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext): Future[Vector[LedgerClient]] =
    Future.traverse(ports)(config.ledgerClient(_, token, maxInboundMessageSize))

}

trait CantonFixture extends CantonFixtureWithResource[Unit] {
  self: Suite =>
  override protected def makeAdditionalResource(ports: Vector[Port]): ResourceOwner[Unit] =
    new ResourceOwner[Unit] {
      override def acquire()(implicit context: ResourceContext): Resource[Unit] =
        Resource(Future.successful(()))(Future.successful(_))
    }
}
