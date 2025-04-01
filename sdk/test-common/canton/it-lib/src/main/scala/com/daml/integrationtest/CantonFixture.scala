// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package integrationtest

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.{
  OwnedResource,
  PekkoBeforeAndAfterAll,
  SuiteResource,
  SuiteResourceManagementAroundAll,
}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.digitalasset.daml.lf.data.Ref
import com.daml.ports.Port
import org.scalatest.Suite

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

import com.digitalasset.canton.tracing.TraceContext

@scala.annotation.nowarn("msg=match may not be exhaustive")
object CantonFixture {

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

  case class LedgerPorts(ledgerPort: Port, adminPort: Port)
}

trait CantonFixtureWithResource[A]
    extends SuiteResource[(Vector[CantonFixture.LedgerPorts], A)]
    with PekkoBeforeAndAfterAll
    with SuiteResourceManagementAroundAll {
  self: Suite =>
  import CantonConfig.TimeProviderType

  protected lazy val authSecret: Option[String] = Option.empty
  protected lazy val darFiles: List[Path] = List.empty
  protected lazy val devMode: Boolean = false
  protected lazy val nParticipants: Int = 1
  protected lazy val timeProviderType: TimeProviderType = TimeProviderType.WallClock
  protected lazy val tlsEnable: Boolean = false
  protected lazy val bootstrapScript: Option[String] = Option.empty
  protected lazy val userId: Option[Ref.UserId] =
    Some(Ref.UserId.assertFromString(getClass.getName))
  protected lazy val cantonJar: Path = CantonRunner.cantonPath
  protected lazy val targetScope: Option[String] = Option.empty

  // This flag setup some behavior to ease debugging tests.
  //  If `CantonFixtureDebugKeepTmpFiles` or `CantonFixtureDebugRemoveTmpFiles`
  //   - some debug info are logged.
  //   - output from the canton process is sent to stdout
  //   - canton.log is generated
  //  Further, if `CantonFixtureDebugKeepTmpFiles`
  //   - temporary files are not deleted once testing is done
  //     (this requires "--test_tmpdir=/tmp/" or "--sandbox_debug" or similar for bazel builds)
  sealed class CantonFixtureDebugMode
  final case object CantonFixtureDebugKeepTmpFiles extends CantonFixtureDebugMode
  final case object CantonFixtureDebugRemoveTmpFiles extends CantonFixtureDebugMode
  final case object CantonFixtureDontDebug extends CantonFixtureDebugMode

  protected val cantonFixtureDebugMode: CantonFixtureDebugMode = CantonFixtureDebugKeepTmpFiles
  def cantonFixtureDebugModeIsDebug: Boolean = cantonFixtureDebugMode match {
    case CantonFixtureDebugKeepTmpFiles | CantonFixtureDebugRemoveTmpFiles => true
    case _ => false
  }

  protected val disableUpgradeValidation: Boolean = false

  // If we need to enable debugging (logs, etc.), but still want to clean up the
  // temporary files after a test is done running
  protected val cantonFixtureDebugModeRemoveTmpFilesRegardless = false

  // When true, the canton process is started with the necessary flags to enable remote debugging and is suspended until
  // a debugger is attached. In order to use this feature, launch the test making use of the fixture from IntelliJ
  // *in debug mode* and look for "Listening for transport dt_socket at address: 5005" in the console output. IntelliJ
  // should display a clickable chip labelled "Attach debugger" next to this line. Clicking on this chip will attach the
  // debugger to the canton process and resume execution. Breakpoints in the canton code under canton/* will then be
  // taken into account by IntelliJ.
  protected val remoteJavaDebugging: Boolean = false

  final protected val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  if (cantonFixtureDebugModeIsDebug) {
    logger
      .asInstanceOf[ch.qos.logback.classic.Logger]
      .setLevel(ch.qos.logback.classic.Level.INFO)
  }

  override protected def afterAll(): Unit = {
    cantonCleanUp()
    super.afterAll()
  }

  protected def makeAdditionalResource(ports: Vector[CantonFixture.LedgerPorts]): ResourceOwner[A]

  final override protected lazy val suiteResource
      : OwnedResource[ResourceContext, (Vector[CantonFixture.LedgerPorts], A)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (Vector[CantonFixture.LedgerPorts], A)](
      for {
        ports <- CantonRunner.run(config, cantonTmpDir, logger, darFiles)
        additional <- makeAdditionalResource(ports)
      } yield (ports, additional),
      acquisitionTimeout = 2.minute,
      releaseTimeout = 2.minute,
    )
  }

  lazy val config = CantonConfig(
    jarPath = cantonJar,
    authSecret = authSecret,
    devMode = devMode,
    nParticipants = nParticipants,
    timeProviderType = timeProviderType,
    tlsEnable = tlsEnable,
    debug = cantonFixtureDebugModeIsDebug,
    bootstrapScript = bootstrapScript,
    targetScope = targetScope,
    disableUpgradeValidation = disableUpgradeValidation,
    enableRemoteJavaDebugging = remoteJavaDebugging,
  )

  protected def info(msg: String): Unit =
    if (cantonFixtureDebugModeIsDebug) logger.info(msg)

  protected val cantonTmpDir = Files.createTempDirectory("CantonFixture")

  protected def cantonCleanUp(): Unit =
    cantonFixtureDebugMode match {
      case CantonFixtureDebugKeepTmpFiles =>
        info(s"The temporary files are located in ${cantonTmpDir}")
      case _ =>
        com.daml.fs.Utils.deleteRecursively(cantonTmpDir)
    }

  final protected def ports: Vector[Port] = suiteResource.value._1.map(_.ledgerPort)
  final protected def ledgerPorts: Vector[CantonFixture.LedgerPorts] = suiteResource.value._1
  final protected def additional: A = suiteResource.value._2

  implicit val traceContext: TraceContext = TraceContext.empty

  final protected def defaultLedgerClient(
      token: Option[String] = None,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext): Future[LedgerClient] =
    config.ledgerClient(ports.head, token, userId, maxInboundMessageSize)

}

trait CantonFixture extends CantonFixtureWithResource[Unit] {
  self: Suite =>
  override protected def makeAdditionalResource(
      ports: Vector[CantonFixture.LedgerPorts]
  ): ResourceOwner[Unit] =
    new ResourceOwner[Unit] {
      override def acquire()(implicit context: ResourceContext): Resource[Unit] =
        Resource(Future.successful(()))(Future.successful(_))
    }
}
