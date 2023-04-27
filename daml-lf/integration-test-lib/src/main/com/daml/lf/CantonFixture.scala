// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package integrationtest

import com.daml.bazeltools.BazelRunfiles._
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, OwnedResource, SuiteResource}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.withoutledgerid.{LedgerClient => LedgerClientWithoutId}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.{LockedFreePort, Port}
import com.daml.scalautil.Statement.discard
import com.daml.timer.RetryStrategy
import com.google.protobuf.ByteString
import org.scalatest.Suite
import spray.json.JsString
import scalaz.syntax.tag._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.Process

import java.nio.charset.StandardCharsets
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

  private[integrationtest] def toJson(s: String): String = JsString(s).toString()
  private[integrationtest] def toJson(path: Path): String = toJson(path.toString)

  private val counter = new java.util.concurrent.atomic.AtomicLong()

  def freshLong() = counter.getAndIncrement()

  def freshName(prefix: String): String = {
    assert(!prefix.contains('_'))
    prefix + "__" + freshLong().toString
  }

  def freshUserId() = Ref.UserId.assertFromString(freshName("user"))

  val adminUserId = Ref.UserId.assertFromString("participant_admin")

}

trait CantonFixtureBase {

  import CantonFixture._

  protected def authSecret: Option[String]
  protected def darFiles: List[Path]
  protected def devMode: Boolean
  protected def nParticipants: Int
  protected def timeProviderType: TimeProviderType
  protected def tlsEnable: Boolean
  protected def applicationId: ApplicationId
  protected def enableDisclosedContracts: Boolean = false

  // This flag setup some behavior to ease debugging tests.
  //  If `true`
  //   - temporary file are not deleted (this requires "--test_tmpdir=/tmp/" or similar for bazel builds)
  //   - some debug info are logged.
  protected val cantonFixtureDebugMode = false

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  protected def info(msg: String): Unit =
    if (cantonFixtureDebugMode) logger.info(msg)

  info(
    s"""CantonFixture parameters:
       |  authSecret = ${authSecret}
       |  darFiles = ${darFiles}
       |  devMode = ${devMode}
       |  nParticipants = ${nParticipants}
       |  timeProviderType = ${timeProviderType}
       |  tlsEnable = ${tlsEnable}
       |""".stripMargin
  )

  protected val cantonTmpDir = Files.createTempDirectory("CantonFixture")
  protected val cantonConfigFile = cantonTmpDir.resolve("participant.config")
  protected val cantonLogFile = cantonTmpDir.resolve("canton.log")
  protected val portFile = cantonTmpDir.resolve("portfile")

  protected def cantonCleanUp(): Unit = {
    if (cantonFixtureDebugMode)
      info(s"The temporary files are located in ${cantonTmpDir}")
    else
      com.daml.fs.Utils.deleteRecursively(cantonTmpDir)
  }

  final protected lazy val tlsConfig =
    if (tlsEnable)
      TlsConfiguration(
        enabled = tlsEnable,
        certChainFile = Some(clientCrt.toFile),
        privateKeyFile = Some(clientPem.toFile),
        trustCollectionFile = Some(caCrt.toFile),
      )
    else
      TlsConfiguration(enabled = tlsEnable)

  protected def cantonResource(implicit
      esf: ExecutionSequencerFactory
  ): ResourceOwner[Vector[Port]] =
    new ResourceOwner[Vector[Port]] {
      override def acquire()(implicit context: ResourceContext): Resource[Vector[Port]] = {
        def start(): Future[(Vector[Port], Process)] = {
          val ports =
            Vector.fill(nParticipants)(LockedFreePort.find() -> LockedFreePort.find())
          val ledgerPorts = ports.map(_._2.port)
          val domainPublicApi = LockedFreePort.find()
          val domainAdminApi = LockedFreePort.find()

          val cantonPath = rlocation(
            "external/canton/lib/canton-open-source-2.7.0-SNAPSHOT.jar"
          )
          val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
          val java = s"${System.getenv("JAVA_HOME")}/bin/java${exe}"
          val (timeType, clockType) = timeProviderType match {
            case TimeProviderType.Static => (Some("monotonic-time"), Some("sim-clock"))
            case TimeProviderType.WallClock => (None, None)
          }
          val authConfig = authSecret.fold("")(secret => s"""auth-services = [{
               |          type = unsafe-jwt-hmac-256
               |          secret = "${toJson(secret)}"
               |        }]
               |""".stripMargin)
          val tslConfig =
            if (tlsEnable)
              s"""tls {
               |          cert-chain-file = ${toJson(serverCrt)}
               |          private-key-file = ${toJson(serverPem)}
               |          trust-collection-file = ${toJson(caCrt)}
               |        }""".stripMargin
            else
              ""
          def participantConfig(i: Int) = {
            val (adminPort, ledgerApiPort) = ports(i)
            s"""participant${i} {
               |      admin-api.port = ${adminPort.port}
               |      ledger-api{
               |        port = ${ledgerApiPort.port}
               |        explicit-disclosure-unsafe = $enableDisclosedContracts
               |        ${authConfig}
               |        ${tslConfig}
               |      }
               |      storage.type = memory
               |      parameters = {
               |        enable-engine-stack-traces = true
               |        dev-version-support = ${devMode}
               |      }
               |      ${timeType.fold("")(x => "testing-time.type = " + x)}
               |    }""".stripMargin
          }
          val participantsConfig =
            (0 until nParticipants).map(participantConfig).mkString("\n")
          val cantonConfig =
            s"""canton {
               |  parameters{
               |    non-standard-config = yes
               |    dev-version-support = yes
               |    ports-file = ${toJson(portFile)}
               |    ${clockType.fold("")(x => "clock.type = " + x)}
               |  }
               |
               |  domains {
               |    local {
               |      storage.type = memory
               |      public-api.port = ${domainPublicApi.port}
               |      admin-api.port = ${domainAdminApi.port}
               |      init.domain-parameters.protocol-version = ${if (devMode) Int.MaxValue else 4}
               |    }
               |  }
               |  participants {
               |    ${participantsConfig}
               |  }
               |}
          """.stripMargin
          discard(Files.write(cantonConfigFile, cantonConfig.getBytes(StandardCharsets.UTF_8)))
          val debugOptions =
            if (cantonFixtureDebugMode) List("--log-file-name", cantonLogFile.toString, "--verbose")
            else List.empty
          for {
            proc <- Future(
              Process(
                java ::
                  "-jar" ::
                  cantonPath ::
                  "daemon" ::
                  "--auto-connect-local" ::
                  "-c" ::
                  cantonConfigFile.toString ::
                  debugOptions
              ).run()
            )
            _ <- RetryStrategy.constant(attempts = 240, waitTime = 1.seconds) { (_, _) =>
              info("waiting for Canton to start")
              Future(Files.size(portFile))
            }
            _ = info("Canton started")
            _ <-
              Future.traverse(ledgerPorts) { port =>
                for {
                  client <- ledgerClient(port, adminJWTToken)
                  _ <- Future.traverse(darFiles) { file =>
                    client.packageManagementClient.uploadDarFile(
                      ByteString.copyFrom(Files.readAllBytes(file))
                    )
                  }
                } yield ()
              }
            _ = info(s"${darFiles.size} packages loaded to ${ports.size} participants")
          } yield (ledgerPorts, proc)
        }
        def stop(r: (Vector[Port], Process)): Future[Unit] = {
          r._2.destroy()
          discard(r._2.exitValue())
          Future.unit
        }
        Resource(start())(stop).map({ case (ports, _) => ports })
      }
    }

  final protected lazy val adminJWTToken: Option[String] = getToken(adminUserId)

  final protected def getToken(
      userId: String,
      authSecret: Option[String] = authSecret,
  ): Option[String] = authSecret.map { secret =>
    val payload = auth.StandardJWTPayload(
      issuer = None,
      userId = userId,
      participantId = None,
      exp = None,
      format = auth.StandardJWTTokenFormat.Scope,
      audiences = List.empty,
    )
    val header = """{"alg": "HS256", "typ": "JWT"}"""
    val jwt = DecodedJwt[String](header, auth.AuthServiceJWTCodec.writeToString(payload))
    JwtSigner.HMAC256.sign(jwt, secret).toEither match {
      case Right(a) => a.value
      case Left(e) => throw new IllegalStateException(e.toString)
    }
  }

  final protected def ledgerClient(
      port: Port,
      token: Option[String],
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): Future[LedgerClient] = {
    import com.daml.ledger.client.configuration._
    LedgerClient.singleHost(
      hostIp = "localhost",
      port = port.value,
      configuration = LedgerClientConfiguration(
        applicationId = token.fold(applicationId.unwrap)(_ => ""),
        ledgerIdRequirement = LedgerIdRequirement.none,
        commandClient = CommandClientConfiguration.default,
        token = token,
      ),
      channelConfig = LedgerClientChannelConfiguration(
        sslContext = tlsConfig.client(),
        maxInboundMessageSize = maxInboundMessageSize,
      ),
    )
  }

  final protected def ledgerClientWithoutId(
      port: Port,
      token: Option[String],
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): LedgerClientWithoutId = {
    import com.daml.ledger.client.configuration._
    LedgerClientWithoutId.singleHost(
      hostIp = "localhost",
      port = port.value,
      configuration = LedgerClientConfiguration(
        applicationId = token.fold(applicationId.unwrap)(_ => ""),
        ledgerIdRequirement = LedgerIdRequirement.none,
        commandClient = CommandClientConfiguration.default,
        token = token,
      ),
      channelConfig = LedgerClientChannelConfiguration(
        sslContext = tlsConfig.client(),
        maxInboundMessageSize = maxInboundMessageSize,
      ),
    )
  }

}

trait CantonFixture
    extends CantonFixtureBase
    with SuiteResource[Vector[Port]]
    with AkkaBeforeAndAfterAll {
  self: Suite =>

  override protected def afterAll(): Unit = {
    cantonCleanUp()
    super.afterAll()
  }

  final override protected lazy val suiteResource: OwnedResource[ResourceContext, Vector[Port]] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, Vector[Port]](
      cantonResource,
      acquisitionTimeout = 2.minute,
      releaseTimeout = 2.minute,
    )
  }

  final protected def defaultLedgerClient(
      token: Option[String] = None,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext): Future[LedgerClient] =
    ledgerClient(suiteResource.value.head, token, maxInboundMessageSize)

  final protected def defaultLedgerClientWithoutId(
      token: Option[String] = None,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext): LedgerClientWithoutId =
    ledgerClientWithoutId(suiteResource.value.head, token, maxInboundMessageSize)

  final protected def ledgerClients(
      token: Option[String] = None,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext): Future[Vector[LedgerClient]] =
    Future.traverse(suiteResource.value)(ledgerClient(_, token, maxInboundMessageSize))

}
