// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package integrationtest

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import com.daml.bazeltools.BazelRunfiles._
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, OwnedResource, SuiteResource}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.LedgerClient
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

@scala.annotation.nowarn("msg=match may not be exhaustive")
object CantonFixture {

  final case class CompiledDar(
      mainPkg: Ref.PackageId,
      compiledPackages: PureCompiledPackages,
  )

  def readDar(
      path: Path,
      compilerConfig: speedy.Compiler.Config,
  ): CompiledDar = {
    val dar = archive.DarDecoder.assertReadArchiveFromFile(path.toFile)
    val pkgs = PureCompiledPackages.assertBuild(dar.all.toMap, compilerConfig)
    CompiledDar(dar.main._1, pkgs)
  }

  private lazy val List(serverCrt, serverPem, caCrt, clientCrt, clientPem) =
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").map { src =>
      Paths.get(rlocation("test-common/test-certificates/" + src))
    }

  private def toJson(s: String): String = JsString(s).toString()
  private def toJson(path: Path): String = toJson(path.toString)

  private val counter = new java.util.concurrent.atomic.AtomicLong()

  def freshLong() = counter.getAndIncrement()

  def freshName(prefix: String): String = {
    assert(!prefix.contains('_'))
    prefix + "__" + freshLong().toString
  }

  def freshUserId() = Ref.UserId.assertFromString(freshName("user"))

  val adminUserId = Ref.UserId.assertFromString("participant_admin")

}

trait CantonFixture extends SuiteResource[Vector[Port]] with AkkaBeforeAndAfterAll {
  self: Suite =>

  import CantonFixture._

  protected def authSecret: Option[String]
  protected def darFiles: List[Path]
  protected def devMode: Boolean
  protected def nParticipants: Int
  protected def timeProviderType: TimeProviderType
  protected def tlsEnable: Boolean
  protected def applicationId: ApplicationId

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

  protected val tmpDir = Files.createTempDirectory("CantonFixture")
  protected val cantonConfigFile = tmpDir.resolve("participant.config")
  protected val cantonLogFile = tmpDir.resolve("canton.log")
  protected val portFile = tmpDir.resolve("portfile")

  private val files = List(cantonConfigFile, portFile, cantonLogFile)

  override protected def afterAll(): Unit = {
    if (cantonFixtureDebugMode)
      info(s"The temporary files are located in ${tmpDir}")
    else {
      files.foreach(file => discard(Files.deleteIfExists(file)))
      Files.delete(tmpDir)
    }
    super.afterAll()
  }

  final protected lazy val tlsConfig =
    TlsConfiguration(
      enabled = tlsEnable,
      certChainFile = Some(clientCrt.toFile),
      privateKeyFile = Some(clientPem.toFile),
      trustCollectionFile = Some(caCrt.toFile),
    )

  private def canton(): ResourceOwner[Vector[Port]] =
    new ResourceOwner[Vector[Port]] {
      override def acquire()(implicit context: ResourceContext): Resource[Vector[Port]] = {
        def start(): Future[(Vector[Port], Process)] = {
          val ports =
            Vector.fill(nParticipants)(LockedFreePort.find() -> LockedFreePort.find())
          val domainPublicApi = LockedFreePort.find()
          val domainAdminApi = LockedFreePort.find()

          val cantonPath = rlocation(
            "external/canton/lib/canton-open-source-2.7.0-SNAPSHOT.jar" // FIXME: remove hard coded version!!
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
               |  parameters.non-standard-config = yes
               |  
               |  parameters {
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
          } yield (ports.map(_._2.port), proc)
        }
        def stop(r: (Vector[Port], Process)): Future[Unit] = {
          r._2.destroy()
          discard(r._2.exitValue())
          Future.unit
        }
        Resource(start())(stop).map({ case (ports, _) => ports })
      }
    }

  final override protected lazy val suiteResource: OwnedResource[ResourceContext, Vector[Port]] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    import ResourceContext.executionContext
    new OwnedResource[ResourceContext, Vector[Port]](
      for {
        ports <- canton()
        _ <- ResourceOwner.forFuture { () =>
          Future.traverse(ports) { port =>
            for {
              client <- ledgerClient(port, adminToken)
              _ <- Future.traverse(darFiles) { file =>
                client.packageManagementClient.uploadDarFile(
                  ByteString.copyFrom(Files.readAllBytes(file))
                )
              }
            } yield ()
          }
        }
        _ = info(s"${darFiles.size} packages loaded to ${ports.size} participants")
      } yield ports,
      acquisitionTimeout = 2.minute,
      releaseTimeout = 2.minute,
    )
  }

  final protected val adminToken: Option[String] = getToken(adminUserId)

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

  final protected def defaultLedgerClient(
      token: Option[String] = None,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext): Future[LedgerClient] =
    ledgerClient(suiteResource.value.head, token, maxInboundMessageSize)

  final protected def ledgerClients(
      token: Option[String] = None,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext) =
    Future.traverse(suiteResource.value)(
      ledgerClient(_, token, maxInboundMessageSize)
    )

  private def ledgerClient(
      port: Port,
      token: Option[String],
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit ec: ExecutionContext): Future[LedgerClient] = {
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

}
