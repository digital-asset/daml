// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.io.File
import java.net.InetAddress
import java.time.Duration

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, Uri, headers}
import akka.http.scaladsl.model.Uri.Path
import com.daml.bazeltools.BazelRunfiles
import com.daml.daml_lf_dev.DamlLf
import com.daml.jwt.domain.DecodedJwt
import com.daml.jwt.{HMAC256Verifier, JwtSigner, JwtVerifierBase}
import com.daml.ledger.api.auth
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{CommandClientConfiguration, LedgerClientConfiguration, LedgerIdRequirement}
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref._
import com.daml.oauth.middleware.{Config => MiddlewareConfig, Server => MiddlewareServer}
import com.daml.oauth.server.{Config => OAuthConfig, Server => OAuthServer}
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox
import com.daml.platform.sandbox.SandboxServer
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.{LockedFreePort, Port}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.lf.engine.trigger.dao.DbTriggerDao
import com.daml.testing.postgresql.PostgresAroundAll
import com.daml.timer.RetryStrategy
import com.typesafe.scalalogging.StrictLogging
import eu.rekawek.toxiproxy._
import org.scalactic.source
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite, SuiteMixin}

import scala.collection.concurrent.TrieMap
import scala.concurrent._
import scala.concurrent.duration._
import scala.sys.process.Process
import scala.util.Success

/**
 * A test-fixture that persists cookies between http requests for each test-case.
 */
trait HttpCookies extends BeforeAndAfterEach { this: Suite =>
  private val cookieJar = TrieMap[String, String]()

  override protected def afterEach(): Unit = {
    try super.afterEach()
    finally cookieJar.clear()
  }

  /**
   * Adds a Cookie header for the currently stored cookies and performs the given http request.
   */
  def httpRequest(request: HttpRequest)(
    implicit system: ActorSystem,
    ec: ExecutionContext): Future[HttpResponse] = {
    Http()
      .singleRequest {
        if (cookieJar.nonEmpty) {
          val cookies = headers.Cookie(values = cookieJar.to[Seq]: _*)
          request.addHeader(cookies)
        } else {
          request
        }
      }
      .andThen {
        case Success(resp) =>
          resp.headers.foreach {
            case headers.`Set-Cookie`(cookie) =>
              cookieJar.update(cookie.name, cookie.value)
            case _ =>
          }
      }
  }

  /**
   * Same as [[httpRequest]] but will follow redirections.
   */
  def httpRequestFollow(request: HttpRequest, maxRedirections: Int = 10)(
    implicit system: ActorSystem,
    ec: ExecutionContext): Future[HttpResponse] = {
    httpRequest(request).flatMap {
      case resp @ HttpResponse(StatusCodes.Redirection(_), _, _, _) =>
        if (maxRedirections == 0) {
          throw new RuntimeException("Too many redirections")
        } else {
          val uri = resp.header[headers.Location].get.uri
          httpRequestFollow(HttpRequest(uri = uri), maxRedirections - 1)
        }
      case resp => Future(resp)
    }
  }
}

trait ToxiproxyFixture extends BeforeAndAfterAll with AkkaBeforeAndAfterAll {
  self: Suite =>

  protected def toxiproxyClient = resource._1

  private var resource: (ToxiproxyClient, Process) = null
  private lazy implicit val executionContext: ExecutionContext = system.getDispatcher

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val host = InetAddress.getLoopbackAddress
    val isWindows: Boolean = sys.props("os.name").toLowerCase.contains("windows")
    val exe =
      if (!isWindows) BazelRunfiles.rlocation("external/toxiproxy_dev_env/bin/toxiproxy-cmd")
      else BazelRunfiles.rlocation("external/toxiproxy_dev_env/toxiproxy-server-windows-amd64.exe")
    val port = LockedFreePort.find()
    val proc = Process(Seq(exe, "--port", port.port.value.toString)).run()
    Await.result(RetryStrategy.constant(attempts = 3, waitTime = 2.seconds) { (_, _) =>
      Future(port.testAndUnlock(host))
    }, 10.seconds)
    val client = new ToxiproxyClient(host.getHostName, port.port.value)
    resource = (client, proc)
  }

  override protected def afterAll(): Unit = {
    resource._2.destroy()
    val _ = resource._2.exitValue()

    super.afterAll()
  }
}

trait AbstractAuthFixture extends SuiteMixin {
  self: Suite =>

  protected def authService: Option[auth.AuthService]
  protected def authToken(payload: AuthServiceJWTPayload): Option[String]
  protected def authConfig: AuthConfig
}

trait NoAuthFixture extends AbstractAuthFixture {
  self: Suite =>

  protected override def authService: Option[auth.AuthService] = None
  protected override def authToken(payload: AuthServiceJWTPayload): Option[String] = None
  protected override def authConfig: AuthConfig = NoAuth
}

trait AuthMiddlewareFixture
  extends AbstractAuthFixture
    with BeforeAndAfterAll
    with AkkaBeforeAndAfterAll {
  self: Suite =>

  protected def authService: Option[auth.AuthService] = Some(auth.AuthServiceJWT(authVerifier))
  protected def authToken(payload: AuthServiceJWTPayload): Option[String] = Some {
    val header = """{"alg": "HS256", "typ": "JWT"}"""
    val jwt = JwtSigner.HMAC256.sign(DecodedJwt(header, AuthServiceJWTCodec.compactPrint(payload)), authSecret).toOption.get
    jwt.value
  }
  protected def authConfig: AuthConfig = AuthMiddleware(authMiddlewareUri)

  private def authVerifier: JwtVerifierBase = HMAC256Verifier(authSecret).toOption.get
  private def authMiddleware: ServerBinding = resource._2
  private def authMiddlewareUri: Uri = Uri()
    .withScheme("http")
    .withAuthority(authMiddleware.localAddress.getHostString, authMiddleware.localAddress.getPort)

  private val authSecret: String = "secret"
  private var resource: (ServerBinding, ServerBinding) = null
  private lazy implicit val executionContext: ExecutionContext = system.getDispatcher

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    resource = Await.result(for {
      oauth <- OAuthServer.start(
        OAuthConfig(
          port = Port.Dynamic,
          ledgerId = this.getClass.getSimpleName,
          // TODO[AH] Choose application ID, see https://github.com/digital-asset/daml/issues/7671
          applicationId = None,
          jwtSecret = authSecret,
        ))
      serverUri = Uri()
        .withScheme("http")
        .withAuthority(oauth.localAddress.getHostString, oauth.localAddress.getPort)
      middleware <- MiddlewareServer.start(
        MiddlewareConfig(
          port = Port.Dynamic,
          oauthAuth = serverUri.withPath(Path./("authorize")),
          oauthToken = serverUri.withPath(Path./("token")),
          clientId = "oauth-middleware-id",
          clientSecret = "oauth-middleware-secret",
          tokenVerifier = authVerifier,
        ))
    } yield (oauth, middleware),
      1.minute)
  }

  override protected def afterAll(): Unit = {
    val (oauth, middleware) = resource
    middleware.unbind()
    oauth.unbind()

    super.afterAll()
  }
}

trait SandboxFixture extends BeforeAndAfterAll with AbstractAuthFixture with AkkaBeforeAndAfterAll {
  self: Suite =>

  protected val damlPackages: List[File] = List()
  protected val ledgerIdMode: LedgerIdMode = LedgerIdMode.Static(LedgerId(this.getClass.getSimpleName))
  private def sandboxConfig: SandboxConfig = sandbox.DefaultConfig.copy(
    port = Port.Dynamic,
    damlPackages = damlPackages,
    ledgerIdMode = ledgerIdMode,
    timeProviderType = Some(TimeProviderType.Static),
    authService = authService,
  )

  protected def sandboxServer: SandboxServer = resource._1
  protected def sandboxPort: Port = resource._2
  protected def sandboxClient(
    applicationId: ApplicationId,
    admin: Boolean = false,
    actAs: List[ApiTypes.Party] = List(),
    readAs: List[ApiTypes.Party] = List())(
    implicit executionContext: ExecutionContext): Future[LedgerClient] =
    LedgerClient.singleHost(
      InetAddress.getLoopbackAddress.getHostName,
      sandboxPort.value,
      LedgerClientConfiguration(
        applicationId = ApplicationId.unwrap(applicationId),
        ledgerIdRequirement = LedgerIdRequirement.none,
        commandClient = CommandClientConfiguration.default,
        sslContext = None,
        token = authToken(
          AuthServiceJWTPayload(
            ledgerId = None,
            applicationId = None,
            participantId = None,
            exp = None,
            admin = admin,
            actAs = actAs.map(ApiTypes.Party.unwrap),
            readAs = readAs.map(ApiTypes.Party.unwrap)))))

  private var resource: (SandboxServer, Port) = null

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val server = new SandboxServer(sandboxConfig, materializer)
    val port = server.port
    resource = (server, port)
  }

  override protected def afterAll(): Unit = {
    sandboxServer.close()

    super.afterAll()
  }
}

trait ToxiSandboxFixture extends BeforeAndAfterAll with ToxiproxyFixture with SandboxFixture {
  self: Suite =>

  protected def toxiSandboxPort = resource._1
  protected def toxiSandboxProxy = resource._2

  private var resource: (Port, Proxy) = null

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val host = InetAddress.getLoopbackAddress
    val lock = LockedFreePort.find()
    val port = lock.port
    val proxy = toxiproxyClient.createProxy(
      "sandbox",
      s"${host.getHostName}:${port}",
      s"${host.getHostName}:${sandboxPort}"
    )
    lock.unlock()
    resource = (port, proxy)
  }

  override protected def afterAll(): Unit = {
    toxiSandboxProxy.delete()

    super.afterAll()
  }
}

trait AbstractTriggerDaoFixture extends SuiteMixin {
  self: Suite =>

  protected def jdbcConfig: Option[JdbcConfig]
}

trait TriggerDaoInMemFixture extends AbstractTriggerDaoFixture {
  self: Suite =>

  override def jdbcConfig: Option[JdbcConfig] = None
}

trait TriggerDaoPostgresFixture
  extends AbstractTriggerDaoFixture
  with BeforeAndAfterEach
  with AkkaBeforeAndAfterAll
  with PostgresAroundAll {
  self: Suite =>

  override def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)

  // Lazy because the postgresDatabase is only available once the tests start
  private lazy val jdbcConfig_ = JdbcConfig(postgresDatabase.url, "operator", "password")
  private lazy val triggerDao =
    DbTriggerDao(jdbcConfig_, poolSize = dao.Connection.PoolSize.IntegrationTest)
  private lazy implicit val executionContext: ExecutionContext = system.getDispatcher

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    triggerDao.initialize fold (fail(_), identity)
  }

  override protected def afterEach(): Unit = {
    triggerDao.destroy() fold (fail(_), identity)

    super.afterEach()
  }

  override protected def afterAll(): Unit = {
    triggerDao.destroyPermanently() fold (fail(_), identity)

    super.afterAll()
  }
}

trait TriggerServiceFixture
  extends SuiteMixin
  with ToxiSandboxFixture
  with AbstractTriggerDaoFixture
  with StrictLogging {
  self: Suite =>

  // Use a small initial interval so we can test restart behaviour more easily.
  private val minRestartInterval = FiniteDuration(1, duration.SECONDS)

  def withTriggerService[A](encodedDar: Option[Dar[(PackageId, DamlLf.ArchivePayload)]])(testFn: Uri => Future[A])(
      implicit ec: ExecutionContext,
      pos: source.Position,
  ): Future[A] = {
    logger.info(s"${pos.fileName}:${pos.lineNumber}: setting up trigger service")

    val host = InetAddress.getLoopbackAddress
    val ledgerConfig = LedgerConfig(
      host.getHostName,
      toxiSandboxPort.value,
      TimeProviderType.Static,
      Duration.ofSeconds(30),
      ServiceConfig.DefaultMaxInboundMessageSize,
    )
    val restartConfig = TriggerRestartConfig(
      minRestartInterval,
      ServiceConfig.DefaultMaxRestartInterval,
    )
    val servicePort = LockedFreePort.find()
    (for {
      (binding, system) <- ServiceMain.startServer(
        host.getHostName,
        servicePort.port.value,
        authConfig,
        ledgerConfig,
        restartConfig,
        encodedDar,
        jdbcConfig,
      )
      uri = Uri.from(scheme = "http", host = "localhost", port = binding.localAddress.getPort)
      result <- testFn(uri).transform(Success(_))
      _ <- {
        system ! Stop
        system.whenTerminated
      }
    } yield result).flatMap(Future.fromTry(_))
  }
}
