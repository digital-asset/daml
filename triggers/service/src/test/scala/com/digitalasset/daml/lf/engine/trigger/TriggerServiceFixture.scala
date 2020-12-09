// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.io.File
import java.net.InetAddress
import java.time.{Clock, Duration => JDuration, Instant, LocalDateTime, ZoneId}
import java.util.{Date, UUID}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import io.grpc.Channel
import com.daml.ledger.api.testing.utils.OwnedResource
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.resources.FutureResourceOwner
import com.daml.scalautil.Statement.discard
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, Uri, headers}
import akka.http.scaladsl.model.Uri.Path
import com.auth0.jwt.JWT
import com.auth0.jwt.JWTVerifier.BaseVerification
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.{Clock => Auth0Clock}
import com.daml.bazeltools.BazelRunfiles
import com.daml.clock.AdjustableClock
import com.daml.daml_lf_dev.DamlLf
import com.daml.jwt.domain.DecodedJwt
import com.daml.jwt.{JwtSigner, JwtVerifier, JwtVerifierBase}
import com.daml.ledger.api.auth
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref._
import com.daml.auth.middleware.oauth2.{Config => MiddlewareConfig, Server => MiddlewareServer}
import com.daml.auth.oauth2.test.server.{Config => OAuthConfig, Server => OAuthServer}
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

  /**
    * Remove all stored cookies.
    */
  def deleteCookies(): Unit = {
    cookieJar.clear()
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
    with BeforeAndAfterEach
    with AkkaBeforeAndAfterAll {
  self: Suite =>

  protected def authService: Option[auth.AuthService] = Some(auth.AuthServiceJWT(authVerifier))
  protected def authToken(payload: AuthServiceJWTPayload): Option[String] = Some {
    val header = """{"alg": "HS256", "typ": "JWT"}"""
    val jwt = JwtSigner.HMAC256
      .sign(DecodedJwt(header, AuthServiceJWTCodec.compactPrint(payload)), authSecret)
      .toOption
      .get
    jwt.value
  }
  protected def authConfig: AuthConfig = AuthMiddleware(authMiddlewareUri)
  protected def authClock: AdjustableClock = resource.value._1
  protected def authServer: OAuthServer = resource.value._2

  private def authVerifier: JwtVerifierBase = new JwtVerifier(
    JWT
      .require(Algorithm.HMAC256(authSecret))
      .asInstanceOf[BaseVerification]
      .build(new Auth0Clock {
        override def getToday: Date = Date.from(authClock.instant())
      })
  )
  private def authMiddleware: ServerBinding = resource.value._3
  private def authMiddlewareUri: Uri =
    Uri()
      .withScheme("http")
      .withAuthority(authMiddleware.localAddress.getHostString, authMiddleware.localAddress.getPort)

  private val authSecret: String = "secret"
  private var resource
    : OwnedResource[ResourceContext, (AdjustableClock, OAuthServer, ServerBinding)] = null

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    implicit val context: ResourceContext = ResourceContext(system.dispatcher)
    def closeServerBinding(binding: ServerBinding)(implicit ec: ExecutionContext): Future[Unit] =
      for {
        _ <- binding.unbind()
      } yield ()
    val ledgerId = this.getClass.getSimpleName
    resource = new OwnedResource(new ResourceOwner[(AdjustableClock, OAuthServer, ServerBinding)] {
      override def acquire()(implicit context: ResourceContext)
        : Resource[(AdjustableClock, OAuthServer, ServerBinding)] = {
        for {
          clock <- Resource(
            Future(
              AdjustableClock(Clock.fixed(Instant.now(), ZoneId.systemDefault()), JDuration.ZERO)))(
            _ => Future(()))
          oauthConfig = OAuthConfig(
            port = Port.Dynamic,
            ledgerId = ledgerId,
            jwtSecret = authSecret,
            clock = Some(clock),
          )
          oauthServer = OAuthServer(oauthConfig)
          oauth <- Resource(oauthServer.start())(closeServerBinding)
          uri = Uri()
            .withScheme("http")
            .withAuthority(oauth.localAddress.getHostString, oauth.localAddress.getPort)
          middlewareConfig = MiddlewareConfig(
            port = Port.Dynamic,
            oauthAuth = uri.withPath(Path./("authorize")),
            oauthToken = uri.withPath(Path./("token")),
            clientId = "oauth-middleware-id",
            clientSecret = "oauth-middleware-secret",
            tokenVerifier = authVerifier,
          )
          middleware <- Resource(MiddlewareServer.start(middlewareConfig))(closeServerBinding)
        } yield (clock, oauthServer, middleware)
      }
    })
    resource.setup()
  }

  override protected def afterAll(): Unit = {
    resource.close()

    super.afterAll()
  }

  override protected def afterEach(): Unit = {
    authServer.resetAuthorizedParties()
    authServer.resetAdmin()

    super.afterEach()
  }
}

trait SandboxFixture extends BeforeAndAfterAll with AbstractAuthFixture with AkkaBeforeAndAfterAll {
  self: Suite =>

  protected val damlPackages: List[File] = List()
  protected val ledgerIdMode: LedgerIdMode =
    LedgerIdMode.Static(LedgerId(this.getClass.getSimpleName))
  private def sandboxConfig: SandboxConfig = sandbox.DefaultConfig.copy(
    port = Port.Dynamic,
    damlPackages = damlPackages,
    ledgerIdMode = ledgerIdMode,
    timeProviderType = Some(TimeProviderType.Static),
    authService = authService,
    ledgerConfig = LedgerConfiguration.defaultLedgerBackedIndex,
    seeding = None,
  )

  protected lazy val sandboxServer: SandboxServer = resource.value._1
  protected lazy val sandboxPort: Port = sandboxServer.port
  protected def sandboxClient(
      applicationId: ApplicationId,
      admin: Boolean = false,
      actAs: List[ApiTypes.Party] = List(),
      readAs: List[ApiTypes.Party] = List())(
      implicit executionContext: ExecutionContext): Future[LedgerClient] =
    LedgerClient(
      resource.value._2,
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
            readAs = readAs.map(ApiTypes.Party.unwrap)
          ))
      )
    )

  private var resource: OwnedResource[ResourceContext, (SandboxServer, Channel)] = null

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    implicit val context: ResourceContext = ResourceContext(system.dispatcher)
    // The owner spins up its own actor system which avoids deadlocks
    // during shutdown.
    resource = new OwnedResource(for {
      sandbox <- SandboxServer.owner(sandboxConfig)
      port <- new FutureResourceOwner[ResourceContext, Port](() =>
        sandbox.portF(context.executionContext))
      channel <- GrpcClientResource.owner(port)
    } yield (sandbox, channel))
    resource.setup()
  }

  override protected def afterAll(): Unit = {
    resource.close()

    super.afterAll()
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
    }, Duration.Inf)
    val client = new ToxiproxyClient(host.getHostName, port.port.value)
    resource = (client, proc)
  }

  override protected def afterAll(): Unit = {
    resource._2.destroy()
    val _ = resource._2.exitValue()

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

    Await.result(triggerDao.initialize, Duration(30, SECONDS))
  }

  override protected def afterEach(): Unit = {
    Await.result(triggerDao.destroy, Duration(30, SECONDS))

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

  private val triggerLog: ConcurrentMap[UUID, Vector[(LocalDateTime, String)]] =
    new ConcurrentHashMap

  def getTriggerStatus(uuid: UUID): Vector[(LocalDateTime, String)] =
    triggerLog.getOrDefault(uuid, Vector.empty)

  private def logTriggerStatus(triggerInstance: UUID, msg: String): Unit = {
    val entry = (LocalDateTime.now, msg)
    discard(triggerLog.merge(triggerInstance, Vector(entry), _ ++ _))
  }

  // Use a small initial interval so we can test restart behaviour more easily.
  private val minRestartInterval = FiniteDuration(1, duration.SECONDS)
  private def triggerServiceOwner(encodedDars: List[Dar[(PackageId, DamlLf.ArchivePayload)]]) =
    new ResourceOwner[ServerBinding] {
      override def acquire()(implicit context: ResourceContext): Resource[ServerBinding] =
        for {
          (binding, _) <- Resource {
            val host = InetAddress.getLoopbackAddress
            val ledgerConfig = LedgerConfig(
              host.getHostName,
              toxiSandboxPort.value,
              TimeProviderType.Static,
              java.time.Duration.ofSeconds(30),
              ServiceConfig.DefaultMaxInboundMessageSize,
            )
            val restartConfig = TriggerRestartConfig(
              minRestartInterval,
              ServiceConfig.DefaultMaxRestartInterval,
            )
            val lock = LockedFreePort.find()
            for {
              r <- ServiceMain.startServer(
                host.getHostName,
                lock.port.value,
                ServiceConfig.DefaultMaxHttpEntityUploadSize,
                ServiceConfig.DefaultHttpEntityUploadTimeout,
                authConfig,
                ledgerConfig,
                restartConfig,
                encodedDars,
                jdbcConfig,
                logTriggerStatus
              )
              _ = lock.unlock()
            } yield r
          } {
            case (_, system) => {
              system ! Server.Stop
              system.whenTerminated.map(_ => ())
            }
          }
        } yield binding
    }

  def withTriggerService[A](encodedDars: List[Dar[(PackageId, DamlLf.ArchivePayload)]])(
      testFn: Uri => Future[A])(
      implicit ec: ExecutionContext,
      pos: source.Position,
  ): Future[A] = {
    logger.info(s"${pos.fileName}:${pos.lineNumber}: setting up trigger service")
    implicit val context: ResourceContext = ResourceContext(ec)
    triggerServiceOwner(encodedDars).use { binding =>
      val uri = Uri.from(scheme = "http", host = "localhost", port = binding.localAddress.getPort)
      testFn(uri)
    }
  }
}
