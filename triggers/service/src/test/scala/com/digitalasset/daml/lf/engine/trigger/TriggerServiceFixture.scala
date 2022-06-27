// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import com.auth0.jwt.JWT
import com.auth0.jwt.JWTVerifier.BaseVerification
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.{Clock => Auth0Clock}
import com.daml.auth.middleware.api.{Client => AuthClient}
import com.daml.auth.middleware.oauth2.{
  SecretString,
  Config => MiddlewareConfig,
  Server => MiddlewareServer,
}
import com.daml.auth.oauth2.test.server.{Config => OAuthConfig, Server => OAuthServer}
import com.daml.bazeltools.BazelRunfiles
import com.daml.clock.AdjustableClock
import com.daml.daml_lf_dev.DamlLf
import com.daml.dbutils.{ConnectionPool, JdbcConfig}
import com.daml.jwt.domain.DecodedJwt
import com.daml.jwt.{JwtSigner, JwtVerifier, JwtVerifierBase}
import com.daml.ledger.api.auth
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, CustomDamlJWTPayload, StandardJWTPayload}
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, OwnedResource}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest._
import com.daml.ledger.sandbox.{BridgeConfig, SandboxOnXRunner}
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref._
import com.daml.lf.engine.trigger.dao.DbTriggerDao
import com.daml.lf.speedy.Compiler
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.{LockedFreePort, Port}
import com.daml.scalautil.Statement.discard
import com.daml.testing.oracle.OracleAroundAll
import com.daml.testing.postgresql.PostgresAroundAll
import com.daml.timer.RetryStrategy
import com.typesafe.scalalogging.StrictLogging
import eu.rekawek.toxiproxy._
import io.grpc.Channel
import org.scalactic.source
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite, SuiteMixin}
import scalaz.syntax.show._

import java.net.InetAddress
import java.time.{Clock, Instant, LocalDateTime, ZoneId, Duration => JDuration}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.{Date, UUID}
import scala.collection.concurrent.TrieMap
import scala.concurrent._
import scala.concurrent.duration._
import scala.sys.process.Process
import scala.util.Success

/** A test-fixture that persists cookies between http requests for each test-case.
  */
trait HttpCookies extends BeforeAndAfterEach { this: Suite =>
  private val cookieJar = TrieMap[String, String]()

  override protected def afterEach(): Unit = {
    try super.afterEach()
    finally cookieJar.clear()
  }

  /** Adds a Cookie header for the currently stored cookies and performs the given http request.
    */
  def httpRequest(
      request: HttpRequest
  )(implicit system: ActorSystem, ec: ExecutionContext): Future[HttpResponse] = {
    Http()
      .singleRequest {
        if (cookieJar.nonEmpty) {
          val hd +: tl = cookieJar.view.map(headers.HttpCookiePair(_)).toSeq
          val cookies = headers.Cookie(hd, tl: _*)
          request.addHeader(cookies)
        } else {
          request
        }
      }
      .andThen { case Success(resp) =>
        resp.headers.foreach {
          case headers.`Set-Cookie`(cookie) =>
            cookieJar.update(cookie.name, cookie.value)
          case _ =>
        }
      }
  }

  /** Same as [[httpRequest]] but will follow redirections.
    */
  def httpRequestFollow(request: HttpRequest, maxRedirections: Int = 10)(implicit
      system: ActorSystem,
      ec: ExecutionContext,
  ): Future[HttpResponse] = {
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

  /** Remove all stored cookies.
    */
  def deleteCookies(): Unit = {
    cookieJar.clear()
  }
}

trait AbstractAuthFixture extends SuiteMixin {
  self: Suite =>

  protected def authService: Option[auth.AuthService]
  protected[this] def authToken(
      admin: Boolean,
      actAs: List[ApiTypes.Party],
      readAs: List[ApiTypes.Party],
  ): Option[String]
  protected def authConfig: AuthConfig
}

trait NoAuthFixture extends AbstractAuthFixture {
  self: Suite =>

  protected override def authService: Option[auth.AuthService] = None
  protected[this] override final def authToken(
      admin: Boolean,
      actAs: List[ApiTypes.Party],
      readAs: List[ApiTypes.Party],
  ) = None
  protected override def authConfig: AuthConfig = NoAuth
}

trait AuthMiddlewareFixture
    extends AbstractAuthFixture
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with AkkaBeforeAndAfterAll {
  self: Suite =>

  protected def authService: Option[auth.AuthService] = Some(auth.AuthServiceJWT(authVerifier))

  protected[this] override final def authToken(
      admin: Boolean,
      actAs: List[ApiTypes.Party],
      readAs: List[ApiTypes.Party],
  ) = Some {
    val payload =
      if (sandboxClientTakesUserToken)
        StandardJWTPayload(userId = "", participantId = None, exp = None)
      else
        CustomDamlJWTPayload(
          ledgerId = None,
          applicationId = None,
          participantId = None,
          exp = None,
          admin = admin,
          actAs = ApiTypes.Party unsubst actAs,
          readAs = ApiTypes.Party unsubst readAs,
        )

    val header = """{"alg": "HS256", "typ": "JWT"}"""
    val jwt = JwtSigner.HMAC256
      .sign(DecodedJwt(header, AuthServiceJWTCodec.compactPrint(payload)), authSecret)
      .fold(e => fail(e.shows), identity)
    jwt.value
  }
  protected def authConfig: AuthConfig = AuthMiddleware(authMiddlewareUri, authMiddlewareUri)
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
  protected[this] def oauth2YieldsUserTokens: Boolean = true
  protected[this] def sandboxClientTakesUserToken: Boolean = true

  private val authSecret: String = "secret"
  private var resource
      : OwnedResource[ResourceContext, (AdjustableClock, OAuthServer, ServerBinding)] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    implicit val context: ResourceContext = ResourceContext(system.dispatcher)
    def closeServerBinding(binding: ServerBinding)(implicit ec: ExecutionContext): Future[Unit] =
      for {
        _ <- binding.unbind()
      } yield ()
    val ledgerId = this.getClass.getSimpleName
    resource = new OwnedResource(new ResourceOwner[(AdjustableClock, OAuthServer, ServerBinding)] {
      override def acquire()(implicit
          context: ResourceContext
      ): Resource[(AdjustableClock, OAuthServer, ServerBinding)] = {
        for {
          clock <- Resource(
            Future(
              AdjustableClock(Clock.fixed(Instant.now(), ZoneId.systemDefault()), JDuration.ZERO)
            )
          )(_ => Future(()))
          oauthConfig = OAuthConfig(
            port = Port.Dynamic,
            ledgerId = ledgerId,
            jwtSecret = authSecret,
            clock = Some(clock),
            yieldUserTokens = oauth2YieldsUserTokens,
          )
          oauthServer = OAuthServer(oauthConfig)
          oauth <- Resource(oauthServer.start())(closeServerBinding)
          uri = Uri()
            .withScheme("http")
            .withAuthority(oauth.localAddress.getHostString, oauth.localAddress.getPort)
          middlewareConfig = MiddlewareConfig(
            address = "localhost",
            port = 0,
            portFile = None,
            callbackUri = None,
            maxLoginRequests = MiddlewareConfig.DefaultMaxLoginRequests,
            loginTimeout = MiddlewareConfig.DefaultLoginTimeout,
            cookieSecure = MiddlewareConfig.DefaultCookieSecure,
            oauthAuth = uri.withPath(Path./("authorize")),
            oauthToken = uri.withPath(Path./("token")),
            oauthAuthTemplate = None,
            oauthTokenTemplate = None,
            oauthRefreshTemplate = None,
            clientId = "oauth-middleware-id",
            clientSecret = SecretString("oauth-middleware-secret"),
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

  private def sandboxConfig(jdbcUrl: String): Config =
    Default.copy(
      ledgerId = this.getClass.getSimpleName,
      engine = DevEngineConfig,
      dataSource = dataSource(jdbcUrl),
      participants = singleParticipant(
        ApiServerConfig.copy(
          seeding = Seeding.Weak,
          timeProviderType = TimeProviderType.Static,
        )
      ),
    )

  protected lazy val sandboxPort: Port = resource.value._1
  protected lazy val channel: Channel = resource.value._2
  protected def sandboxClient(
      applicationId: ApplicationId,
      admin: Boolean = false,
      actAs: List[ApiTypes.Party] = List(),
      readAs: List[ApiTypes.Party] = List(),
  )(implicit executionContext: ExecutionContext): Future[LedgerClient] =
    LedgerClient(
      channel,
      LedgerClientConfiguration(
        applicationId = ApplicationId.unwrap(applicationId),
        ledgerIdRequirement = LedgerIdRequirement.none,
        commandClient = CommandClientConfiguration.default,
        token = authToken(admin, actAs = actAs, readAs = readAs),
      ),
    )

  private var resource: OwnedResource[ResourceContext, (Port, Channel)] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    implicit val context: ResourceContext = ResourceContext(system.dispatcher)
    // The owner spins up its own actor system which avoids deadlocks
    // during shutdown.
    resource = new OwnedResource(
      for {
        // We must provide a random database if none is provided.
        // The default is to always use the same index database URL, which means that tests can
        // share an index. As you can imagine, this causes all manner of issues, the most important
        // of which is that the ledger and index databases will be out of sync.
        jdbcUrl <- SandboxBackend.H2Database.owner
          .map(info => info.jdbcUrl)

        port <- SandboxOnXRunner.owner(
          configAdaptor = ConfigAdaptor(authService),
          config = sandboxConfig(jdbcUrl = jdbcUrl),
          bridgeConfig = BridgeConfig(),
        )
        channel <- GrpcClientResource.owner(port)
      } yield (port, channel),
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
    resource.setup()
  }

  override protected def afterAll(): Unit = {
    resource.close()

    super.afterAll()
  }
}

trait ToxiproxyFixture extends BeforeAndAfterAll with AkkaBeforeAndAfterAll {
  self: Suite =>

  protected def toxiproxyClient: ToxiproxyClient = resource._1

  private var resource: (ToxiproxyClient, Process) = _
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
    Await.result(
      RetryStrategy.constant(attempts = 3, waitTime = 2.seconds) { (_, _) =>
        Future(port.testAndUnlock(host))
      },
      Duration.Inf,
    )
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

  protected def toxiSandboxPort: Port = resource._1

  protected def toxiSandboxProxy: Proxy = resource._2

  private var resource: (Port, Proxy) = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val host = InetAddress.getLoopbackAddress
    val lock = LockedFreePort.find()
    val port = lock.port
    val proxy = toxiproxyClient.createProxy(
      "sandbox",
      s"${host.getHostName}:$port",
      s"${host.getHostName}:$sandboxPort",
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
  private lazy val jdbcConfig_ =
    JdbcConfig(
      "org.postgresql.Driver",
      postgresDatabase.url,
      "operator",
      "password",
      ConnectionPool.PoolSize.Integration,
    )
  private lazy val triggerDao = DbTriggerDao(jdbcConfig_)
  private lazy implicit val executionContext: ExecutionContext = system.getDispatcher

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    Await.result(triggerDao.initialize(false), Duration(30, SECONDS))
  }

  override protected def afterEach(): Unit = {
    Await.result(triggerDao.destroy, Duration(30, SECONDS))
    super.afterEach()
  }

  override protected def afterAll(): Unit = {
    triggerDao.destroyPermanently().fold(fail(_), identity)
    super.afterAll()
  }
}

trait TriggerDaoOracleFixture
    extends AbstractTriggerDaoFixture
    with BeforeAndAfterEach
    with AkkaBeforeAndAfterAll
    with OracleAroundAll {
  self: Suite =>

  override def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)

  // Lazy because the oracleDatabase is only available once the tests start
  private lazy val jdbcConfig_ =
    JdbcConfig(
      "oracle.jdbc.OracleDriver",
      oracleJdbcUrlWithoutCredentials,
      oracleUserName,
      oracleUserPwd,
      ConnectionPool.PoolSize.Production,
    )
  // TODO For whatever reason we need a larger pool here, otherwise
  // the connection deadlocks. I have no idea why :(
  private lazy val triggerDao =
    DbTriggerDao(jdbcConfig_)
  private lazy implicit val executionContext: ExecutionContext = system.getDispatcher

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    Await.result(triggerDao.initialize(false), Duration(31, SECONDS))
  }

  override protected def afterEach(): Unit = {
    Await.result(triggerDao.destroy, Duration(30, SECONDS))
    super.afterEach()
  }

  override protected def afterAll(): Unit = {
    triggerDao.destroyPermanently().fold(fail(_), identity)
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
  private def triggerServiceOwner(
      encodedDars: List[Dar[(PackageId, DamlLf.ArchivePayload)]],
      authCallback: Option[Uri],
  ) =
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
              Cli.DefaultMaxInboundMessageSize,
            )
            val restartConfig = TriggerRestartConfig(
              minRestartInterval,
              Cli.DefaultMaxRestartInterval,
            )
            for {
              r <- ServiceMain.startServer(
                host.getHostName,
                Port.Dynamic.value,
                Cli.DefaultMaxAuthCallbacks,
                Cli.DefaultAuthCallbackTimeout,
                Cli.DefaultMaxHttpEntityUploadSize,
                Cli.DefaultHttpEntityUploadTimeout,
                authConfig,
                AuthClient.RedirectToLogin.Yes,
                authCallback,
                ledgerConfig,
                restartConfig,
                encodedDars,
                jdbcConfig,
                false,
                Compiler.Config.Dev,
                logTriggerStatus,
              )
            } yield r
          } { case (_, system) =>
            system ! Server.Stop
            system.whenTerminated.map(_ => ())
          }
        } yield binding
    }

  def withTriggerService[A](
      encodedDars: List[Dar[(PackageId, DamlLf.ArchivePayload)]],
      authCallback: Option[Uri] = None,
  )(testFn: Uri => Future[A])(implicit
      ec: ExecutionContext,
      pos: source.Position,
  ): Future[A] = {
    logger.info(s"${pos.fileName}:${pos.lineNumber}: setting up trigger service")
    implicit val context: ResourceContext = ResourceContext(ec)
    triggerServiceOwner(encodedDars, authCallback).use { binding =>
      val uri = Uri.from(scheme = "http", host = "localhost", port = binding.localAddress.getPort)
      testFn(uri)
    }
  }
}
