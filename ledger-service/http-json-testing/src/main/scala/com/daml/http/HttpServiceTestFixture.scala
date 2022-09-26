// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.api.util.TimestampConversion
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.http.HttpService.doLoad
import com.daml.http.dbbackend.{ContractDao, JdbcConfig}
import com.daml.http.json.{DomainJsonDecoder, DomainJsonEncoder}
import com.daml.http.util.ClientUtil.boxedRecord
import com.daml.http.util.Logging.{InstanceUUID, instanceUUIDLogCtx}
import com.daml.http.util.TestUtil.getResponseDataBytes
import com.daml.http.util.{FutureUtil, NewBoolean}
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.daml.ledger.api.auth.{
  AuthService,
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  CustomDamlJWTPayload,
}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.v1.{value => v}
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.ledger.resources.ResourceContext
import com.daml.ledger.runner.common
import com.daml.ledger.sandbox.SandboxOnXForTest._
import com.daml.ledger.sandbox.{BridgeConfig, SandboxOnXRunner}
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{Assertions, Inside}
import scalaz._
import scalaz.std.option._
import scalaz.std.scalaFuture._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import spray.json._

import java.io.File
import java.nio.file.Files
import java.time.Instant
import scala.concurrent.duration.{DAYS, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

object HttpServiceTestFixture extends LazyLogging with Assertions with Inside {

  import json.JsonProtocol._

  private val doNotReloadPackages = FiniteDuration(100, DAYS)

  def withHttpService[A](
      testName: String,
      ledgerPort: Port,
      jdbcConfig: Option[JdbcConfig],
      staticContentConfig: Option[StaticContentConfig],
      leakPasswords: LeakPasswords = LeakPasswords.FiresheepStyle,
      maxInboundMessageSize: Int = StartSettings.DefaultMaxInboundMessageSize,
      useTls: UseTls = UseTls.NoTls,
      wsConfig: Option[WebsocketConfig] = None,
      nonRepudiation: nonrepudiation.Configuration.Cli = nonrepudiation.Configuration.Cli.Empty,
      ledgerIdOverwrite: Option[LedgerId] = None,
      token: Option[Jwt] = None,
  )(testFn: (Uri, DomainJsonEncoder, DomainJsonDecoder, DamlLedgerClient) => Future[A])(implicit
      asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext,
  ): Future[A] = {
    implicit val lc: LoggingContextOf[InstanceUUID] = instanceUUIDLogCtx()
    implicit val metrics: Metrics = new Metrics(new MetricRegistry())
    val ledgerId = ledgerIdOverwrite.getOrElse(LedgerId(testName))
    val applicationId = ApplicationId(testName)

    val contractDaoF: Future[Option[ContractDao]] = jdbcConfig.map(c => initializeDb(c)).sequence

    val httpServiceF: Future[(ServerBinding, Option[ContractDao])] = for {
      contractDao <- contractDaoF
      config = Config(
        ledgerHost = "localhost",
        ledgerPort = ledgerPort.value,
        address = "localhost",
        httpPort = 0,
        portFile = None,
        tlsConfig = if (useTls) clientTlsConfig else noTlsConfig,
        wsConfig = wsConfig,
        maxInboundMessageSize = maxInboundMessageSize,
        allowNonHttps = leakPasswords,
        staticContentConfig = staticContentConfig,
        packageReloadInterval = doNotReloadPackages,
        nonRepudiation = nonRepudiation,
      )
      httpService <- stripLeft(
        HttpService.start(
          startSettings = config,
          contractDao = contractDao,
        )
      )
    } yield httpService

    val client = DamlLedgerClient.singleHost(
      "localhost",
      ledgerPort.value,
      clientConfig(applicationId, token.map(_.value)),
      clientChannelConfig(useTls),
    )

    val codecsF: Future[(DomainJsonEncoder, DomainJsonDecoder)] = for {
      codecs <- jsonCodecs(client, ledgerId, token)
    } yield codecs

    val fa: Future[A] = for {
      (httpService, _) <- httpServiceF
      address = httpService.localAddress
      uri = Uri.from(scheme = "http", host = address.getHostName, port = address.getPort)
      (encoder, decoder) <- codecsF
      a <- testFn(uri, encoder, decoder, client)
    } yield a

    fa.transformWith { ta =>
      httpServiceF
        .flatMap { case (serv, dao) =>
          logger.info("Shutting down http service")
          dao.foreach(_.close())
          serv.unbind()
        }
        .fallbackTo(Future.unit)
        .transform(_ => ta)
    }
  }

  def withLedger[A](
      dars: List[File],
      testName: String,
      token: Option[String] = None,
      useTls: UseTls = UseTls.NoTls,
      authService: Option[AuthService] = None,
  )(testFn: (Port, DamlLedgerClient, LedgerId) => Future[A])(implicit
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext,
  ): Future[A] = {

    val ledgerId = LedgerId(testName)
    val applicationId = ApplicationId(testName)
    implicit val resourceContext: ResourceContext = ResourceContext(ec)

    val ledgerF = for {
      urlResource <- Future(
        SandboxBackend.H2Database.owner
          .map(info => info.jdbcUrl)
          .acquire()
      )
      jdbcUrl <- urlResource.asFuture

      config = ledgerConfig(
        ledgerPort = Port.Dynamic,
        ledgerId = ledgerId,
        useTls = useTls,
        jdbcUrl = jdbcUrl,
      )
      portF <- Future(
        SandboxOnXRunner.owner(ConfigAdaptor(authService), config, bridgeConfig).acquire()
      )
      port <- portF.asFuture
    } yield (portF, port)

    val clientF: Future[DamlLedgerClient] = for {
      (_, ledgerPort) <- ledgerF
    } yield DamlLedgerClient.singleHost(
      "localhost",
      ledgerPort.value,
      clientConfig(applicationId, token),
      clientChannelConfig(useTls),
    )

    val fa: Future[A] = for {
      (_, ledgerPort) <- ledgerF
      client <- clientF
      _ <- Future.sequence(dars.map { dar =>
        client.packageManagementClient.uploadDarFile(
          ByteString.copyFrom(Files.readAllBytes(dar.toPath))
        )
      })
      a <- testFn(ledgerPort, client, ledgerId)
    } yield a

    fa.transformWith { ta =>
      ledgerF
        .flatMap(_._1.release())
        .fallbackTo(Future.unit)
        .transform(_ => ta)
    }
  }

  def bridgeConfig: BridgeConfig = BridgeConfig()

  private def ledgerConfig(
      ledgerPort: Port,
      ledgerId: LedgerId,
      useTls: UseTls,
      jdbcUrl: String,
  ): common.Config = Default.copy(
    ledgerId = ledgerId.unwrap,
    engine = DevEngineConfig,
    dataSource = dataSource(jdbcUrl),
    participants = singleParticipant(
      ApiServerConfig.copy(
        seeding = Seeding.Weak,
        timeProviderType = TimeProviderType.WallClock,
        tls = if (useTls) Some(serverTlsConfig) else None,
        port = ledgerPort,
      )
    ),
  )

  private def clientConfig(
      applicationId: ApplicationId,
      token: Option[String],
  ): LedgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      token = token,
    )

  private def clientChannelConfig(useTls: UseTls): LedgerClientChannelConfiguration =
    if (useTls) {
      LedgerClientChannelConfiguration(clientTlsConfig.client())
    } else {
      LedgerClientChannelConfiguration.InsecureDefaults
    }

  def jsonCodecs(
      client: DamlLedgerClient,
      ledgerId: LedgerId,
      token: Option[Jwt],
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[(DomainJsonEncoder, DomainJsonDecoder)] = {
    val packageService = new PackageService(doLoad(client.packageClient))
    packageService
      .reload(
        token.getOrElse(Jwt("we use a dummy because there is no token in these tests.")),
        ledgerId,
      )
      .flatMap(x => FutureUtil.toFuture(x))
      .map(_ => HttpService.buildJsonCodecs(packageService))
  }

  private def stripLeft(
      fa: Future[HttpService.Error \/ (ServerBinding, Option[ContractDao])]
  )(implicit ec: ExecutionContext): Future[(ServerBinding, Option[ContractDao])] =
    fa.flatMap {
      case -\/(e) =>
        Future.failed(new IllegalStateException(s"Cannot start HTTP Service: ${e.message}"))
      case \/-(a) =>
        Future.successful(a)
    }

  private def initializeDb(c: JdbcConfig)(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      metrics: Metrics,
  ): Future[ContractDao] =
    for {
      dao <- Future(ContractDao(c))
      isSuccess <- DbStartupOps
        .fromStartupMode(dao, c.startMode)
        .unsafeToFuture()
      _ = if (!isSuccess) throw new Exception("Db startup failed")
    } yield dao

  object UseTls extends NewBoolean.Named {
    val Tls: UseTls = True
    val NoTls: UseTls = False
  }
  type UseTls = UseTls.T

  object LeakPasswords extends NewBoolean.Named {
    val FiresheepStyle: LeakPasswords = True
    val No: LeakPasswords = False
  }
  type LeakPasswords = LeakPasswords.T

  private val List(serverCrt, serverPem, caCrt, clientCrt, clientPem) = {
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").map { src =>
      Some(new File(rlocation("ledger/test-common/test-certificates/" + src)))
    }
  }

  final val serverTlsConfig = TlsConfiguration(enabled = true, serverCrt, serverPem, caCrt)
  final val clientTlsConfig = TlsConfiguration(enabled = true, clientCrt, clientPem, caCrt)
  private val noTlsConfig = TlsConfiguration(enabled = false, None, None, None)

  def jwtForParties(
      actAs: List[String],
      readAs: List[String],
      ledgerId: Option[String] = None,
      withoutNamespace: Boolean = false,
      admin: Boolean = false,
  ): Jwt = {
    import AuthServiceJWTCodec.JsonImplicits._
    val payload: JsValue = {
      val customJwtPayload: AuthServiceJWTPayload =
        CustomDamlJWTPayload(
          ledgerId = ledgerId,
          applicationId = Some("test"),
          actAs = actAs,
          participantId = None,
          exp = None,
          admin = admin,
          readAs = readAs,
        )
      val payloadJson = customJwtPayload.toJson
      if (withoutNamespace) {
        // unsafe code but if someone changes the underlying structure
        // they will notice the failing tests.
        val payloadObj = payloadJson.asInstanceOf[JsObject]
        val innerFieldsObj =
          payloadObj.fields(AuthServiceJWTCodec.oidcNamespace).asInstanceOf[JsObject]
        new JsObject(
          payloadObj.fields ++ innerFieldsObj.fields - AuthServiceJWTCodec.oidcNamespace
        )
      } else payloadJson
    }
    JwtSigner.HMAC256
      .sign(
        DecodedJwt(
          """{"alg": "HS256", "typ": "JWT"}""",
          payload.prettyPrint,
        ),
        "secret",
      )
      .fold(e => throw new IllegalArgumentException(s"cannot sign a JWT: ${e.shows}"), identity)
  }

  def headersWithPartyAuth(
      actAs: List[String],
      readAs: List[String],
      ledgerId: Option[String],
      withoutNamespace: Boolean = false,
  ): List[Authorization] = {
    authorizationHeader(jwtForParties(actAs, readAs, ledgerId, withoutNamespace))
  }

  def authorizationHeader(token: Jwt): List[Authorization] =
    List(Authorization(OAuth2BearerToken(token.value)))

  def postRequest(uri: Uri, json: JsValue, headers: List[HttpHeader] = Nil)(implicit
      as: ActorSystem,
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[(StatusCode, JsValue)] = {
    Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri,
          headers = headers,
          entity = HttpEntity(ContentTypes.`application/json`, json.prettyPrint),
        )
      )
      .flatMap { resp =>
        val bodyF: Future[String] = getResponseDataBytes(resp, debug = true)
        bodyF.map(body => {
          (resp.status, body.parseJson)
        })
      }
  }

  def postJsonStringRequestEncoded(uri: Uri, jsonString: String, headers: List[HttpHeader])(implicit
      as: ActorSystem,
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[(StatusCode, String)] = {
    logger.info(s"postJson: ${uri.toString} json: ${jsonString: String}")
    Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri,
          headers = headers,
          entity = HttpEntity(ContentTypes.`application/json`, jsonString),
        )
      )
      .flatMap { resp =>
        val bodyF: Future[String] = getResponseDataBytes(resp, debug = true)
        bodyF.map(body => (resp.status, body))
      }
  }

  def postJsonStringRequest(uri: Uri, jsonString: String, headers: List[HttpHeader])(implicit
      as: ActorSystem,
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[(StatusCode, JsValue)] =
    postJsonStringRequestEncoded(uri, jsonString, headers).map { case (status, body) =>
      (status, body.parseJson)
    }

  def postJsonRequest(uri: Uri, json: JsValue, headers: List[HttpHeader])(implicit
      as: ActorSystem,
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[(StatusCode, JsValue)] =
    postJsonStringRequest(uri, json.prettyPrint, headers)

  def postCreateCommand(
      cmd: domain.CreateCommand[v.Record, domain.ContractTypeId.OptionalPkg],
      encoder: DomainJsonEncoder,
      uri: Uri,
      headers: List[HttpHeader],
  )(implicit
      as: ActorSystem,
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[(StatusCode, JsValue)] = {
    for {
      json <- FutureUtil.toFuture(encoder.encodeCreateCommand(cmd)): Future[JsValue]
      result <- postJsonRequest(uri.withPath(Uri.Path("/v1/create")), json, headers = headers)
    } yield result
  }

  def postArchiveCommand(
      templateId: domain.ContractTypeId.OptionalPkg,
      contractId: domain.ContractId,
      encoder: DomainJsonEncoder,
      uri: Uri,
      headers: List[HttpHeader],
  )(implicit
      as: ActorSystem,
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[(StatusCode, JsValue)] = {
    val ref = domain.EnrichedContractId(Some(templateId), contractId)
    val cmd = archiveCommand(ref)
    for {
      json <- FutureUtil.toFuture(encoder.encodeExerciseCommand(cmd)): Future[JsValue]
      result <- postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), json, headers)
    } yield result
  }

  def getRequestEncoded(uri: Uri, headers: List[HttpHeader] = List())(implicit
      as: ActorSystem,
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[(StatusCode, String)] = {
    Http()
      .singleRequest(
        HttpRequest(method = HttpMethods.GET, uri = uri, headers = headers)
      )
      .flatMap { resp =>
        val bodyF: Future[String] = getResponseDataBytes(resp, debug = true)
        bodyF.map(body => (resp.status, body))
      }
  }

  def getRequest(uri: Uri, headers: List[HttpHeader])(implicit
      as: ActorSystem,
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[(StatusCode, JsValue)] =
    getRequestEncoded(uri, headers).map { case (status, body) =>
      (status, body.parseJson)
    }

  def archiveCommand[Ref](reference: Ref): domain.ExerciseCommand[v.Value, Ref] = {
    val arg: v.Record = v.Record()
    val choice = lar.Choice("Archive")
    domain.ExerciseCommand(reference, choice, boxedRecord(arg), None, None)
  }

  def accountCreateCommand(
      owner: domain.Party,
      number: String,
      time: v.Value.Sum.Timestamp = TimestampConversion.roundInstantToMicros(Instant.now),
  ): domain.CreateCommand[v.Record, domain.ContractTypeId.OptionalPkg] = {
    val templateId = domain.ContractTypeId.Template(None, "Account", "Account")
    val timeValue = v.Value(time)
    val enabledVariantValue =
      v.Value(v.Value.Sum.Variant(v.Variant(None, "Enabled", Some(timeValue))))
    val arg = v.Record(
      fields = List(
        v.RecordField("owner", Some(v.Value(v.Value.Sum.Party(owner.unwrap)))),
        v.RecordField("number", Some(v.Value(v.Value.Sum.Text(number)))),
        v.RecordField("status", Some(enabledVariantValue)),
      )
    )

    domain.CreateCommand(templateId, arg, None)
  }

  def sharedAccountCreateCommand(
      owners: Seq[String],
      number: String,
      time: v.Value.Sum.Timestamp = TimestampConversion.roundInstantToMicros(Instant.now),
  ): domain.CreateCommand[v.Record, domain.ContractTypeId.OptionalPkg] = {
    val templateId = domain.ContractTypeId.Template(None, "Account", "SharedAccount")
    val timeValue = v.Value(time)
    val enabledVariantValue =
      v.Value(v.Value.Sum.Variant(v.Variant(None, "Enabled", Some(timeValue))))
    val arg = v.Record(
      fields = List(
        v.RecordField(
          "owners",
          Some(v.Value(v.Value.Sum.List(v.List(owners.map(o => v.Value(v.Value.Sum.Party(o))))))),
        ),
        v.RecordField("number", Some(v.Value(v.Value.Sum.Text(number)))),
        v.RecordField("status", Some(enabledVariantValue)),
      )
    )

    domain.CreateCommand(templateId, arg, None)
  }
}
