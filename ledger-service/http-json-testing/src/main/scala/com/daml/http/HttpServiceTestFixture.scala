// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File
import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import com.daml.api.util.TimestampConversion
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.http.dbbackend.ContractDao
import com.daml.http.json.{DomainJsonDecoder, DomainJsonEncoder, SprayJson}
import com.daml.http.util.ClientUtil.boxedRecord
import com.daml.http.util.TestUtil.getResponseDataBytes
import com.daml.http.util.{FutureUtil, NewBoolean}
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.daml.ledger.api.auth.{AuthService, AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.v1.{value => v}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox
import com.daml.platform.sandbox.SandboxServer
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{Assertions, Inside}
import scalaz._
import scalaz.std.option._
import scalaz.std.scalaFuture._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import spray.json._

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
      maxInboundMessageSize: Int = HttpService.DefaultMaxInboundMessageSize,
      useTls: UseTls = UseTls.NoTls,
      wsConfig: Option[WebsocketConfig] = None,
  )(testFn: (Uri, DomainJsonEncoder, DomainJsonDecoder, LedgerClient) => Future[A])(implicit
      asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext,
  ): Future[A] = {

    val applicationId = ApplicationId(testName)

    val contractDaoF: Future[Option[ContractDao]] = jdbcConfig.map(c => initializeDb(c)).sequence

    val httpServiceF: Future[ServerBinding] = for {
      contractDao <- contractDaoF
      config = Config(
        ledgerHost = "localhost",
        ledgerPort = ledgerPort.value,
        address = "localhost",
        httpPort = 0,
        portFile = None,
        tlsConfig = if (useTls) clientTlsConfig else noTlsConfig,
        wsConfig = wsConfig,
        accessTokenFile = None,
        maxInboundMessageSize = maxInboundMessageSize,
        allowNonHttps = leakPasswords,
        staticContentConfig = staticContentConfig,
        packageReloadInterval = doNotReloadPackages,
      )
      httpService <- stripLeft(
        HttpService.start(
          startSettings = config,
          contractDao = contractDao,
        )
      )
    } yield httpService

    val clientF: Future[LedgerClient] = for {
      client <- LedgerClient.singleHost(
        "localhost",
        ledgerPort.value,
        clientConfig(applicationId, useTls = useTls),
      )
    } yield client

    val codecsF: Future[(DomainJsonEncoder, DomainJsonDecoder)] = for {
      client <- clientF
      codecs <- jsonCodecs(client)
    } yield codecs

    val fa: Future[A] = for {
      httpService <- httpServiceF
      address = httpService.localAddress
      uri = Uri.from(scheme = "http", host = address.getHostName, port = address.getPort)
      (encoder, decoder) <- codecsF
      client <- clientF
      a <- testFn(uri, encoder, decoder, client)
    } yield a

    fa.transformWith { ta =>
      Future
        .sequence(
          Seq(
            httpServiceF.flatMap(_.unbind())
          ) map (_ fallbackTo Future.unit)
        )
        .transform(_ => ta)
    }
  }

  def withLedger[A](
      dars: List[File],
      testName: String,
      token: Option[String] = None,
      useTls: UseTls = UseTls.NoTls,
      authService: Option[AuthService] = None,
  )(testFn: (Port, LedgerClient) => Future[A])(implicit
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext,
  ): Future[A] = {

    val ledgerId = LedgerId(testName)
    val applicationId = ApplicationId(testName)

    val ledgerF = for {
      ledger <- Future(
        new SandboxServer(ledgerConfig(Port.Dynamic, dars, ledgerId, authService, useTls), mat)
      )
      port <- ledger.portF
    } yield (ledger, port)

    val clientF: Future[LedgerClient] = for {
      (_, ledgerPort) <- ledgerF
      client <- LedgerClient.singleHost(
        "localhost",
        ledgerPort.value,
        clientConfig(applicationId, token, useTls),
      )
    } yield client

    val fa: Future[A] = for {
      (_, ledgerPort) <- ledgerF
      client <- clientF
      a <- testFn(ledgerPort, client)
    } yield a

    fa.onComplete { _ =>
      ledgerF.foreach(_._1.close())
    }

    fa
  }

  private def ledgerConfig(
      ledgerPort: Port,
      dars: List[File],
      ledgerId: LedgerId,
      authService: Option[AuthService],
      useTls: UseTls,
  ): SandboxConfig =
    sandbox.DefaultConfig.copy(
      port = ledgerPort,
      damlPackages = dars,
      timeProviderType = Some(TimeProviderType.WallClock),
      tlsConfig = if (useTls) Some(serverTlsConfig) else None,
      ledgerIdMode = LedgerIdMode.Static(ledgerId),
      authService = authService,
      seeding = Some(Seeding.Weak),
    )

  private def clientConfig[A](
      applicationId: ApplicationId,
      token: Option[String] = None,
      useTls: UseTls,
  ): LedgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = if (useTls) clientTlsConfig.client else None,
      token = token,
    )

  def jsonCodecs(
      client: LedgerClient
  )(implicit ec: ExecutionContext): Future[(DomainJsonEncoder, DomainJsonDecoder)] = {
    val packageService = new PackageService(
      HttpService.loadPackageStoreUpdates(client.packageClient, holderM = None)
    )
    packageService
      .reload(ec)
      .flatMap(x => FutureUtil.toFuture(x))
      .map(_ => HttpService.buildJsonCodecs(packageService))
  }

  private def stripLeft(
      fa: Future[HttpService.Error \/ ServerBinding]
  )(implicit ec: ExecutionContext): Future[ServerBinding] =
    fa.flatMap {
      case -\/(e) =>
        Future.failed(new IllegalStateException(s"Cannot start HTTP Service: ${e.message}"))
      case \/-(a) =>
        Future.successful(a)
    }

  private def initializeDb(c: JdbcConfig)(implicit ec: ExecutionContext): Future[ContractDao] =
    for {
      dao <- Future(ContractDao(c.driver, c.url, c.user, c.password))
      _ <- {
        import dao.{logHandler, jdbcDriver}
        dao.transact(ContractDao.initialize).unsafeToFuture(): Future[Unit]
      }
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

  private val serverTlsConfig = TlsConfiguration(enabled = true, serverCrt, serverPem, caCrt)
  private val clientTlsConfig = TlsConfiguration(enabled = true, clientCrt, clientPem, caCrt)
  private val noTlsConfig = TlsConfiguration(enabled = false, None, None, None)

  def jwtForParties(actAs: List[String], readAs: List[String], ledgerId: String) = {
    import AuthServiceJWTCodec.JsonImplicits._
    val decodedJwt = DecodedJwt(
      """{"alg": "HS256", "typ": "JWT"}""",
      AuthServiceJWTPayload(
        ledgerId = Some(ledgerId),
        applicationId = Some("test"),
        actAs = actAs,
        participantId = None,
        exp = None,
        admin = false,
        readAs = readAs,
      ).toJson.prettyPrint,
    )
    JwtSigner.HMAC256
      .sign(decodedJwt, "secret")
      .fold(e => throw new IllegalArgumentException(s"cannot sign a JWT: ${e.shows}"), identity)
  }

  def headersWithPartyAuth(actAs: List[String], readAs: List[String], ledgerId: String) =
    authorizationHeader(jwtForParties(actAs, readAs, ledgerId))

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
      cmd: domain.CreateCommand[v.Record],
      encoder: DomainJsonEncoder,
      uri: Uri,
      headers: List[HttpHeader],
  )(implicit
      as: ActorSystem,
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[(StatusCode, JsValue)] = {
    import encoder.implicits._
    for {
      json <- FutureUtil.toFuture(SprayJson.encode1(cmd)): Future[JsValue]
      result <- postJsonRequest(uri.withPath(Uri.Path("/v1/create")), json, headers = headers)
    } yield result
  }

  def postArchiveCommand(
      templateId: domain.TemplateId.OptionalPkg,
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
    domain.ExerciseCommand(reference, choice, boxedRecord(arg), None)
  }

  def accountCreateCommand(
      owner: domain.Party,
      number: String,
      time: v.Value.Sum.Timestamp = TimestampConversion.instantToMicros(Instant.now),
  ): domain.CreateCommand[v.Record] = {
    val templateId = domain.TemplateId(None, "Account", "Account")
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

  def getContractId(result: JsValue): domain.ContractId =
    inside(result.asJsObject.fields.get("contractId")) { case Some(JsString(contractId)) =>
      domain.ContractId(contractId)
    }

  def getResult(output: JsValue): JsValue = getChild(output, "result")

  def getWarnings(output: JsValue): JsValue = getChild(output, "warnings")

  def getChild(output: JsValue, field: String): JsValue = {
    def errorMsg = s"Expected JsObject with '$field' field, got: $output"
    output
      .asJsObject(errorMsg)
      .fields
      .getOrElse(field, fail(errorMsg))
  }
}
