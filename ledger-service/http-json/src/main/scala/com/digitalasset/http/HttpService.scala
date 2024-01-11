// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.ConnectionContext
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.HttpsConnectionContext
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.settings.ServerSettings
import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.http.dbbackend.ContractDao
import com.daml.http.json.{
  ApiValueToJsValueConverter,
  DomainJsonDecoder,
  DomainJsonEncoder,
  JsValueToApiValueConverter,
}
import com.daml.http.metrics.HttpJsonApiMetrics
import com.daml.http.util.ApiValueToLfValueConverter
import com.daml.http.util.FutureUtil._
import com.daml.http.util.Logging.InstanceUUID
import com.daml.jwt.JwtDecoder
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.client.services.pkg.withoutledgerid.PackageClient
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.ledger.service.LedgerReader
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.pekkohttp.HttpMetricsInterceptor
import com.daml.ports.{Port, PortFiles}
import io.grpc.health.v1.health.{HealthCheckRequest, HealthGrpc}
import scalaz.Scalaz._
import scalaz._
import scala.util.Using

import java.nio.file.{Files, Path}
import java.security.{Key, KeyStore}

import scala.concurrent.{ExecutionContext, Future}
import ch.qos.logback.classic.{Level => LogLevel}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.auth.AuthServiceJWTCodec
import com.daml.ledger.api.{domain => LedgerApiDomain}
import com.daml.ledger.api.tls.TlsConfiguration

object HttpService {

  private val logger = ContextualizedLogger.get(getClass)

  // used only to populate a required field in LedgerClientConfiguration
  private val DummyApplicationId: ApplicationId = ApplicationId("HTTP-JSON-API-Gateway")

  // default to 10 minutes for ledger connection retry period when ledger is not ready on start
  private val MaxInitialLedgerConnectRetryAttempts = 600

  private type ET[A] = EitherT[Future, Error, A]

  object Error {
    def fromLedgerClientError(e: LedgerClientBase.Error): Error = Error(e.message)
  }

  final case class Error(message: String)

  private def isLogLevelEqualOrBelowDebug(logLevel: Option[LogLevel]) =
    logLevel.exists(!_.isGreaterOrEqual(LogLevel.INFO))

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def start(
      startSettings: StartSettings,
      contractDao: Option[ContractDao] = None,
      validateJwt: EndpointsCompanion.ValidateJwt = decodeJwt,
  )(implicit
      asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      metrics: HttpJsonApiMetrics,
  ): Future[Error \/ (ServerBinding, Option[ContractDao])] = {

    logger.info("HTTP Server pre-startup")

    import startSettings._

    implicit val settings: ServerSettings = ServerSettings(asys).withTransparentHeadRequests(true)

    val parseJwt: EndpointsCompanion.ParseJwt =
      startSettings.authConfig.flatMap(_.targetScope).fold(parseAnyJwt)(parseScopeJwt)

    val clientConfig = LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(DummyApplicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
    )

    val clientChannelConfiguration =
      LedgerClientChannelConfiguration(
        sslContext = tlsConfig.client(),
        maxInboundMessageSize = maxInboundMessageSize,
      )

    import org.apache.pekko.http.scaladsl.server.Directives._
    val bindingEt: EitherT[Future, Error, (ServerBinding, Option[ContractDao])] = for {

      client <- eitherT(
        ledgerClient(
          ledgerHost,
          ledgerPort,
          clientConfig,
          clientChannelConfiguration,
        )
      ): ET[DamlLedgerClient]

      pkgManagementClient <- eitherT(
        ledgerClient(
          ledgerHost,
          ledgerPort,
          clientConfig,
          packageMaxInboundMessageSize.fold(clientChannelConfiguration)(size =>
            clientChannelConfiguration.copy(maxInboundMessageSize = size)
          ),
        )
      ): ET[DamlLedgerClient]

      _ = logger.info(s"contractDao: ${contractDao.toString}")

      packageCache = LedgerReader.LoadCache.freshCache()

      packageService = new PackageService(doLoad(pkgManagementClient.packageClient, packageCache))

      commandService = new CommandService(
        LedgerClientJwt.submitAndWaitForTransaction(client),
        LedgerClientJwt.submitAndWaitForTransactionTree(client),
      )

      contractsService = new ContractsService(
        packageService.resolveContractTypeId,
        packageService.allTemplateIds,
        LedgerClientJwt.getActiveContracts(client),
        LedgerClientJwt.getCreatesAndArchivesSince(client),
        LedgerClientJwt.getTermination(client),
        LedgerReader.damlLfTypeLookup(() => packageService.packageStore),
        contractDao,
      )

      partiesService = new PartiesService(
        LedgerClientJwt.listKnownParties(client),
        LedgerClientJwt.getParties(client),
        LedgerClientJwt.allocateParty(client),
      )

      packageManagementService = new PackageManagementService(
        LedgerClientJwt.listPackages(pkgManagementClient),
        LedgerClientJwt.getPackage(pkgManagementClient),
        { case (jwt, ledgerId, byteString) =>
          implicit lc =>
            LedgerClientJwt
              .uploadDar(pkgManagementClient)(ec)(
                jwt,
                ledgerId,
                byteString,
              )(lc)
              .flatMap(_ => packageService.reload(jwt, ledgerId))
              .map(_ => ())
        },
      )

      meteringReportService = new MeteringReportService(
        { case (jwt, request) =>
          implicit lc =>
            LedgerClientJwt.getMeteringReport(client)(ec)(jwt, request)(
              lc
            )
        }
      )

      ledgerHealthService = HealthGrpc.stub(client.channel)

      healthService = new HealthService(
        () => ledgerHealthService.check(HealthCheckRequest()),
        contractDao,
        healthTimeoutSeconds,
      )

      _ = metrics.health.registerHealthGauge(
        HttpJsonApiMetrics.ComponentName,
        () => healthService.ready().map(_.checks.forall(_.result)),
      )

      (encoder, decoder) = buildJsonCodecs(packageService)

      jsonEndpoints = new Endpoints(
        allowNonHttps,
        validateJwt,
        parseJwt,
        commandService,
        contractsService,
        partiesService,
        packageManagementService,
        meteringReportService,
        healthService,
        encoder,
        decoder,
        isLogLevelEqualOrBelowDebug(logLevel),
        client.userManagementClient,
        client.identityClient,
      )

      websocketService = new WebSocketService(
        contractsService,
        packageService.resolveContractTypeId,
        decoder,
        LedgerReader.damlLfTypeLookup(() => packageService.packageStore),
        wsConfig,
      )

      websocketEndpoints = new WebsocketEndpoints(
        validateJwt,
        parseJwt,
        websocketService,
        client.userManagementClient,
        client.identityClient,
      )

      rateDurationSizeMetrics = HttpMetricsInterceptor.rateDurationSizeMetrics(
        metrics.http
      )

      defaultEndpoints =
        rateDurationSizeMetrics apply concat(
          jsonEndpoints.all: Route,
          websocketEndpoints.transactionWebSocket,
        )

      allEndpoints = concat(
        staticContentConfig.cata(
          c => concat(StaticContentEndpoints.all(c), defaultEndpoints),
          defaultEndpoints,
        ),
        EndpointsCompanion.notFound,
      )

      binding <- liftET[Error] {
        val builder = Http().newServerAt(address, httpPort).withSettings(settings)
        https
          .fold(builder) { config =>
            logger.info(s"Enabling HTTPS with $config")
            builder.enableHttps(httpsConnectionContext(config))
          }
          .bind(allEndpoints)
      }

      _ <- either(portFile.cata(f => createPortFile(f, binding), \/-(()))): ET[Unit]

    } yield (binding, contractDao)

    bindingEt.run: Future[Error \/ (ServerBinding, Option[ContractDao])]
  }

  private[http] def httpsConnectionContext(config: TlsConfiguration): HttpsConnectionContext = {
    import java.security.SecureRandom
    import javax.net.ssl.{SSLContext, KeyManagerFactory, TrustManagerFactory}

    val keyStore = buildKeyStore(
      config.certChainFile.get.toPath,
      config.privateKeyFile.get.toPath,
      config.trustCollectionFile.get.toPath,
    )

    val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(keyStore, null)

    val trustManagerFactory = TrustManagerFactory.getInstance("SunX509")
    trustManagerFactory.init(keyStore)

    val context = SSLContext.getInstance("TLS")
    context.init(
      keyManagerFactory.getKeyManagers,
      trustManagerFactory.getTrustManagers,
      new SecureRandom,
    )

    ConnectionContext.httpsServer(context)
  }

  def buildKeyStore(certFile: Path, privateKeyFile: Path, caCertFile: Path): KeyStore = {
    import java.security.cert.CertificateFactory

    val alias = "key" // This can be anything as long as it's consistent.

    val cf = CertificateFactory.getInstance("X.509")
    val cert = Using.resource(Files.newInputStream(certFile)) { cf.generateCertificate(_) }
    val caCert = Using.resource(Files.newInputStream(caCertFile)) { cf.generateCertificate(_) }
    val privateKey = loadPrivateKey(privateKeyFile)

    val keyStore = KeyStore.getInstance("PKCS12")
    keyStore.load(null)
    keyStore.setCertificateEntry(alias, cert)
    keyStore.setCertificateEntry(alias, caCert)
    keyStore.setKeyEntry(alias, privateKey, null, Array(cert, caCert))
    keyStore
  }

  def loadPrivateKey(pkRsaPemFile: Path): Key = {
    import org.bouncycastle.openssl.PEMParser
    import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
    import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter

    Using.resource(Files.newBufferedReader(pkRsaPemFile)) { reader =>
      val pemParser = new PEMParser(reader)
      val pkInfo = PrivateKeyInfo.getInstance(pemParser.readObject())
      new JcaPEMKeyConverter().getPrivateKey(pkInfo)
    }
  }

  private[http] def doLoad(
      packageClient: PackageClient,
      loadCache: LedgerReader.LoadCache,
  )(jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(ids: Set[String])(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[PackageService.ServerError \/ Option[PackageStore]] =
    LedgerReader
      .loadPackageStoreUpdates(
        packageClient,
        loadCache,
        some(jwt.value),
        ledgerId,
      )(ids)
      .map(_.leftMap(e => PackageService.ServerError(e)))

  def stop(
      f: Future[Error \/ (ServerBinding, Option[ContractDao])]
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[Unit] = {
    logger.info("Stopping server...")
    f.collect { case \/-((a, dao)) =>
      dao.foreach(_.close())
      a.unbind().void
    }.join
  }

  // Decode JWT without any validation
  private[http] val decodeJwt: EndpointsCompanion.ValidateJwt =
    jwt => JwtDecoder.decode(jwt).leftMap(e => EndpointsCompanion.Unauthorized(e.shows))

  private[http] val parseAnyJwt: EndpointsCompanion.ParseJwt =
    jwt =>
      AuthServiceJWTCodec
        .readFromString(jwt.payload)
        .toEither
        .disjunction
        .leftMap(EndpointsCompanion.Error.fromThrowable)

  private[http] def parseScopeJwt(targetScope: String): EndpointsCompanion.ParseJwt =
    jwt =>
      AuthServiceJWTCodec
        .readScopeBasedTokenFromString(targetScope)(jwt.payload)
        .toEither
        .disjunction
        .leftMap(EndpointsCompanion.Error.fromThrowable)

  private[http] def buildJsonCodecs(
      packageService: PackageService
  )(implicit ec: ExecutionContext): (DomainJsonEncoder, DomainJsonDecoder) = {

    val lfTypeLookup = LedgerReader.damlLfTypeLookup(() => packageService.packageStore) _
    val jsValueToApiValueConverter = new JsValueToApiValueConverter(lfTypeLookup)

    val apiValueToJsValueConverter = new ApiValueToJsValueConverter(
      ApiValueToLfValueConverter.apiValueToLfValue
    )

    val encoder = new DomainJsonEncoder(
      apiValueToJsValueConverter.apiRecordToJsObject,
      apiValueToJsValueConverter.apiValueToJsValue,
    )

    val decoder = new DomainJsonDecoder(
      packageService.resolveContractTypeId,
      packageService.resolveTemplateRecordType,
      packageService.resolveChoiceArgType,
      packageService.resolveKeyType,
      jsValueToApiValueConverter.jsValueToApiValue,
      jsValueToApiValueConverter.jsValueToLfValue,
    )

    (encoder, decoder)
  }

  private def ledgerClient(
      ledgerHost: String,
      ledgerPort: Int,
      clientConfig: LedgerClientConfiguration,
      clientChannelConfig: LedgerClientChannelConfiguration,
  )(implicit
      ec: ExecutionContext,
      aesf: ExecutionSequencerFactory,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[Error \/ DamlLedgerClient] =
    LedgerClient
      .fromRetried(
        ledgerHost,
        ledgerPort,
        clientConfig,
        clientChannelConfig,
        MaxInitialLedgerConnectRetryAttempts,
      )
      .map(
        _.leftMap(Error.fromLedgerClientError)
      )

  private def createPortFile(
      file: Path,
      binding: org.apache.pekko.http.scaladsl.Http.ServerBinding,
  ): Error \/ Unit = {
    import util.ErrorOps._
    PortFiles.write(file, Port(binding.localAddress.getPort)).liftErr(Error.apply)
  }

}
