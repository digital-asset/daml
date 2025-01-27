// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.jwt.{Jwt, JwtDecoder}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContextOf
import com.daml.metrics.pekkohttp.HttpMetricsInterceptor
import com.daml.ports.{Port, PortFiles}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.{TlsClientConfig, TlsServerConfig}
import com.digitalasset.canton.http.json.v2.V2Routes
import com.digitalasset.canton.http.json.{
  ApiJsonDecoder,
  ApiJsonEncoder,
  ApiValueToJsValueConverter,
  JsValueToApiValueConverter,
}
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.http.util.ApiValueToLfValueConverter
import com.digitalasset.canton.http.util.FutureUtil.*
import com.digitalasset.canton.http.util.Logging.InstanceUUID
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.canton.ledger.client.LedgerClient as DamlLedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.ledger.client.services.admin.{
  IdentityProviderConfigClient,
  UserManagementClient,
}
import com.digitalasset.canton.ledger.client.services.pkg.PackageClient
import com.digitalasset.canton.ledger.participant.state.PackageSyncService
import com.digitalasset.canton.ledger.service.LedgerReader
import com.digitalasset.canton.ledger.service.LedgerReader.PackageStore
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import io.grpc.Channel
import io.grpc.health.v1.health.{HealthCheckRequest, HealthGrpc}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.server.{PathMatcher, Route}
import org.apache.pekko.http.scaladsl.settings.ServerSettings
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import org.apache.pekko.stream.Materializer
import scalaz.*
import scalaz.Scalaz.*

import java.nio.file.{Files, Path}
import java.security.{Key, KeyStore}
import javax.net.ssl.SSLContext
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Using

class HttpService(
    startSettings: StartSettings,
    httpsConfiguration: Option[TlsServerConfig],
    channel: Channel,
    packageSyncService: PackageSyncService,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    asys: ActorSystem,
    mat: Materializer,
    esf: ExecutionSequencerFactory,
    lc: LoggingContextOf[InstanceUUID],
    metrics: HttpApiMetrics,
) extends ResourceOwner[ServerBinding]
    with NamedLogging
    with NoTracing {
  import HttpService.doLoad

  private type ET[A] = EitherT[Future, HttpService.Error, A]

  private val directEc = DirectExecutionContext(noTracingLogger)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def acquire()(implicit context: ResourceContext): Resource[ServerBinding] =
    Resource({
      logger.info(s"Starting JSON API server, ${lc.makeString}")

      import startSettings.*
      val DummyApplicationId: ApplicationId = ApplicationId("HTTP-JSON-API-Gateway")

      implicit val settings: ServerSettings = ServerSettings(asys).withTransparentHeadRequests(true)
      implicit val wsConfig = startSettings.websocketConfig.getOrElse(WebsocketConfig())

      val clientConfig = LedgerClientConfiguration(
        applicationId = ApplicationId.unwrap(DummyApplicationId),
        commandClient = CommandClientConfiguration.default,
      )

      val ledgerClient: DamlLedgerClient =
        DamlLedgerClient.withoutToken(channel, clientConfig, loggerFactory)

      val resolveUser: EndpointsCompanion.ResolveUser =
        if (startSettings.userManagementWithoutAuthorization)
          HttpService.resolveUserWithIdp(
            ledgerClient.userManagementClient,
            ledgerClient.identityProviderConfigClient,
          )
        else
          HttpService.resolveUser(ledgerClient.userManagementClient)

      import org.apache.pekko.http.scaladsl.server.Directives.*
      val bindingEt: EitherT[Future, HttpService.Error, ServerBinding] =
        for {
          _ <- eitherT(Future.successful(\/-(ledgerClient)))
          packageCache = LedgerReader.LoadCache.freshCache()

          packageService = new PackageService(
            reloadPackageStoreIfChanged =
              doLoad(ledgerClient.packageService, LedgerReader(loggerFactory), packageCache),
            loggerFactory = loggerFactory,
          )

          ledgerClientJwt = LedgerClientJwt(loggerFactory)

          commandService = new CommandService(
            ledgerClientJwt.submitAndWaitForTransaction(ledgerClient),
            ledgerClientJwt.submitAndWaitForTransactionTree(ledgerClient),
            loggerFactory,
          )

          contractsService = new ContractsService(
            packageService.resolveContractTypeId,
            packageService.allTemplateIds,
            ledgerClientJwt.getByContractId(ledgerClient),
            ledgerClientJwt.getActiveContracts(ledgerClient),
            ledgerClientJwt.getCreatesAndArchivesSince(ledgerClient),
            ledgerClientJwt.getLedgerEnd(ledgerClient),
            loggerFactory,
          )

          partiesService = new PartiesService(
            ledgerClientJwt.listKnownParties(ledgerClient),
            ledgerClientJwt.getParties(ledgerClient),
            ledgerClientJwt.allocateParty(ledgerClient),
          )

          packageManagementService = new PackageManagementService(
            ledgerClientJwt.listPackages(ledgerClient),
            ledgerClientJwt.getPackage(ledgerClient),
            { case (jwt, byteString) =>
              implicit lc =>
                ledgerClientJwt
                  .uploadDar(ledgerClient)(directEc, traceContext)(
                    jwt,
                    byteString,
                  )(lc)
                  .flatMap(_ => packageService.reload(jwt))
                  .map(_ => ())
            },
          )

          meteringReportService = new MeteringReportService(
            { case (jwt, request) =>
              implicit lc =>
                ledgerClientJwt
                  .getMeteringReport(ledgerClient)(directEc, traceContext)(jwt, request)(
                    lc
                  )
            }
          )

          ledgerHealthService = HealthGrpc.stub(channel)

          healthService = new HealthService(() => ledgerHealthService.check(HealthCheckRequest()))

          _ = metrics.health.registerHealthGauge(
            HttpApiMetrics.ComponentName,
            () => healthService.ready().map(_.checks.forall(_.result)),
          )

          (encoder, decoder) = HttpService.buildJsonCodecs(packageService)

          v2Routes = V2Routes(
            ledgerClient,
            packageService,
            metadataServiceEnabled = startSettings.damlDefinitionsServiceEnabled,
            packageSyncService,
            mat.executionContext,
            loggerFactory,
          )

          jsonEndpoints = new Endpoints(
            httpsConfiguration.isEmpty,
            HttpService.decodeJwt,
            commandService,
            contractsService,
            partiesService,
            packageManagementService,
            meteringReportService,
            healthService,
            v2Routes,
            encoder,
            decoder,
            debugLoggingOfHttpBodies,
            resolveUser,
            ledgerClient.userManagementClient,
            loggerFactory,
          )

          websocketService = new WebSocketService(
            contractsService,
            packageService.resolveContractTypeId,
            decoder,
            websocketConfig,
            loggerFactory,
          )

          websocketEndpoints = new WebsocketEndpoints(
            HttpService.decodeJwt,
            websocketService,
            resolveUser,
            loggerFactory,
          )

          rateDurationSizeMetrics = HttpMetricsInterceptor.rateDurationSizeMetrics(metrics.http)

          defaultEndpoints =
            rateDurationSizeMetrics apply concat(
              jsonEndpoints.all: Route,
              websocketEndpoints.transactionWebSocket,
            )

          allEndpoints: Route = concat(
            defaultEndpoints,
            EndpointsCompanion.notFound(logger),
          )
          prefixedEndpoints = server.pathPrefix
            .map(_.split("/").toList.dropWhile(_.isEmpty))
            .collect { case head :: tl =>
              val joinedPrefix = tl.foldLeft(PathMatcher(Uri.Path(head), ()))(_ slash _)
              pathPrefix(joinedPrefix)(allEndpoints)
            }
            .getOrElse(allEndpoints)

          binding <- liftET[HttpService.Error] {
            val serverBuilder = Http()
              .newServerAt(server.address, server.port.getOrElse(0))
              .withSettings(settings)

            httpsConfiguration
              .fold(serverBuilder) { config =>
                logger.info(s"Enabling HTTPS with $config")
                serverBuilder.enableHttps(HttpService.httpsConnectionContext(config))
              }
              .bind(prefixedEndpoints)
          }

          _ <- either(
            server.portFile.cata(f => HttpService.createPortFile(f, binding), \/-(()))
          ): ET[Unit]

        } yield binding

      (bindingEt.run: Future[HttpService.Error \/ ServerBinding]).flatMap {
        case -\/(error) => Future.failed(new RuntimeException(error.message))
        case \/-(binding) => Future.successful(binding)
      }
    }) { binding =>
      logger.info(s"Stopping JSON API server..., ${lc.makeString}")
      binding.unbind().void
    }

}

object HttpService extends NoTracing {

  def resolveUser(userManagementClient: UserManagementClient): EndpointsCompanion.ResolveUser =
    jwt => userId => userManagementClient.listUserRights(userId = userId, token = Some(jwt.value))

  def resolveUserWithIdp(
      userManagementClient: UserManagementClient,
      idpClient: IdentityProviderConfigClient,
  )(implicit ec: ExecutionContext): EndpointsCompanion.ResolveUser = jwt =>
    userId => {
      for {
        idps <- idpClient
          .listIdentityProviderConfigs(token = Some(jwt.value))
          .map(_.map(_.identityProviderId.value))
        userWithIdp <- Future
          .traverse("" +: idps)(idp =>
            userManagementClient
              .listUsers(
                token = Some(jwt.value),
                identityProviderId = idp,
                pageToken = "",
                // Hardcoded limit for users within any idp. This is enough for the limited usage
                // of this functionality in the transition phase from json-api v1 to v2.
                pageSize = 1000,
              )
              .map(_._1)
          )
          .map(_.flatten.filter(_.id == userId))
        userRight <- Future.traverse(userWithIdp)(user =>
          userManagementClient.listUserRights(
            token = Some(jwt.value),
            userId = userId,
            identityProviderId = user.identityProviderId.toRequestString,
          )
        )
      } yield userRight.flatten

    }
  // TODO(#13303) Check that this is intended to be used as ValidateJwt in prod code
  //              and inline.
  // Decode JWT without any validation
  private val decodeJwt: EndpointsCompanion.ValidateJwt =
    jwt => JwtDecoder.decode(jwt).leftMap(e => EndpointsCompanion.Unauthorized(e.shows))

  def doLoad(
      packageClient: PackageClient,
      ledgerReader: LedgerReader,
      loadCache: LedgerReader.LoadCache,
  )(jwt: Jwt)(ids: Set[String])(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[PackageService.ServerError \/ Option[PackageStore]] =
    ledgerReader
      .loadPackageStoreUpdates(
        packageClient,
        loadCache,
        some(jwt.value),
      )(ids)
      .map(_.leftMap(e => PackageService.ServerError(e)))

  def buildJsonCodecs(
      packageService: PackageService
  )(implicit ec: ExecutionContext): (ApiJsonEncoder, ApiJsonDecoder) = {

    val lfTypeLookup = LedgerReader.damlLfTypeLookup(() => packageService.packageStore) _
    val jsValueToApiValueConverter = new JsValueToApiValueConverter(lfTypeLookup)

    val apiValueToJsValueConverter = new ApiValueToJsValueConverter(
      ApiValueToLfValueConverter.apiValueToLfValue
    )

    val encoder = new ApiJsonEncoder(
      apiValueToJsValueConverter.apiRecordToJsObject,
      apiValueToJsValueConverter.apiValueToJsValue,
    )

    val decoder = new ApiJsonDecoder(
      packageService.resolveContractTypeId,
      packageService.resolveTemplateRecordType,
      packageService.resolveChoiceArgType,
      packageService.resolveKeyType,
      jsValueToApiValueConverter.jsValueToApiValue,
      jsValueToApiValueConverter.jsValueToLfValue,
    )

    (encoder, decoder)
  }

  private[http] def createPortFile(
      file: Path,
      binding: org.apache.pekko.http.scaladsl.Http.ServerBinding,
  ): HttpService.Error \/ Unit = {
    import com.digitalasset.canton.http.util.ErrorOps.*
    PortFiles.write(file, Port(binding.localAddress.getPort)).liftErr(Error.apply)
  }

  def buildSSLContext(config: TlsServerConfig): SSLContext = {
    val keyStore = buildKeyStore(config)
    buildSSLContext(keyStore)
  }

  def buildSSLContext(config: TlsClientConfig): SSLContext = {
    val keyStore = buildKeyStore(config)
    buildSSLContext(keyStore)
  }

  // TODO(i22574): Remove OptionPartial and Null warts in HttpService
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def buildSSLContext(keyStore: KeyStore): SSLContext = {
    import java.security.SecureRandom
    import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

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
    context
  }

  private def httpsConnectionContext(config: TlsServerConfig): HttpsConnectionContext =
    ConnectionContext.httpsServer(buildSSLContext(config))

  // TODO(i22574): Remove OptionPartial and Null warts in HttpService
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private def buildKeyStore(config: TlsServerConfig): KeyStore = buildKeyStore(
    config.certChainFile.unwrap.toPath,
    config.privateKeyFile.unwrap.toPath,
    config.trustCollectionFile.get.unwrap.toPath,
  )

  // TODO(i22574): Remove OptionPartial and Null warts in HttpService
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private def buildKeyStore(config: TlsClientConfig): KeyStore = buildKeyStore(
    config.clientCert.get.certChainFile.toPath,
    config.clientCert.get.privateKeyFile.toPath,
    config.trustCollectionFile.get.unwrap.toPath,
  )

  // TODO(i22574): Remove OptionPartial and Null warts in HttpService
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def buildKeyStore(certFile: Path, privateKeyFile: Path, caCertFile: Path): KeyStore = {
    import java.security.cert.CertificateFactory
    val alias = "key" // This can be anything as long as it's consistent.

    val cf = CertificateFactory.getInstance("X.509")
    val cert = Using.resource(Files.newInputStream(certFile))(cf.generateCertificate(_))
    val caCert = Using.resource(Files.newInputStream(caCertFile))(cf.generateCertificate(_))
    val privateKey = loadPrivateKey(privateKeyFile)

    val keyStore = KeyStore.getInstance("PKCS12")
    keyStore.load(null)
    keyStore.setCertificateEntry(alias, cert)
    keyStore.setCertificateEntry(alias, caCert)
    keyStore.setKeyEntry(alias, privateKey, null, Array(cert, caCert))
    keyStore.setCertificateEntry("trusted-ca", caCert)
    keyStore
  }

  private def loadPrivateKey(pkRsaPemFile: Path): Key = {
    import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
    import org.bouncycastle.openssl.PEMParser
    import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter

    Using.resource(Files.newBufferedReader(pkRsaPemFile)) { reader =>
      val pemParser = new PEMParser(reader)
      val pkInfo = PrivateKeyInfo.getInstance(pemParser.readObject())
      new JcaPEMKeyConverter().getPrivateKey(pkInfo)
    }
  }

  final case class Error(message: String)
}
