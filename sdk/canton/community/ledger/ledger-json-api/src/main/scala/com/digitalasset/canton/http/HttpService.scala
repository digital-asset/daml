// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContextOf
import com.daml.metrics.api.MetricHandle.Gauge.CloseableGauge
import com.daml.metrics.pekkohttp.HttpMetricsInterceptor
import com.daml.ports.{Port, PortFiles}
import com.daml.tls.TlsVersion
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.config.{
  ApiLoggingConfig,
  ServerAuthRequirementConfig,
  TlsClientConfig,
  TlsServerConfig,
}
import com.digitalasset.canton.http.HttpService.HttpServiceHandle
import com.digitalasset.canton.http.json.v2.V2Routes
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.http.util.FutureUtil.*
import com.digitalasset.canton.http.util.Logging.InstanceUUID
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.UserId
import com.digitalasset.canton.ledger.client.LedgerClient as DamlLedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.ledger.participant.state.PackageSyncService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.platform.PackagePreferenceBackend
import com.digitalasset.canton.tracing.NoTracing
import io.grpc.Channel
import io.grpc.health.v1.health.{HealthCheckRequest, HealthGrpc}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.server.Directives.{concat, pathPrefix}
import org.apache.pekko.http.scaladsl.server.{PathMatcher, Route}
import org.apache.pekko.http.scaladsl.settings.ServerSettings
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import org.apache.pekko.stream.Materializer
import scalaz.*
import scalaz.Scalaz.*

import java.io.InputStream
import java.nio.file.{Files, Path}
import java.security.{Key, KeyStore}
import javax.net.ssl.SSLContext
import scala.concurrent.Future
import scala.util.Using

class HttpService(
    startSettings: JsonApiConfig,
    httpsConfiguration: Option[TlsServerConfig],
    channel: Channel,
    packageSyncService: PackageSyncService,
    packagePreferenceBackend: PackagePreferenceBackend,
    apiLoggingConfig: ApiLoggingConfig,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    asys: ActorSystem,
    mat: Materializer,
    esf: ExecutionSequencerFactory,
    lc: LoggingContextOf[InstanceUUID],
    metrics: HttpApiMetrics,
    authInterceptor: AuthInterceptor,
) extends ResourceOwner[HttpServiceHandle]
    with NamedLogging
    with NoTracing {
  private type ET[A] = EitherT[Future, HttpService.Error, A]

  def acquire()(implicit context: ResourceContext): Resource[HttpServiceHandle] = {
    logger.info(s"Starting JSON API server, ${lc.makeString}")

    val DummyUserId: UserId = UserId("HTTP-JSON-API-Gateway")

    val clientConfig = LedgerClientConfiguration(
      userId = UserId.unwrap(DummyUserId),
      commandClient = CommandClientConfiguration.default,
    )

    val ledgerClient: DamlLedgerClient =
      DamlLedgerClient.withoutToken(channel, clientConfig, loggerFactory)

    val ledgerHealthService = HealthGrpc.stub(channel)
    val healthService = new HealthService(() => ledgerHealthService.check(HealthCheckRequest()))

    for {
      gauge <- Resource(Future {
        metrics.health.registerHealthGauge(
          HttpApiMetrics.ComponentName,
          () => healthService.ready().map(_.checks.forall(_.result)),
        )
      })(gauge => Future(gauge.close()))
      binding <- Resource(serverBinding(ledgerClient, healthService)) { binding =>
        logger.info(s"Stopping JSON API server..., ${lc.makeString}")
        binding.unbind().void
      }
    } yield HttpServiceHandle(binding, gauge)
  }

  private def serverBinding(
      ledgerClient: DamlLedgerClient,
      healthService: HealthService,
  )(implicit context: ResourceContext): Future[ServerBinding] = {
    val settings: ServerSettings = ServerSettings(asys)
      .withTransparentHeadRequests(true)
      .mapTimeouts(_.withRequestTimeout(startSettings.requestTimeout))

    implicit val wsConfig = startSettings.websocketConfig.getOrElse(WebsocketConfig())

    val bindingEt: EitherT[Future, HttpService.Error, ServerBinding] =
      for {
        _ <- eitherT(Future.successful(\/-(ledgerClient)))

        v2Routes = V2Routes(
          ledgerClient,
          metadataServiceEnabled = startSettings.damlDefinitionsServiceEnabled,
          packageSyncService,
          packagePreferenceBackend,
          mat.executionContext,
          apiLoggingConfig,
          loggerFactory,
        )

        jsonEndpoints = new JsonRoutes(
          healthService,
          v2Routes,
          startSettings.debugLoggingOfHttpBodies,
          loggerFactory,
        )

        rateDurationSizeMetrics = HttpMetricsInterceptor.rateDurationSizeMetrics(metrics.http)

        defaultEndpoints =
          rateDurationSizeMetrics apply jsonEndpoints.all

        allEndpoints: Route = concat(
          defaultEndpoints,
          EndpointsCompanion.notFound(logger),
        )
        prefixedEndpoints = startSettings.pathPrefix
          .map(_.split("/").toList.dropWhile(_.isEmpty))
          .collect { case head :: tl =>
            val joinedPrefix = tl.foldLeft(PathMatcher(Uri.Path(head), ()))(_ slash _)
            pathPrefix(joinedPrefix)(allEndpoints)
          }
          .getOrElse(allEndpoints)

        binding <- liftET[HttpService.Error] {
          val serverBuilder = Http()
            .newServerAt(startSettings.address, startSettings.port.unwrap)
            .withSettings(settings)

          httpsConfiguration
            .fold(serverBuilder) { config =>
              logger.info(s"Enabling HTTPS with $config")
              serverBuilder.enableHttps(HttpService.httpsConnectionContext(config)(logger))
            }
            .bind(prefixedEndpoints)
        }

        _ <- either(
          startSettings.portFile.cata(f => HttpService.createPortFile(f, binding), \/-(()))
        ): ET[Unit]

      } yield binding

    (bindingEt.run: Future[HttpService.Error \/ ServerBinding]).flatMap {
      case -\/(error) => Future.failed(new RuntimeException(error.message))
      case \/-(binding) => Future.successful(binding)
    }
  }

}

object HttpService extends NoTracing {

  final case class HttpServiceHandle(binding: ServerBinding, gauge: CloseableGauge)
  // if no minimumServerProtocolVersion is set `config.protocols` returns an empty list
  // but we still want to setup some protocols
  private val allowedProtocols =
    Set[TlsVersion.TlsVersion](TlsVersion.V1_2, TlsVersion.V1_3).map(_.version)

  private[http] def createPortFile(
      file: Path,
      binding: org.apache.pekko.http.scaladsl.Http.ServerBinding,
  ): HttpService.Error \/ Unit = {
    import com.digitalasset.canton.http.util.ErrorOps.*
    PortFiles.write(file, Port(binding.localAddress.getPort)).liftErr(Error.apply)
  }

  def buildSSLContext(config: TlsServerConfig): SSLContext = {
    val serverKeyStore = buildKeyStore(config)
    buildSSLContext(serverKeyStore)
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

  private def httpsConnectionContext(
      config: TlsServerConfig
  )(implicit logger: TracedLogger): HttpsConnectionContext =
    ConnectionContext.httpsServer { () =>
      val engine = buildSSLContext(config).createSSLEngine()
      engine.setUseClientMode(false)
      engine.setEnabledCipherSuites(config.ciphers.getOrElse(Seq.empty).toArray)
      logger.info(
        s"Ledger JSON API Enabled Ciphers: ${engine.getEnabledCipherSuites.mkString(", ")}"
      )
      config.clientAuth match {
        case ServerAuthRequirementConfig.None =>
          engine.setNeedClientAuth(false)
          engine.setWantClientAuth(false)
        case ServerAuthRequirementConfig.Optional =>
          engine.setNeedClientAuth(false)
          engine.setWantClientAuth(true)
        case ServerAuthRequirementConfig.Require(
              _
            ) => // certs inside Require are only needed to construct grpc client
          engine.setNeedClientAuth(true)
          engine.setWantClientAuth(true)
      }
      logger.info(
        s"Ledger JSON API NeedClientAuth:${engine.getNeedClientAuth} WantClientAuth:${engine.getWantClientAuth}"
      )

      val enabledProtocols = config.protocols.getOrElse(
        engine
          .getEnabledProtocols()
          .toSeq
          .filter(
            HttpService.allowedProtocols.contains(_)
          )
      )

      logger.info(s"Ledger JSON API enabled TLS protocols: $enabledProtocols")
      engine.setEnabledProtocols(enabledProtocols.toArray)
      engine
    }

  // TODO(i22574): Remove OptionPartial and Null warts in HttpService
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private def buildKeyStore(config: TlsServerConfig): KeyStore = buildKeyStore(
    config.certChainFile.pemStream,
    config.privateKeyFile.pemFile.unwrap.toPath,
    config.trustCollectionFile.get.pemStream,
  )

  // TODO(i22574): Remove OptionPartial and Null warts in HttpService
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private def buildKeyStore(config: TlsClientConfig): KeyStore = buildKeyStore(
    config.clientCert.get.certChainFile.pemStream,
    config.clientCert.get.privateKeyFile.pemFile.unwrap.toPath,
    config.trustCollectionFile.get.pemStream,
  )

  // TODO(i22574): Remove OptionPartial and Null warts in HttpService
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def buildKeyStore(
      certFile: InputStream,
      privateKeyFile: Path,
      caCertFile: InputStream,
  ): KeyStore = {
    import java.security.cert.CertificateFactory
    val alias = "key" // This can be anything as long as it's consistent.

    val cf = CertificateFactory.getInstance("X.509")
    val cert = Using.resource(certFile)(cf.generateCertificate(_))
    val caCert = Using.resource(caCertFile)(cf.generateCertificate(_))
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
