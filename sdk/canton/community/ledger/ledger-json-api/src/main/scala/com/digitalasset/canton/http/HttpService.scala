// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.jwt.JwtDecoder
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContextOf
import com.daml.metrics.pekkohttp.HttpMetricsInterceptor
import com.daml.ports.{Port, PortFiles}
import com.daml.tls.TlsVersion
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.config.{
  ServerAuthRequirementConfig,
  TlsClientConfig,
  TlsServerConfig,
}
import com.digitalasset.canton.http.json.v1.V1Routes
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
import com.digitalasset.canton.ledger.client.services.admin.{
  IdentityProviderConfigClient,
  UserManagementClient,
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
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Using

class HttpService(
    startSettings: StartSettings,
    httpsConfiguration: Option[TlsServerConfig],
    channel: Channel,
    packageSyncService: PackageSyncService,
    packagePreferenceBackend: PackagePreferenceBackend,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    asys: ActorSystem,
    mat: Materializer,
    esf: ExecutionSequencerFactory,
    lc: LoggingContextOf[InstanceUUID],
    metrics: HttpApiMetrics,
    authInterceptor: AuthInterceptor,
) extends ResourceOwner[ServerBinding]
    with NamedLogging
    with NoTracing {
  private type ET[A] = EitherT[Future, HttpService.Error, A]

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def acquire()(implicit context: ResourceContext): Resource[ServerBinding] =
    Resource({
      logger.info(s"Starting JSON API server, ${lc.makeString}")

      import startSettings.*
      val DummyUserId: UserId = UserId("HTTP-JSON-API-Gateway")

      val settings: ServerSettings = ServerSettings(asys)
        .withTransparentHeadRequests(true)
        .mapTimeouts(_.withRequestTimeout(startSettings.server.requestTimeout))

      implicit val wsConfig = startSettings.websocketConfig.getOrElse(WebsocketConfig())

      val clientConfig = LedgerClientConfiguration(
        userId = UserId.unwrap(DummyUserId),
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
          ledgerHealthService = HealthGrpc.stub(channel)

          healthService = new HealthService(() => ledgerHealthService.check(HealthCheckRequest()))

          _ = metrics.health.registerHealthGauge(
            HttpApiMetrics.ComponentName,
            () => healthService.ready().map(_.checks.forall(_.result)),
          )

          v2Routes = V2Routes(
            ledgerClient,
            metadataServiceEnabled = startSettings.damlDefinitionsServiceEnabled,
            packageSyncService,
            packagePreferenceBackend,
            mat.executionContext,
            loggerFactory,
          )

          v1Routes = V1Routes(
            ledgerClient,
            httpsConfiguration.isEmpty,
            HttpService.decodeJwt,
            debugLoggingOfHttpBodies,
            resolveUser,
            ledgerClient.userManagementClient,
            loggerFactory,
            websocketConfig,
          )

          jsonEndpoints = new Endpoints(
            healthService,
            v2Routes,
            v1Routes,
            debugLoggingOfHttpBodies,
            loggerFactory,
          )

          rateDurationSizeMetrics = HttpMetricsInterceptor.rateDurationSizeMetrics(metrics.http)

          defaultEndpoints =
            rateDurationSizeMetrics apply jsonEndpoints.all

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
                serverBuilder.enableHttps(HttpService.httpsConnectionContext(config)(logger))
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
  // if no minimumServerProtocolVersion is set `config.protocols` returns an empty list
  // but we still want to setup some protocols
  private val allowedProtocols =
    Set[TlsVersion.TlsVersion](TlsVersion.V1_2, TlsVersion.V1_3).map(_.version)

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
    jwt =>
      \/.fromEither(
        JwtDecoder.decode(jwt).leftMap(e => EndpointsCompanion.Unauthorized(e.prettyPrint))
      )

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
