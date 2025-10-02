// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.jwt.{
  AuthServiceJWTCodec,
  DecodedJwt,
  JwtSigner,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, PositiveInt}
import com.digitalasset.canton.config.{AuthServiceConfig, PemFile, TlsServerConfig}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.UseTls
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.integration.tests.ledgerapi.submission.BaseInteractiveSubmissionTest.ParticipantSelector
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.client.LedgerClient as DamlLedgerClient
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.JarResourceUtils
import monocle.macros.syntax.lens.*
import org.apache.pekko.http.scaladsl.model.Uri
import org.scalatest.time.{Millis, Seconds, Span}

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}

import AuthServiceConfig.Wildcard

trait HttpJsonApiTestBase extends CantonFixture {
  protected def packageFiles: List[java.io.File] = List()
  protected def authSecret: Option[String] = None
  def wsConfig: Option[WebsocketConfig] = None
  def useTls: UseTls = UseTls.NoTls
  val maxPartiesPageSize = PositiveInt.tryCreate(100)

  implicit override val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(120, Seconds)), interval = scaled(Span(150, Millis)))

  lazy protected val darFiles: Seq[Path] = packageFiles.map(_.toPath)

  lazy private val certChainFilePath =
    JarResourceUtils.resourceFile("test-certificates/server.crt")
  lazy private val privateKeyFilePath =
    JarResourceUtils.resourceFile("test-certificates/server.pem")
  lazy private val trustCertCollectionFilePath =
    JarResourceUtils.resourceFile("test-certificates/ca.crt")
  lazy protected val tls = TlsServerConfig(
    certChainFile = PemFile(ExistingFile.tryCreate(certChainFilePath)),
    privateKeyFile = PemFile(ExistingFile.tryCreate(privateKeyFilePath)),
    trustCollectionFile = Some(PemFile(ExistingFile.tryCreate(trustCertCollectionFilePath))),
    minimumServerProtocolVersion = None,
  )

  var validSynchronizerId: SynchronizerId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(ConfigTransforms.useStaticTime)
      .prependConfigTransform(
        ConfigTransforms.enableHttpLedgerApi("participant1", wsConfig)
      )
      .addConfigTransforms(
        ConfigTransforms.updateParticipantConfig("participant1")(
          ConfigTransforms.useTestingTimeService
        ),
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.ledgerApi.authServices).modify(
            _ ++ authSecret
              .map(secret =>
                AuthServiceConfig.UnsafeJwtHmac256(
                  NonEmptyString.tryCreate(secret),
                  None,
                  None,
                )
              )
              .toList
          )
        ),
        ConfigTransforms
          .updateParticipantConfig("participant1")(participantConfig =>
            if (useTls)
              participantConfig
                .focus(_.ledgerApi.tls)
                .replace(Some(tls))
                .focus(_.ledgerApi.authServices)
                .replace(Seq(Wildcard))
            else participantConfig
          ),
        ConfigTransforms
          .updateParticipantConfig("participant1")(participantConfig =>
            participantConfig
              .focus(_.ledgerApi.partyManagementService.maxPartiesPageSize)
              .replace(maxPartiesPageSize)
          ),
      )
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        validSynchronizerId = daId

        createChannel(participant1)
        darFiles.foreach(path => participant1.dars.upload(path.toFile.getAbsolutePath))
      }

  def adHocHttp(participantSelector: ParticipantSelector, token: Option[String] = None)(implicit
      env: TestConsoleEnvironment
  ): Future[AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData] = {
    implicit val esf = env.executionSequencerFactory
    implicit val ec = env.executionContext
    val participant = participantSelector(env)
    import com.digitalasset.canton.ledger.client.configuration.*
    val jsonApiPort = participant.config.httpLedgerApi.server.internalPort
      .valueOrFail("port must be configured")

    val userId = getClass.getName
    val client = DamlLedgerClient.withoutToken(
      channel = channel,
      config = LedgerClientConfiguration(
        userId = token.fold(userId)(_ => ""),
        commandClient = CommandClientConfiguration.default,
        token = () => token,
      ),
      loggerFactory,
    )

    val scheme = if (useTls) "https" else "http"
    val uri = Uri.from(scheme = scheme, host = "localhost", port = jsonApiPort.unwrap)
    Future.successful(
      AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData(
        uri,
        client,
      )
    )
  }

  def usingLedger[A](token: Option[String] = None)(
      testFn: (Int, DamlLedgerClient) => A
  )(fixtureParam: FixtureParam)(implicit
      ec: ExecutionContext,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): A = {
    import com.digitalasset.canton.ledger.client.configuration.*
    val jsonApiPort = fixtureParam.participant1.config.httpLedgerApi.server.internalPort
      .valueOrFail("port must be configured")

    val userId = getClass.getName
    val client = DamlLedgerClient.withoutToken(
      channel = channel,
      config = LedgerClientConfiguration(
        userId = token.fold(userId)(_ => ""),
        commandClient = CommandClientConfiguration.default,
        token = () => token,
      ),
      loggerFactory,
    )

    testFn(jsonApiPort.unwrap, client)
  }

  def usingParticipantLedger[A](
      token: Option[String] = None,
      participantSelector: FixtureParam => LocalParticipantReference = _.participant1,
  )(
      testFn: (Int, DamlLedgerClient) => A
  )(fixtureParam: FixtureParam)(implicit
      ec: ExecutionContext,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): A = {
    import com.digitalasset.canton.ledger.client.configuration.*
    val participant = participantSelector(fixtureParam)
    val jsonApiPort = participant.config.httpLedgerApi.server.internalPort
      .valueOrFail("port must be configured")

    val userId = getClass.getName
    val client = DamlLedgerClient.withoutToken(
      channel = channel,
      config = LedgerClientConfiguration(
        userId = token.fold(userId)(_ => ""),
        commandClient = CommandClientConfiguration.default,
        token = () => token,
      ),
      loggerFactory,
    )

    testFn(jsonApiPort.unwrap, client)
  }

  protected def getToken(
      userId: String,
      authSecret: Option[String] = None,
  ): Option[String] = authSecret.map { secret =>
    val payload = StandardJWTPayload(
      issuer = None,
      userId = userId,
      participantId = None,
      exp = None,
      format = StandardJWTTokenFormat.Scope,
      audiences = List.empty,
      scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
    )
    val header = """{"alg": "HS256", "typ": "JWT"}"""
    val jwt =
      DecodedJwt[String](header, AuthServiceJWTCodec.writePayload(payload).compactPrint)
    JwtSigner.HMAC256.sign(jwt, secret) match {
      case Right(a) => a.value
      case Left(e) => throw new IllegalStateException(e.toString)
    }
  }
}
