// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.data.EitherT
import cats.syntax.functor.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authenticity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.auth.{AuthorizationChecksErrors, AuthorizedUser, CantonAdminToken}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.AuthServiceConfig
import com.digitalasset.canton.config.AuthServiceConfig.Wildcard
import com.digitalasset.canton.config.CantonRequireTypes.{InstanceName, NonEmptyString}
import com.digitalasset.canton.connection.v30.{ApiInfoServiceGrpc, GetApiInfoRequest}
import com.digitalasset.canton.console.RemoteInstanceReference
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{SuppressionRule, TracedLogger}
import com.digitalasset.canton.networking.grpc.{
  CantonGrpcUtil,
  ClientChannelBuilder,
  GrpcError,
  ManagedChannelBuilderProxy,
}
import com.digitalasset.canton.participant.config.{ParticipantNodeConfig, RemoteParticipantConfig}
import com.digitalasset.canton.participant.ledger.api.JwtTokenUtilities
import com.digitalasset.canton.topology.admin.v30.{
  ListPartiesRequest,
  TopologyAggregationServiceGrpc,
}
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level.WARN

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait AdminApiUserConfigAuthorizationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite {

  private val mySecret = NonEmptyString.tryCreate("pyjama")
  private val scope = "long-johns"
  private val adminToken = CantonAdminToken.create(new SymbolicPureCrypto())

  lazy private val securityAsset: SecurityTest =
    SecurityTest(property = Authenticity, asset = "admin api client")

  protected def randomUserId(): String = UUID.randomUUID().toString
  private val configuredUser = randomUserId()
  private val otherUser = randomUserId()

  def createRemoteConfigForParticipant(
      name: String,
      localToRemote: ParticipantNodeConfig => List[(String, RemoteParticipantConfig)],
  ): ConfigTransform =
    cantonConfig =>
      cantonConfig.participants
        .collectFirst {
          case (pName, pValue) if pName.unwrap == name => pValue
        }
        .fold(cantonConfig)(pValue =>
          cantonConfig
            .focus(_.remoteParticipants)
            .replace(cantonConfig.remoteParticipants ++ localToRemote(pValue).map {
              case (remoteName, remoteConfig) =>
                InstanceName.tryCreate(remoteName) -> remoteConfig
            })
        )

  def remoteParticipantWithToken(
      localConfig: ParticipantNodeConfig,
      token: Option[String],
  ): RemoteParticipantConfig =
    RemoteParticipantConfig(
      ledgerApi = localConfig.ledgerApi.clientConfig,
      adminApi = localConfig.adminApi.clientConfig,
      token = token,
    )

  def testRemote[T <: RemoteInstanceReference](remote: T): Assertion =
    remote.keys.public.list().size should be > 0

  private val authorizedUser = AuthorizedUser(
    userId = configuredUser,
    allowedServices = List(
      "com.digitalasset.canton.connection.v30.ApiInfoService",
      "com.digitalasset.canton.crypto.admin.v30.VaultService",
    ),
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      // enable user-config jwt authorization on admin-api of p1 (admin-token needed for start-up)
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant1") { config =>
        config
          .focus(_.adminApi.adminTokenConfig.fixedAdminToken)
          .replace(Some(adminToken.secret))
          .focus(_.adminApi.authServices)
          .replace(
            Seq(
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = mySecret,
                  targetAudience = None,
                  targetScope = Some(scope),
                  users = List(authorizedUser),
                )
            )
          )
          .focus(_.adminApi.adminTokenConfig.adminClaim)
          .replace(true)
      })
      // enable wildcard and user-config jwt authorization on admin-api of p2 (admin-token needed for start-up)
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant2") { config =>
        config
          .focus(_.adminApi.adminTokenConfig.fixedAdminToken)
          .replace(Some(adminToken.secret))
          .focus(_.adminApi.authServices)
          .replace(
            Seq[AuthServiceConfig](
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = mySecret,
                  targetAudience = None,
                  targetScope = Some(scope),
                  users = List(authorizedUser),
                ),
              Wildcard,
            )
          )
          .focus(_.adminApi.adminTokenConfig.adminClaim)
          .replace(true)
      })
      // Only successful configurations are possible for remote nodes
      // An invalid one produces a failure already at a start-up stage
      // participant1 - adminToken only
      .addConfigTransform {
        createRemoteConfigForParticipant(
          "participant1",
          localConfig =>
            List(
              "p1-admin-token" ->
                remoteParticipantWithToken(
                  localConfig,
                  localConfig.adminApi.adminTokenConfig.fixedAdminToken,
                ),
              "p1-user-config" ->
                remoteParticipantWithToken(
                  localConfig,
                  Some(
                    JwtTokenUtilities.buildUnsafeToken(
                      mySecret.unwrap,
                      userId = Some(configuredUser),
                      scope = Some(scope),
                    )
                  ),
                ),
            ),
        )
      }
      // participant2 - adminToken and user-config
      .addConfigTransform {
        createRemoteConfigForParticipant(
          "participant2",
          localConfig =>
            List(
              "p2-admin-token" ->
                remoteParticipantWithToken(
                  localConfig,
                  localConfig.adminApi.adminTokenConfig.fixedAdminToken,
                ),
              "p2-user-config" ->
                remoteParticipantWithToken(
                  localConfig,
                  Some(
                    JwtTokenUtilities.buildUnsafeToken(
                      mySecret.unwrap,
                      scope = Some(scope),
                    )
                  ),
                ),
              "p2-no-token" ->
                remoteParticipantWithToken(localConfig, None),
            ),
        )
      }
      // participant3 - no authorization configured
      .addConfigTransform {
        createRemoteConfigForParticipant(
          "participant3",
          localConfig =>
            List(
              "p3-admin-token" ->
                remoteParticipantWithToken(
                  localConfig,
                  localConfig.adminApi.adminTokenConfig.fixedAdminToken,
                ),
              "p3-user-config" ->
                remoteParticipantWithToken(
                  localConfig,
                  Some(
                    JwtTokenUtilities.buildUnsafeToken(
                      mySecret.unwrap,
                      scope = Some(scope),
                    )
                  ),
                ),
              "p3-no-token" ->
                remoteParticipantWithToken(localConfig, None),
            ),
        )
      }
      .withSetup { implicit env =>
        import env.*
        // Make ssl errors visible in the log.
        logging.set_level("io.grpc.netty.shaded.io.netty", "DEBUG")
      }
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(
          Set(
            // excluding local aliases
            "p1-admin-token",
            "p1-user-config",
            "p2-admin-token",
            "p2-user-config",
            "p2-no-token",
            "p3-admin-token",
            "p3-user-config",
            "p3-no-token",
          )
        )
      )

  "A participant" when {
    "the admin-token is configured as a backup" can {
      "connect to the domain" taggedAs securityAsset.setHappyCase(
        "use a correct admin token"
      ) in { implicit env =>
        import env.*

        Seq(participant1, participant2, participant3).foreach {
          _.synchronizers.connect_local(sequencer1, alias = daName)
        }
      }
    }

    "user-config authorization is configured" can {
      "use admin-api with an admin-token" taggedAs securityAsset.setHappyCase(
        "use an admin token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant1.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "participant1",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          Some(adminToken.secret),
        ).futureValue shouldBe ()

        testRemote(rp("p1-admin-token"))
      }

      "use admin-api with user-config token" taggedAs securityAsset.setHappyCase(
        "use user-config token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant1.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "participant1",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          Some(
            JwtTokenUtilities.buildUnsafeToken(
              mySecret.unwrap,
              userId = Some(configuredUser),
              scope = Some(scope),
            )
          ),
        ).futureValue shouldBe ()

        testRemote(rp("p1-user-config"))
      }

      "don't allow using admin-api with token for unknown user" taggedAs securityAsset.setAttack(
        Attack(
          "Admin api client",
          "provides an auth token for unknown user",
          "refuse access to the service with a failure",
        )
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant1.config.adminApi.clientConfig
        )

        assertUnauthenticated(
          checkCantonApiInfo(
            "participant1",
            "admin-api",
            channelBuilder,
            logger,
            timeouts.network,
            Some(
              JwtTokenUtilities.buildUnsafeToken(
                secret = mySecret.unwrap,
                userId = Some(otherUser),
                scope = Some(scope),
              )
            ),
          )
        )
      }

      "don't allow using admin-api with user-configured token for forbidden service" taggedAs securityAsset
        .setAttack(
          Attack(
            "Admin api client",
            "provides an auth token for configured user for accessing a forbidden service",
            "refuse access to the service with a failure",
          )
        ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant1.config.adminApi.clientConfig
        )

        assertUnauthenticated(
          checkListParties(
            "participant1",
            "admin-api",
            channelBuilder,
            logger,
            timeouts.network,
            Some(
              JwtTokenUtilities.buildUnsafeToken(
                mySecret.unwrap,
                userId = Some(configuredUser),
                scope = Some(scope),
              )
            ),
          )
        )
      }

      "don't allow using admin-api without any token" taggedAs securityAsset.setAttack(
        Attack(
          "Admin api client",
          "does not provide an auth token",
          "refuse access to the service with a failure",
        )
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant1.config.adminApi.clientConfig
        )

        assertUnauthenticated(
          checkCantonApiInfo(
            "participant1",
            "admin-api",
            channelBuilder,
            logger,
            timeouts.network,
            None,
          )
        )
      }
    }

    "wildcard authorization is configured" can {
      "use admin-api with an admin-token" taggedAs securityAsset.setHappyCase(
        "use an admin token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant2.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "participant2",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          Some(adminToken.secret),
        ).futureValue shouldBe ()

        testRemote(rp("p2-admin-token"))
      }

      "use admin-api with user-config token" taggedAs securityAsset.setHappyCase(
        "use user-config token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant2.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "participant2",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          Some(
            JwtTokenUtilities.buildUnsafeToken(
              mySecret.unwrap,
              userId = Some(configuredUser),
              scope = Some(scope),
            )
          ),
        ).futureValue shouldBe ()

        testRemote(rp("p2-user-config"))
      }

      "use admin-api without any token" taggedAs securityAsset.setHappyCase(
        "call without any token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant2.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "participant2",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          None,
        ).futureValue shouldBe ()

        testRemote(rp("p2-no-token"))
      }
    }

    "no authorization is configured" can {
      "use admin-api with an admin-token" taggedAs securityAsset.setHappyCase(
        "use an admin token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant3.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "participant3",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          Some(adminToken.secret),
        ).futureValue shouldBe ()

        testRemote(rp("p3-admin-token"))
      }

      "use admin-api with user-config token" taggedAs securityAsset.setHappyCase(
        "use user-config token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant3.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "participant3",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          Some(
            JwtTokenUtilities.buildUnsafeToken(
              mySecret.unwrap,
              userId = Some(configuredUser),
              scope = Some(scope),
            )
          ),
        ).futureValue shouldBe ()

        testRemote(rp("p3-user-config"))
      }

      "use admin-api without any token" taggedAs securityAsset.setHappyCase(
        "call without any token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant3.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "participant3",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          None,
        ).futureValue shouldBe ()

        testRemote(rp("p3-no-token"))
      }
    }
  }

  def checkCantonApiInfo(
      serverName: String,
      expectedName: String,
      channelBuilder: ManagedChannelBuilderProxy,
      logger: TracedLogger,
      timeout: config.NonNegativeDuration,
      token: Option[String],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, GrpcError, Unit] =
    CantonGrpcUtil
      .sendSingleGrpcRequest(
        serverName = s"$serverName/$expectedName",
        requestDescription = "GetApiInfo",
        channelBuilder = channelBuilder,
        stubFactory = ApiInfoServiceGrpc.stub,
        timeout = timeout.unwrap,
        logger = logger,
        logPolicy = CantonGrpcUtil.SilentLogPolicy,
        hasRunOnClosing = this,
        retryPolicy = CantonGrpcUtil.RetryPolicy.noRetry,
        token = token,
      )(_.getApiInfo(GetApiInfoRequest()))
      .failOnShutdown
      .void

  def checkListParties(
      serverName: String,
      expectedName: String,
      channelBuilder: ManagedChannelBuilderProxy,
      logger: TracedLogger,
      timeout: config.NonNegativeDuration,
      token: Option[String],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, GrpcError, Unit] =
    CantonGrpcUtil
      .sendSingleGrpcRequest(
        serverName = s"$serverName/$expectedName",
        requestDescription = "ListParties",
        channelBuilder = channelBuilder,
        stubFactory = TopologyAggregationServiceGrpc.stub,
        timeout = timeout.unwrap,
        logger = logger,
        logPolicy = CantonGrpcUtil.SilentLogPolicy,
        hasRunOnClosing = this,
        retryPolicy = CantonGrpcUtil.RetryPolicy.noRetry,
        token = token,
      )(
        _.listParties(
          ListPartiesRequest(
            synchronizerIds = List(),
            filterParty = "",
            filterParticipant = "",
            asOf = None,
            limit = 1,
          )
        )
      )
      .failOnShutdown
      .void

  def assertUnauthenticated(f: => EitherT[Future, GrpcError, Unit]): Assertion =
    loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(WARN))(
      inside(f.value.futureValue) {
        case Left(GrpcError.GrpcClientError(_, _, status, _, _)) =>
          status.getCode shouldBe AuthorizationChecksErrors.Unauthenticated
            .MissingJwtToken()
            .asGrpcError
            .getStatus
            .getCode
        case Left(_) => fail("unexpected error")
      },
      entries => {
        assert(entries.nonEmpty)
        forEvery(entries)(
          _.warningMessage should include("UNAUTHENTICATED")
        )
      },
    )
}

// Don't run in-memory because this may fail due to stale reads in H2.
class AdminApiUserConfigAuthorizationIntegrationTestDefault
    extends AdminApiUserConfigAuthorizationIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

}

class AdminApiUserConfigAuthorizationIntegrationTestPostgres
    extends AdminApiUserConfigAuthorizationIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

}
