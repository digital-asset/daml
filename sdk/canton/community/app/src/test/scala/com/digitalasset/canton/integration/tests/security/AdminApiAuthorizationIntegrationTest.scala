// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.data.EitherT
import cats.syntax.functor.*
import com.daml.jwt.JwtTimestampLeeway
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authenticity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.auth.{AuthorizationChecksErrors, CantonAdminToken}
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
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule, TracedLogger}
import com.digitalasset.canton.networking.grpc.{
  CantonGrpcUtil,
  ClientChannelBuilder,
  GrpcError,
  ManagedChannelBuilderProxy,
}
import com.digitalasset.canton.participant.config.{ParticipantNodeConfig, RemoteParticipantConfig}
import com.digitalasset.canton.participant.ledger.api.JwtTokenUtilities
import com.digitalasset.canton.synchronizer.mediator.{MediatorNodeConfig, RemoteMediatorConfig}
import com.digitalasset.canton.synchronizer.sequencer.config.{
  RemoteSequencerConfig,
  SequencerNodeConfig,
}
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level.WARN

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
trait AdminApiAuthorizationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite {

  private val mySecret = NonEmptyString.tryCreate("pyjama")
  private val scope = "long-johns"
  private val adminToken = CantonAdminToken.create(new SymbolicPureCrypto())
  private val expirationLeeway: Long = 3600

  lazy private val securityAsset: SecurityTest =
    SecurityTest(property = Authenticity, asset = "admin api client")

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

  def createRemoteConfigForMediator(
      name: String,
      localToRemote: MediatorNodeConfig => List[(String, RemoteMediatorConfig)],
  ): ConfigTransform =
    cantonConfig =>
      cantonConfig.mediators
        .collectFirst {
          case (mName, mValue) if mName.unwrap == name => mValue
        }
        .fold(cantonConfig)(mValue =>
          cantonConfig
            .focus(_.remoteMediators)
            .replace(cantonConfig.remoteMediators ++ localToRemote(mValue).map {
              case (remoteName, remoteConfig) =>
                InstanceName.tryCreate(remoteName) -> remoteConfig
            })
        )

  def remoteMediatorWithToken(
      localConfig: MediatorNodeConfig,
      token: Option[String],
  ): RemoteMediatorConfig =
    RemoteMediatorConfig(
      adminApi = localConfig.adminApi.clientConfig,
      token = token,
    )

  def createRemoteConfigForSequencer(
      name: String,
      localToRemote: SequencerNodeConfig => List[(String, RemoteSequencerConfig)],
  ): ConfigTransform =
    cantonConfig =>
      cantonConfig.sequencers
        .collectFirst {
          case (sName, sValue) if sName.unwrap == name => sValue
        }
        .fold(cantonConfig)(sValue =>
          cantonConfig
            .focus(_.remoteSequencers)
            .replace(cantonConfig.remoteSequencers ++ localToRemote(sValue).map {
              case (remoteName, remoteConfig) =>
                InstanceName.tryCreate(remoteName) -> remoteConfig
            })
        )

  def remoteSequencerWithToken(
      localConfig: SequencerNodeConfig,
      token: Option[String],
  ): RemoteSequencerConfig =
    RemoteSequencerConfig(
      adminApi = localConfig.adminApi.clientConfig,
      publicApi = localConfig.publicApi.clientConfig,
      token = token,
    )

  def testRemote[T <: RemoteInstanceReference](remote: T): Assertion =
    remote.keys.public.list().size should be > 0

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      // enable only admin-token jwt authorization on admin-api of p1
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant1") { config =>
        config
          .focus(_.adminApi.adminTokenConfig.fixedAdminToken)
          .replace(Some(adminToken.secret))
          .focus(_.adminApi.adminTokenConfig.adminClaim)
          .replace(true)
      })
      // enable privileged jwt authorization on admin-api of p2
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant2") { config =>
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
                  privileged = true,
                )
            )
          )
          .focus(_.adminApi.adminTokenConfig.adminClaim)
          .replace(true)
      })
      // enable wildcard jwt authorization on admin-api of p3
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant3") { config =>
        config
          .focus(_.adminApi.authServices)
          .replace(Seq(Wildcard))
          .focus(_.adminApi.adminTokenConfig.adminClaim)
          .replace(true)
      })
      // enable the unsupported user-based jwt authorization on admin-api of p4
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant4") { config =>
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
                )
            )
          )
          .focus(_.adminApi.adminTokenConfig.adminClaim)
          .replace(true)
      })
      // enable privileged jwt authorization on admin-api of sequencer1
      .addConfigTransform(ConfigTransforms.updateSequencerConfig("sequencer1") { config =>
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
                  privileged = true,
                )
            )
          )
          .focus(_.adminApi.adminTokenConfig.adminClaim)
          .replace(true)
      })
      // enable privileged jwt authorization on admin-api of mediator1 (admin-token needed for start-up)
      .addConfigTransform(ConfigTransforms.updateMediatorConfig("mediator1") { config =>
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
                  privileged = true,
                )
            )
          )
          .focus(_.adminApi.adminTokenConfig.adminClaim)
          .replace(true)
          .focus(
            _.adminApi.jwtTimestampLeeway
          )
          .replace(Some(JwtTimestampLeeway(expiresAt = Some(expirationLeeway))))
      })
      // Only successful configurations are possible for remote nodes
      // An invalid one produces a failure already at a start-up stage
      // participant1 - adminToken only
      .addConfigTransform {
        createRemoteConfigForParticipant(
          "participant1",
          localConfig =>
            List(
              "p1-adminToken" ->
                remoteParticipantWithToken(
                  localConfig,
                  localConfig.adminApi.adminTokenConfig.fixedAdminToken,
                ),
              "p1-noToken" ->
                remoteParticipantWithToken(localConfig, None),
            ),
        )
      }
      // participant2 - adminToken and privileged
      .addConfigTransform {
        createRemoteConfigForParticipant(
          "participant2",
          localConfig =>
            List(
              "p2-adminToken" ->
                remoteParticipantWithToken(
                  localConfig,
                  localConfig.adminApi.adminTokenConfig.fixedAdminToken,
                ),
              "p2-privileged" ->
                remoteParticipantWithToken(
                  localConfig,
                  Some(
                    JwtTokenUtilities.buildUnsafeToken(
                      mySecret.unwrap,
                      scope = Some(scope),
                    )
                  ),
                ),
            ),
        )
      }
      // participant3 - adminToken and wildcard
      .addConfigTransform {
        createRemoteConfigForParticipant(
          "participant3",
          localConfig =>
            List(
              "p3-adminToken" ->
                remoteParticipantWithToken(
                  localConfig,
                  localConfig.adminApi.adminTokenConfig.fixedAdminToken,
                ),
              "p3-privileged" ->
                remoteParticipantWithToken(
                  localConfig,
                  Some(
                    JwtTokenUtilities.buildUnsafeToken(
                      mySecret.unwrap,
                      scope = Some(scope),
                    )
                  ),
                ),
              "p3-noToken" ->
                remoteParticipantWithToken(localConfig, None),
            ),
        )
      }
      // participant4 - adminToken and an invalid user token config
      .addConfigTransform {
        createRemoteConfigForParticipant(
          "participant4",
          localConfig =>
            List(
              "p4-adminToken" ->
                remoteParticipantWithToken(
                  localConfig,
                  localConfig.adminApi.adminTokenConfig.fixedAdminToken,
                )
            ),
        )
      }
      .addConfigTransform {
        createRemoteConfigForMediator(
          "mediator1",
          localConfig =>
            List(
              "m1-adminToken" ->
                remoteMediatorWithToken(
                  localConfig,
                  localConfig.adminApi.adminTokenConfig.fixedAdminToken,
                ),
              "m1-privileged" ->
                remoteMediatorWithToken(
                  localConfig,
                  Some(
                    JwtTokenUtilities.buildUnsafeToken(
                      mySecret.unwrap,
                      scope = Some(scope),
                    )
                  ),
                ),
            ),
        )
      }
      .addConfigTransform {
        createRemoteConfigForSequencer(
          "sequencer1",
          localConfig =>
            List(
              "s1-adminToken" ->
                remoteSequencerWithToken(
                  localConfig,
                  localConfig.adminApi.adminTokenConfig.fixedAdminToken,
                ),
              "s1-privileged" ->
                remoteSequencerWithToken(
                  localConfig,
                  Some(
                    JwtTokenUtilities.buildUnsafeToken(
                      mySecret.unwrap,
                      scope = Some(scope),
                    )
                  ),
                ),
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
            "p1-adminToken",
            "p1-noToken",
            "p2-adminToken",
            "p2-privileged",
            "p3-adminToken",
            "p3-privileged",
            "p3-noToken",
            "p4-adminToken",
          )
        )
      )

  "A participant" when {
    "the admin-token is configured as a backup" can {
      "connect to the synchronizer" taggedAs securityAsset.setHappyCase(
        "use a correct admin token"
      ) in { implicit env =>
        import env.*

        Seq(participant1, participant2, participant3, participant4).foreach {
          _.synchronizers.connect_local(sequencer1, alias = daName)
        }
      }
    }

    "admin-token is the only configured option" can {
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

        testRemote(rp("p1-adminToken"))
      }

      "use admin-api without any token" taggedAs securityAsset.setHappyCase(
        "call without any token"
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
          None,
        ).futureValue shouldBe ()

        testRemote(rp("p1-noToken"))
      }
    }

    "privileged authorization is configured" can {
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

        testRemote(rp("p2-adminToken"))
      }

      "use admin-api with privileged token" taggedAs securityAsset.setHappyCase(
        "use privileged token"
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
              scope = Some(scope),
            )
          ),
        ).futureValue shouldBe ()

        testRemote(rp("p2-privileged"))
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
          participant2.config.adminApi.clientConfig
        )

        assertUnauthenticated(
          checkCantonApiInfo(
            "participant2",
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

        testRemote(rp("p3-adminToken"))
      }

      "use admin-api with privileged token" taggedAs securityAsset.setHappyCase(
        "use privileged token"
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
              scope = Some(scope),
            )
          ),
        ).futureValue shouldBe ()

        testRemote(rp("p3-privileged"))
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

        testRemote(rp("p3-noToken"))
      }
    }

    "user authorization is configured" can {
      "use admin-api with an admin-token" taggedAs securityAsset.setHappyCase(
        "use an admin token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant4.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "participant4",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          Some(adminToken.secret),
        ).futureValue shouldBe ()

        testRemote(rp("p4-adminToken"))
      }

      "don't allow using admin-api with privileged token" taggedAs securityAsset.setAttack(
        Attack(
          "Admin api client",
          "attempts to access with user-based auth token",
          "refuse access to the service with a failure",
        )
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          participant4.config.adminApi.clientConfig
        )

        assertUnauthenticated(
          checkCantonApiInfo(
            "participant4",
            "admin-api",
            channelBuilder,
            logger,
            timeouts.network,
            Some(
              JwtTokenUtilities.buildUnsafeToken(
                mySecret.unwrap,
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
          participant4.config.adminApi.clientConfig
        )

        assertUnauthenticated(
          checkCantonApiInfo(
            "participant4",
            "admin-api",
            channelBuilder,
            logger,
            timeouts.network,
            None,
          )
        )
      }
    }
  }

  "A sequencer" when {
    "privileged authorization is configured" can {
      "use admin-api with an admin-token" taggedAs securityAsset.setHappyCase(
        "use an admin token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          sequencer1.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "sequencer1",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          Some(adminToken.secret),
        ).futureValue shouldBe ()

        testRemote(rs("s1-adminToken"))
      }

      "use admin-api with privileged token" taggedAs securityAsset.setHappyCase(
        "use privileged token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          sequencer1.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "sequencer1",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          Some(
            JwtTokenUtilities.buildUnsafeToken(
              mySecret.unwrap,
              scope = Some(scope),
            )
          ),
        ).futureValue shouldBe ()

        testRemote(rs("s1-privileged"))
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
          sequencer1.config.adminApi.clientConfig
        )

        assertUnauthenticated(
          checkCantonApiInfo(
            "sequencer1",
            "admin-api",
            channelBuilder,
            logger,
            timeouts.network,
            None,
          )
        )

      }
    }
  }

  "A mediator" when {
    "privileged authorization is configured" can {
      "use admin-api with an admin-token" taggedAs securityAsset.setHappyCase(
        "use an admin token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          mediator1.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "mediator1",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          Some(adminToken.secret),
        ).futureValue shouldBe ()

        testRemote(rm("m1-adminToken"))
      }

      "use admin-api with privileged token" taggedAs securityAsset.setHappyCase(
        "use privileged token"
      ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          mediator1.config.adminApi.clientConfig
        )

        checkCantonApiInfo(
          "mediator1",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          Some(
            JwtTokenUtilities.buildUnsafeToken(
              mySecret.unwrap,
              scope = Some(scope),
            )
          ),
        ).futureValue shouldBe ()

        testRemote(rm("m1-privileged"))
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
          mediator1.config.adminApi.clientConfig
        )

        assertUnauthenticated(
          checkCantonApiInfo(
            "mediator1",
            "admin-api",
            channelBuilder,
            logger,
            timeouts.network,
            None,
          )
        )

      }
    }
    "leeway parameters are configured" can {
      "use admin-api with an expired privileged token within the leeway margin" taggedAs securityAsset
        .setHappyCase(
          "use expired privileged token within the leeway"
        ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          mediator1.config.adminApi.clientConfig
        )

        val timeIntoLeeway = Instant.now.minus(Duration.ofSeconds((0.1 * expirationLeeway).toLong))

        checkCantonApiInfo(
          "mediator1",
          "admin-api",
          channelBuilder,
          logger,
          timeouts.network,
          Some(
            JwtTokenUtilities.buildUnsafeToken(
              mySecret.unwrap,
              scope = Some(scope),
              exp = Some(timeIntoLeeway),
            )
          ),
        ).futureValue shouldBe ()
      }

      "don't allow using admin-api with an expired privileged token outside of the leeway margin" taggedAs securityAsset
        .setAttack(
          Attack(
            "Admin api client",
            "provides an expired auth token",
            "refuse access to the service with a failure",
          )
        ) in { implicit env =>
        import env.*

        val channelBuilder = ClientChannelBuilder.createChannelBuilderToTrustedServer(
          mediator1.config.adminApi.clientConfig
        )

        val timeOutOfLeeway = Instant.now.minus(Duration.ofSeconds(2 * expirationLeeway))
        val regExp = """UNAUTHENTICATED\(6,0\): The command is missing a \(valid\) JWT token""".r

        assertLogs(
          checkCantonApiInfo(
            "mediator1",
            "admin-api",
            channelBuilder,
            logger,
            timeouts.network,
            Some(
              JwtTokenUtilities.buildUnsafeToken(
                mySecret.unwrap,
                scope = Some(scope),
                exp = Some(timeOutOfLeeway),
              )
            ),
          ),
          _.warningMessage should include("The Token has expired"),
          _.warningMessage should include regex regExp,
          _.warningMessage should include regex regExp,
        )
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

  def assertLogs(
      f: => EitherT[Future, GrpcError, Unit],
      assertions: (LogEntry => Assertion)*
  ): Assertion =
    loggerFactory.assertLogs(SuppressionRule.LevelAndAbove(WARN))(
      inside(f.value.futureValue) {
        case Left(GrpcError.GrpcClientError(_, _, status, _, _)) =>
          status.getCode shouldBe AuthorizationChecksErrors.Unauthenticated
            .MissingJwtToken()
            .asGrpcError
            .getStatus
            .getCode
        case Left(_) => fail("unexpected error")
      },
      assertions *,
    )

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
class AdminApiAuthorizationIntegrationTestDefault extends AdminApiAuthorizationIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class AdminApiAuthorizationIntegrationTestPostgres extends AdminApiAuthorizationIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
