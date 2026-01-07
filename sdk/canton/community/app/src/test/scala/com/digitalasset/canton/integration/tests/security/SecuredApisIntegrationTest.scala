// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.jwt.JwtTimestampLeeway
import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.*
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.{InstanceName, NonEmptyString}
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.console.{
  CommandFailure,
  ExternalLedgerApiClient,
  ParticipantReference,
  RemoteParticipantReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.participant.config.RemoteParticipantConfig
import com.digitalasset.canton.participant.ledger.api.JwtTokenUtilities
import monocle.macros.syntax.lens.*

import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters.*

/** Test various TLS and JWT settings
  *
  * Scenario:
  *   - sequencer1 uses TLS by default
  *   - participant1 uses JWT with TLS server side authentication on admin and ledger api
  *   - participant2 uses TLS with mutual authentication on admin and ledger-api
  *     - valid2 succeeds to connect using root-ca as trust anchor
  *   - participant3 uses TLS server side authentication
  *     - but remote fail3 client has invalid certificate
  */
trait SecuredApisIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite
    with AccessTestScenario {

  private val mySecret = NonEmptyString.tryCreate("pyjama")

  private val expirationLeeway: Long = 3600

  private def copyWith(
      rp: RemoteParticipantConfig,
      secret: String,
      expiration: Option[Instant] = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
  ): RemoteParticipantConfig =
    rp.copy(token =
      Some(
        JwtTokenUtilities.buildUnsafeToken(
          secret,
          Some(UserManagementStore.DefaultParticipantAdminUserId),
          exp = expiration,
        )
      )
    )

  private def generateTLSServerConfig(name: String): TlsServerConfig =
    TlsServerConfig(
      certChainFile =
        PemFile(ExistingFile.tryCreate(s"./community/app/src/test/resources/tls/$name.crt")),
      privateKeyFile =
        PemFile(ExistingFile.tryCreate(s"./community/app/src/test/resources/tls/$name.pem")),
    )

  private lazy val ledgerAPImTLSConfig = generateTLSServerConfig("ledger-api").copy(
    trustCollectionFile =
      Some(PemFile(ExistingFile.tryCreate("./community/app/src/test/resources/tls/root-ca.crt"))),
    clientAuth = ServerAuthRequirementConfig.Require(adminClient =
      TlsClientCertificate(
        certChainFile = PemFile(
          ExistingFile.tryCreate("./community/app/src/test/resources/tls/admin-client.crt")
        ),
        privateKeyFile = PemFile(
          ExistingFile.tryCreate("./community/app/src/test/resources/tls/admin-client.pem")
        ),
      )
    ),
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      // enable TLS on the public sequencer api
      .addConfigTransform(ConfigTransforms.updateSequencerConfig("sequencer1") { config =>
        val base = generateTLSServerConfig("public-api")
        config
          .focus(_.publicApi.tls)
          .replace(
            Some(
              TlsBaseServerConfig(
                certChainFile = base.certChainFile,
                privateKeyFile = base.privateKeyFile,
              )
            )
          )
      })
      // enable TLS on admin / ledger api for p3
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant3") { config =>
        config
          .focus(_.adminApi.tls)
          .replace(Some(generateTLSServerConfig("admin-api")))
          .focus(_.ledgerApi.tls)
          .replace(Some(ledgerAPImTLSConfig))
      })
      .addConfigTransform { config =>
        // we duplicate the participant3 definition with a different name so we can
        // access it as a remote participant through a separate participant reference.
        // we then intentionally screw up the certs it uses to check the connection
        // is correctly refused.
        val brokenTls =
          TlsClientConfig(
            trustCollectionFile = Some(
              PemFile(ExistingFile.tryCreate("./community/app/src/test/resources/tls/some.crt"))
            ),
            None,
          )
        val p3 = config.participantsByString("participant3")
        val remote3 =
          RemoteParticipantConfig(
            adminApi = p3.adminApi.clientConfig.focus(_.tls).replace(Some(brokenTls)),
            ledgerApi = p3.ledgerApi.clientConfig.focus(_.tls).replace(Some(brokenTls)),
          )
        val valid3 =
          RemoteParticipantConfig(
            adminApi = p3.adminApi.clientConfig,
            ledgerApi = p3.ledgerApi.clientConfig,
          )
        val rootTls = Some(
          TlsClientConfig(
            trustCollectionFile = Some(
              PemFile(ExistingFile.tryCreate("./community/app/src/test/resources/tls/root-ca.crt"))
            ),
            valid3.ledgerApi.tls
              .getOrElse(fail("client certificate should be there as i configured it"))
              .clientCert,
          )
        )
        val valid3Root = RemoteParticipantConfig(
          adminApi = p3.adminApi.clientConfig.focus(_.tls).replace(rootTls),
          ledgerApi = p3.ledgerApi.clientConfig.focus(_.tls).replace(rootTls),
        )
        config
          .focus(_.remoteParticipants)
          .replace(
            config.remoteParticipants ++ Map(
              InstanceName.tryCreate("fail3") -> remote3,
              InstanceName.tryCreate("valid3") -> valid3,
              InstanceName.tryCreate("valid3Root") -> valid3Root,
            )
          )
      }
      // enable JWT HMAC256 on participant1 and add server side TLS for participant1 on ledger-api and admin-api
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant1") { config =>
        val tls = generateTLSServerConfig("ledger-api")
        config
          .focus(_.ledgerApi.authServices)
          .replace(
            Seq(
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = mySecret,
                  targetAudience = None,
                  targetScope = None,
                )
            )
          )
          .focus(_.ledgerApi.tls)
          .replace(Some(tls))
          .focus(_.adminApi.tls)
          .replace(Some(tls))
          .focus(
            _.ledgerApi.jwtTimestampLeeway
          )
          .replace(Some(JwtTimestampLeeway(expiresAt = Some(expirationLeeway))))
          .focus(_.ledgerApi.adminTokenConfig.adminClaim)
          .replace(true)
      })
      // enable mutual TLS auth on participant2 ledger api and admin api
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant2") { config =>
        config
          .focus(_.adminApi.tls)
          .replace(Some(ledgerAPImTLSConfig))
          .focus(_.ledgerApi.tls)
          .replace(Some(ledgerAPImTLSConfig))
          .focus(_.ledgerApi.adminTokenConfig.adminClaim)
          .replace(true)
      })
      // add remote client with invalid client cert
      .addConfigTransform { config =>
        val p2 = config.participantsByString("participant2")
        val invalidClientCert = TlsClientCertificate(
          certChainFile =
            PemFile(ExistingFile.tryCreate("./community/app/src/test/resources/tls/some.crt")),
          privateKeyFile =
            PemFile(ExistingFile.tryCreate("./community/app/src/test/resources/tls/some.pem")),
        )
        def fixTls(clientConfig: FullClientConfig, clientCert: Option[TlsClientCertificate])
            : FullClientConfig = {
          val tls = clientConfig.tls
            .getOrElse(sys.error("tls is there ... "))
            .focus(_.clientCert)
            .replace(clientCert)
          clientConfig.focus(_.tls).replace(Some(tls))
        }
        val invalidCertConfig =
          RemoteParticipantConfig(
            adminApi = fixTls(p2.adminApi.clientConfig, Some(invalidClientCert)),
            ledgerApi = fixTls(p2.ledgerApi.clientConfig, Some(invalidClientCert)),
          )
        val missingCertConfig =
          RemoteParticipantConfig(
            adminApi = fixTls(p2.adminApi.clientConfig, None),
            ledgerApi = fixTls(p2.ledgerApi.clientConfig, None),
          )
        config
          .focus(_.remoteParticipants)
          .replace(
            config.remoteParticipants ++ Map(
              InstanceName.tryCreate("invalid2") -> invalidCertConfig,
              InstanceName.tryCreate("missing2") -> missingCertConfig,
            )
          )
      }
      // add two remote settings for p1, one with a valid token and one with an invalid one
      .addConfigTransform { config =>
        val p1 = config.participantsByString("participant1")
        val iv1 = RemoteParticipantConfig(
          adminApi = p1.adminApi.clientConfig,
          ledgerApi = p1.ledgerApi.clientConfig,
          token = Some(
            JwtTokenUtilities
              .buildUnsafeToken("invalid", Some(UserManagementStore.DefaultParticipantAdminUserId))
          ),
        )
        val v1 = copyWith(iv1, mySecret.unwrap)
        config
          .focus(_.remoteParticipants)
          .replace(
            config.remoteParticipants ++ Map(
              InstanceName.tryCreate("invalid1") -> iv1,
              InstanceName.tryCreate("valid1") -> v1,
            )
          )
      }
      // add two remote settings for p1 with expired tokens, one covered by JWT leeway and one not covered
      .addConfigTransform { config =>
        val valid = config.remoteParticipantsByString("valid1")
        val timeIntoLeeway = Instant.now.minus(Duration.ofSeconds((0.01 * expirationLeeway).toLong))
        val timeOutOfLeeway = Instant.now.minus(Duration.ofSeconds(2 * expirationLeeway))
        // Participant with expired token covered by the leeway parameters
        val expiredLeewayCovered = copyWith(valid, mySecret.unwrap, Some(timeIntoLeeway))
        // Participant with expired token NOT covered by the leeway parameters
        val expiredLeewayNotCovered = copyWith(valid, mySecret.unwrap, Some(timeOutOfLeeway))
        config
          .focus(_.remoteParticipants)
          .replace(
            config.remoteParticipants ++ Map(
              InstanceName.tryCreate("expiredLeewayCovered") -> expiredLeewayCovered,
              InstanceName.tryCreate("expiredLeewayNotCovered") -> expiredLeewayNotCovered,
            )
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
            "fail3",
            "valid3",
            "valid3Root",
            "invalid2",
            "missing2",
            "invalid1",
            "valid1",
            "expiredLeewayCovered",
            "expiredLeewayNotCovered",
          )
        )
      )

  "A client with TLS enabled connecting to a participant node" when {

    val participantAuthenticity = SecurityTest(property = Authenticity, asset = "Participant node")

    "presented an untrusted server certificate" must {
      "refuse ledger api and admin api commands" taggedAs
        participantAuthenticity.setAttack(
          CommonAttacks.impersonateServerWithTls("Participant node")
        ) in { implicit env =>
          import env.*

          // this test proves that TLS server side authentication on the admin api works
          val participant3 = lp("participant3")
          val remote3 = rp("fail3")

          participant3.config.adminApi.tls shouldBe defined
          remote3.config.adminApi.port shouldBe participant3.config.adminApi.port

          // we are using an invalid TLS certificate, so these commands should fail
          assertThrowsAndLogsCommandFailures(
            remote3.id,
            _.commandFailureMessage should include(
              s"Request failed for ${remote3.name}/admin-api. Is the server running"
            ),
          )
          assertThrowsAndLogsCommandFailures(
            remote3.synchronizers.list_registered(),
            _.commandFailureMessage should include(
              s"Request failed for ${remote3.name}/admin-api. Is the server running"
            ),
          )
          assertThrowsAndLogsCommandFailures(
            remote3.ledger_api.state.end(),
            _.commandFailureMessage should include(
              s"Request failed for ${remote3.name}/ledger-api. Is the server running"
            ),
          )
        }
    }

    "presented a trusted server certificate" can_ { setting =>
      "execute several admin api and ledger api commands" taggedAs_ { scen =>
        participantAuthenticity.setHappyCase(s"when $setting can $scen")
      } in { implicit env =>
        import env.*

        val valid3 = rp("valid3")
        val valid3Root = rp("valid3Root")

        def compare[T](
            reference: RemoteParticipantReference,
            command: ParticipantReference => T,
        ) = {
          val local = command(participant3)
          val remote = command(reference)
          assertResult(local)(remote)
        }

        Seq(valid3, valid3Root).foreach { ref =>
          compare(ref, _.id)
          compare(ref, _.synchronizers.list_registered())
          compare(ref, _.ledger_api.state.end())
        }
      }
    }
  }

  "A client with TLS enabled connecting to a synchronizer on the public API" when {

    val publicApiAuthenticity =
      SecurityTest(property = Authenticity, asset = "Synchronizer node")

    "presented a trusted server certificate" can {
      "submit requests to the synchronizer" taggedAs
        publicApiAuthenticity.setHappyCase(
          "Synchronizer/Sequencer client can submit requests with TLS enabled"
        ) in { implicit env =>
          import env.*

          sequencer1.config.publicApi.tls should not be empty
          participant1.synchronizers.connect_local(sequencer1, daName)
          assertPingSucceeds(participant1, participant1, synchronizerId = Some(daId))
        }
    }

    "presented an untrusted server certificate" must {
      "refuse sequencer requests" taggedAs publicApiAuthenticity
        .setAttack(CommonAttacks.impersonateServerWithTls("Synchronizer node"))
        .toBeImplemented in { _ =>
        // TODO(test-coverage): Test the synchronizer client with an untrusted server certificate
        succeed
      }
    }

  }

  "An application using server side TLS authentication and UnsafeJwtHmac256" when {
    // We are merely testing UnsafeJwtHmac256 here to check whether the ledger api auth service can be enabled from
    // Canton. It is assumed that the ledger api test tool will test other settings of the auth service.

    val securityTag = SecurityTest(property = Authorization, asset = "Ledger API application")

    "the token is correct" can {
      "use the ledger api through the local console" taggedAs securityTag.setHappyCase(
        "Ledger API client in local console works with correct UnsafeJwtHmac256 token"
      ) in { implicit env =>
        import env.*

        // if JWT is enabled, we need to be able to bypass it as otherwise, our participant internal
        // admin services will fail to work against the ledger-api. for this purpose, we maintain a
        // adminToken bypass

        participant1.config.ledgerApi.tls should not be empty
        participant1.config.adminApi.tls should not be empty

        participant1.config.ledgerApi.authServices shouldBe Seq(
          AuthServiceConfig.UnsafeJwtHmac256(
            secret = mySecret,
            targetAudience = None,
            targetScope = None,
          )
        )
        // p1 is configured to use JWT authentication, so the adminToken bypass needs to work
        participant1.start()
        participant1.ledger_api.state.acs.of_all().discard
      }

      "use the ledger api through a remote console" taggedAs securityTag.setHappyCase(
        "Ledger API client in remote console works with correct UnsafeJwtHmac256 token"
      ) in { implicit env =>
        import env.*
        val valid1 = rp("valid1")
        valid1.ledger_api.parties.list()
      }
    }

    "the token is expired" must {
      "be accepted from the ledger api when leeway overlaps the expiration time" taggedAs securityTag
        .setHappyCase(
          "Ledger API client in remote console works with expired UnsafeJwtHmac256 token that is overlapped by the JWT leeway parameters"
        ) in { implicit env =>
        import env.*
        val expiredLeewayCoveredParticipant = rp("expiredLeewayCovered")
        expiredLeewayCoveredParticipant.ledger_api.parties.list()
      }
      "be rejected from the ledger api when leeway does not overlap the expiration time" taggedAs securityTag
        .setAttack(
          Attack(
            actor = "Unauthorized ledger API client",
            threat = "An unauthorized application tries to access the ledger API",
            mitigation = "Reject ledger api access of a client with an expired JWT.",
          )
        ) in { implicit env =>
        import env.*

        val expiredLeewayNotCoveredParticipant = rp("expiredLeewayNotCovered")

        // will fail (as party endpoint requires admin privs and the expired token is not covered by the leeway)
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          expiredLeewayNotCoveredParticipant.ledger_api.parties.list(),
          _.warningMessage should include("The Token has expired"),
          _.warningMessage should include("The Token has expired"),
          _.warningMessage should include regex
            """UNAUTHENTICATED\(6,.{8}\): The command is missing a \(valid\) JWT token""".r,
          _.commandFailureMessage should include("UNAUTHENTICATED"),
        )
      }
    }

    "the token is not correct" must {
      "be rejected from the ledger api" taggedAs securityTag.setAttack(
        Attack(
          actor = "Unauthorized ledger API client",
          threat = "An unauthorized application tries to access the ledger API",
          mitigation = "Reject ledger api access of a client with an invalid JWT.",
        )
      ) in { implicit env =>
        import env.*

        val invalid1 = rp("invalid1")

        // will fail (as party endpoint requires admin privs)
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          invalid1.ledger_api.parties.list(),
          _.warningMessage should include("The Token's Signature resulted invalid "),
          _.warningMessage should include("The Token's Signature resulted invalid "),
          _.warningMessage should include regex
            """UNAUTHENTICATED\(6,.{8}\): The command is missing a \(valid\) JWT token""".r,
          _.commandFailureMessage should include("UNAUTHENTICATED"),
        )
      }
    }

    "the token has insufficient privileges" must {
      "be rejected from the ledger api" taggedAs securityTag.setAttack(
        Attack(
          actor = "Unauthorized ledger API client",
          threat = "An unauthorized application tries to access the ledger API",
          mitigation =
            """Specify authorization in JWT and reject client requests on the server if the specified
                            authorization in the token does not match the required one""",
        )
      ) in { implicit env =>
        import env.*

        val bank = participant1.parties.enable("bank", synchronizeParticipants = Seq())
        val owner =
          participant1.parties.enable("owner", synchronizeParticipants = Seq())
        val bankUser =
          participant1.ledger_api.users.create("bankUser", Set(bank), Some(bank), Set(bank)).id
        val ownerUser =
          participant1.ledger_api.users.create("ownerUser", Set(owner), Some(owner), Set(owner)).id

        participant1.dars.upload(CantonExamplesPath)

        val iouCommand = new iou.Iou(
          bank.toProtoPrimitive,
          owner.toProtoPrimitive,
          new iou.Amount(100.0.toBigDecimal, "USD"),
          List.empty.asJava,
        ).create.commands.loneElement

        val bankClient =
          ExternalLedgerApiClient.forReference(
            participant1,
            JwtTokenUtilities.buildUnsafeToken(
              mySecret.unwrap,
              Some(bankUser),
            ),
          )

        val ownerClient =
          ExternalLedgerApiClient.forReference(
            participant1,
            JwtTokenUtilities.buildUnsafeToken(
              mySecret.unwrap,
              Some(ownerUser),
            ),
          )

        val synchronizerId = participant1.synchronizers
          .list_connected()
          .map(_.physicalSynchronizerId)
          .headOption
          .valueOrFail("not connected to a synchronizer")
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          ownerClient.ledger_api.javaapi.commands
            .submit(Seq(bank), Seq(iouCommand), synchronizerId = Some(synchronizerId)),
          _.warningMessage should fullyMatch regex (
            """PERMISSION_DENIED\(7,.{8}\): Claims do not authorize to act as party 'bank::.*'""".r
          ),
          _.commandFailureMessage should include("PERMISSION_DENIED"),
        )

        bankClient.ledger_api.javaapi.commands
          .submit(Seq(bank), Seq(iouCommand), synchronizerId = Some(synchronizerId))

      }
    }
  }

  "A participant node with mutual TLS authentication enabled" when {

    val securityTest = SecurityTest(property = Authenticity, asset = "Ledger/Admin API client")

    "a ledger/admin API client connects with a trusted client certificate" can {
      "accept commands" taggedAs securityTest.setHappyCase(
        "Participant node with mutual authentication enabled accepts ledger/admin API commands from a client with a trusted certificate"
      ) in { implicit env =>
        import env.*

        participant2.id
        participant2.ledger_api.state.end()
      }

    }

    "a ledger/admin API client connects with an invalid or missing client certificate" must {

      "reject commands" taggedAs securityTest.setAttack(
        CommonAttacks.impersonateClientWithTls("ledger/admin API")
      ) in { implicit env =>
        import env.*

        val invalid2 = rp("invalid2")
        val missing2 = rp("missing2")

        Seq[() => Unit](
          () => invalid2.id,
          () => invalid2.ledger_api.state.end(),
          () => missing2.id,
          () => missing2.ledger_api.state.end(),
        ).foreach { action =>
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            action(),
            _.commandFailureMessage should include("Are you using the right TLS settings"),
          )
        }
      }
    }
  }

  "A synchronizer node with mutual TLS authentication enabled" when {
    val securityTest =
      SecurityTest(property = Authenticity, asset = "Synchronizer Admin API client")

    // TODO(test-coverage): Add mutual authentication test for synchronizer admin API. synchronizer public API does not always support client certs and will be tested in the sequencer API integration test.

    "an admin API client connects with a trusted client certificate" can {

      "accept commands" taggedAs securityTest
        .setHappyCase(
          "Synchronizer node with mutual authentication enabled accepts admin API commands from a client with a trusted certificate"
        )
        .toBeImplemented in { _ =>
        succeed
      }

    }

    "an admin API client connects with an invalid or missing client certificate" must {

      "reject commands" taggedAs securityTest
        .setAttack(
          CommonAttacks.impersonateClientWithTls("admin API")
        )
        .toBeImplemented in { _ =>
        succeed
      }
    }

  }
}

class SecuredApisIntegrationTestInMemory extends SecuredApisIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class SecuredApisIntegrationTestPostgres extends SecuredApisIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
