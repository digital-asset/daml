// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.stream.Materializer
import com.daml.bazeltools.BazelRunfiles._
import com.daml.cliopts.Logging.LogEncoder
import com.daml.grpc.adapter.{PekkoExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.metrics.HttpJsonApiMetrics
import com.daml.http.util.Logging.{InstanceUUID, instanceUUIDLogCtx}
import com.daml.http.{HttpService, StartSettings, nonrepudiation}
import com.daml.integrationtest._
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, CustomDamlJWTPayload}
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.api.testing.utils.{
  PekkoBeforeAndAfterAll,
  OwnedResource,
  SuiteResourceManagementAroundAll,
  SuiteResource,
}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.archive.{Dar, DarDecoder}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script._
import com.daml.lf.engine.script.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.engine.script.v1.ledgerinteraction.JsonLedgerClient
import com.daml.lf.language.Ast.Package
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue._
import com.daml.lf.typesig.EnvironmentSignature
import com.daml.lf.typesig.reader.SignatureReader
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.logging.LoggingContextOf
import com.daml.ports.Port
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.traverse._
import scalaz.{-\/, \/-}
import spray.json._

import java.nio.file.{Files, Path, Paths}
import com.daml.metrics.api.reporters.MetricsReporter
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, Future}

trait JsonApiFixture
    extends SuiteResource[(Port, ServerBinding)]
    with SuiteResourceManagementAroundAll
    with PekkoBeforeAndAfterAll
    with Inside {
  self: Suite =>

  private val logger = LoggerFactory.getLogger(getClass)

  lazy val tmpDir = Files.createTempDirectory("JsonApiFixture")

  override protected def afterAll(): Unit = {
    com.daml.fs.Utils.deleteRecursively(tmpDir)
    jsonApiExecutionSequencerFactory.close()
    materializer.shutdown()
    Await.result(jsonApiActorSystem.terminate(), 30.seconds)
    super.afterAll()
  }

  private val secret = "secret"

  val darFile = rlocation(Paths.get("daml-script/test/script-test-v1.dar"))

  val darFileNoLedger = rlocation(Paths.get("daml-script/test/script-test-no-ledger.dar"))

  val applicationId = Some(Ref.ApplicationId.assertFromString(getClass.getName))
  val darFiles = List(darFile)

  val config = CantonConfig(authSecret = Some(secret))

  protected def serverPort = suiteResource.value._1
  protected def httpPort = suiteResource.value._2.localAddress.getPort

  // We have to use a different actorsystem for the JSON API since package reloading
  // blocks everything so it will timeout as sandbox cannot make progress simultaneously.
  private val jsonApiActorSystem: ActorSystem = ActorSystem("json-api")
  private val jsonApiMaterializer: Materializer = Materializer(system)
  private val jsonApiExecutionSequencerFactory: ExecutionSequencerFactory =
    new PekkoExecutionSequencerPool(poolName = "json-api", actorCount = 1)

  protected def getCustomToken(
      actAs: List[String],
      readAs: List[String],
      admin: Boolean,
      ledgerId: String = config.ledgerIds(0),
  ): String = {
    val payload = CustomDamlJWTPayload(
      ledgerId = Some(ledgerId),
      participantId = None,
      exp = None,
      applicationId = Some(applicationId.getOrElse("")),
      actAs = actAs,
      readAs = readAs,
      admin = admin,
    )
    val header = """{"alg": "HS256", "typ": "JWT"}"""
    val jwt = DecodedJwt[String](header, AuthServiceJWTCodec.writeToString(payload))
    JwtSigner.HMAC256.sign(jwt, secret) match {
      case -\/(e) => throw new IllegalStateException(e.toString)
      case \/-(a) => a.value
    }
  }

  protected lazy val suiteResource: OwnedResource[ResourceContext, (Port, ServerBinding)] = {
    implicit val context: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (Port, ServerBinding)](
      for {
        ports <- CantonRunner.run(config, tmpDir, logger, darFiles)
        serverPort = ports.head.ledgerPort
        httpService <- new ResourceOwner[ServerBinding] {
          override def acquire()(implicit context: ResourceContext): Resource[ServerBinding] = {
            implicit val lc: LoggingContextOf[InstanceUUID] = instanceUUIDLogCtx(identity(_))
            Resource[ServerBinding] {
              val config = new StartSettings.Default {
                override val ledgerHost = "localhost"
                override val ledgerPort = serverPort.value
                override val address = "localhost"
                override val httpPort = 0
                override val portFile = None
                override val https = None
                override val tlsConfig = TlsConfiguration(enabled = false, None, None, None)
                override val wsConfig = None
                override val allowNonHttps = true
                override val authConfig = None
                override val nonRepudiation = nonrepudiation.Configuration.Cli.Empty
                override val logLevel = None
                override val logEncoder = LogEncoder.Plain
                override val metricsReporter: Option[MetricsReporter] = None
                override val metricsReportingInterval: FiniteDuration = 10.seconds
              }
              HttpService
                .start(config)(
                  jsonApiActorSystem,
                  jsonApiMaterializer,
                  jsonApiExecutionSequencerFactory,
                  jsonApiActorSystem.dispatcher,
                  lc,
                  metrics = HttpJsonApiMetrics.ForTesting,
                )
                .flatMap {
                  case -\/(e) => Future.failed(new IllegalStateException(e.toString))
                  case \/-(a) => Future.successful(a._1)
                }
            }((binding: ServerBinding) => binding.unbind().map(_ => ()))
          }
        }
      } yield (serverPort, httpService),
      acquisitionTimeout = 2.minute,
      releaseTimeout = 2.minute,
    )
  }
}

final class JsonApiIt extends AsyncWordSpec with JsonApiFixture with Matchers with TryValues {

  private def readDar(file: Path): (Dar[(PackageId, Package)], EnvironmentSignature) = {
    val dar = DarDecoder.assertReadArchiveFromFile(file.toFile)
    val ifaceDar = dar.map(pkg => SignatureReader.readPackageSignature(() => \/-(pkg))._2)
    val envIface = EnvironmentSignature.fromPackageSignatures(ifaceDar)
    (dar, envIface)
  }

  val (dar, envIface) = readDar(darFile)
  val (darNoLedger, envIfaceNoLedger) = readDar(darFileNoLedger)

  private def getClients(
      parties: List[String],
      defaultParty: Option[Party] = None,
      admin: Boolean = false,
      applicationId: Option[Option[Ref.ApplicationId]] = None,
      envIface: EnvironmentSignature = envIface,
  ) = {
    // We give the default participant some nonsense party so the checks for party mismatch fail
    // due to the mismatch and not because the token does not allow inferring a party
    val defaultParticipant =
      ApiParameters(
        "http://localhost",
        httpPort,
        Some(getCustomToken(defaultParty.toList, List.empty, true)),
        applicationId,
      )
    val partyMap = parties.map(p => (Party.assertFromString(p), Participant(p))).toMap
    val participantMap = parties
      .map(p =>
        (
          Participant(p),
          ApiParameters(
            "http://localhost",
            httpPort,
            Some(getCustomToken(List(p), List.empty, admin)),
            applicationId,
          ),
        )
      )
      .toMap
    val participantParams = Participants(Some(defaultParticipant), participantMap, partyMap)
    Runner.jsonClients(participantParams, envIface)
  }

  private def getMultiPartyClients(
      parties: List[String],
      readAs: List[String] = List.empty,
      applicationId: Option[Option[Ref.ApplicationId]] = None,
      envIface: EnvironmentSignature = envIface,
  ) = {
    // We give the default participant some nonsense party so the checks for party mismatch fail
    // due to the mismatch and not because the token does not allow inferring a party
    val defaultParticipant =
      ApiParameters(
        "http://localhost",
        httpPort,
        Some(getCustomToken(parties, readAs, true)),
        applicationId,
      )
    val participantParams = Participants(Some(defaultParticipant), Map.empty, Map.empty)
    for {
      ps <- Runner.jsonClients(participantParams, envIface)
    } yield ps
  }

  private def ledgerClient(token: Option[String]) =
    config.ledgerClient(serverPort, token, applicationId)

  private def allocateParty = for {
    adminClient <- ledgerClient(config.adminToken)
    details <- adminClient.partyManagementClient.allocateParty(None, None)
  } yield details.party

  private def getUserClients(
      user: UserId,
      envIface: EnvironmentSignature = envIface,
  ) = {
    // We give the default participant some nonsense party so the checks for party mismatch fail
    // due to the mismatch and not because the token does not allow inferring a party
    val defaultParticipant =
      ApiParameters(
        "http://localhost",
        httpPort,
        config.getToken(user),
        None,
      )
    val participantParams = Participants(Some(defaultParticipant), Map.empty, Map.empty)
    Runner.jsonClients(participantParams, envIface)
  }

  private def run(
      clients: Participants[ScriptLedgerClient],
      name: QualifiedName,
      inputValue: Option[JsValue],
      dar: Dar[(PackageId, Package)] = dar,
  ): Future[SValue] = {
    val scriptId = Identifier(dar.main._1, name)
    Runner.run(dar, scriptId, inputValue, clients, ScriptTimeMode.WallClock)
  }

  "Daml Script over JSON API" can {
    "Basic" should {
      "return 42" in {
        for {
          alice <- allocateParty
          clients <- getClients(List(alice))
          result <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:jsonBasic"),
            Some(JsString(alice)),
          )
        } yield {
          assert(result == SInt64(42))
        }
      }
    }
    "CreateAndExercise" should {
      "return 42" in {
        for {
          alice <- allocateParty
          clients <- getClients(List(alice))
          result <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:jsonCreateAndExercise"),
            Some(JsString(alice)),
          )
        } yield {
          assert(result == SInt64(42))
        }
      }
    }
    "ExerciseByKey" should {
      "return equal contract ids" in {
        for {
          alice <- allocateParty
          clients <- getClients(List(alice))
          result <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:jsonExerciseByKey"),
            Some(JsString(alice)),
          )
        } yield {
          result match {
            case SRecord(_, _, vals) if vals.size == 2 =>
              assert(vals.get(0) == vals.get(1))
            case _ => fail(s"Expected Tuple2 but got $result")
          }
        }
      }
    }
    "submit with party mismatch fails" in {
      for {
        alice <- allocateParty
        bob <- allocateParty
        clients <- getClients(List(alice), defaultParty = Some(alice))
        exception <- recoverToExceptionIf[RuntimeException](
          run(
            clients,
            QualifiedName.assertFromString("ScriptTest:jsonCreate"),
            Some(JsString(bob)),
          )
        )
      } yield {
        assert(
          exception.getCause.getMessage === s"Tried to submit a command with actAs = [${bob}] but token provides claims for actAs = [${alice}]. Missing claims: [${bob}]"
        )
      }
    }
    "application id mismatch" in {
      for {
        alice <- allocateParty
        exception <- recoverToExceptionIf[RuntimeException](
          getClients(
            List(alice),
            applicationId = Some(Some(Ref.ApplicationId.assertFromString("wrong"))),
          )
        )
      } yield assert(
        exception.getMessage === s"ApplicationId specified in token Some(${applicationId.getOrElse("")}) must match Some(wrong)"
      )
    }
    "application id correct" in {
      for {
        alice <- allocateParty
        clients <- getClients(
          List(alice),
          defaultParty = Some(alice),
          applicationId = Some(applicationId),
        )
        r <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:jsonCreateAndExercise"),
          Some(JsString(alice)),
        )
      } yield assert(r == SInt64(42))
    }
    "query with party mismatch fails" in {
      for {
        alice <- allocateParty
        bob <- allocateParty
        clients <- getClients(List(alice), defaultParty = Some(alice))
        exception <- recoverToExceptionIf[RuntimeException](
          run(
            clients,
            QualifiedName.assertFromString("ScriptTest:jsonQuery"),
            Some(JsString(bob)),
          )
        )
      } yield {
        assert(
          exception.getCause.getMessage === s"Tried to query as [${bob}] but token provides claims for [${alice}]. Missing claims: [${bob}]"
        )
      }
    }
    "submit with no party fails" in {
      for {
        alice <- allocateParty
        clients <- getClients(List.empty)
        exception <- recoverToExceptionIf[RuntimeException](
          run(
            clients,
            QualifiedName.assertFromString("ScriptTest:jsonCreate"),
            Some(JsString(alice)),
          )
        )
      } yield {
        assert(
          exception.getCause.getMessage === "Tried to submit a command with actAs = [" + alice + "] but token contains no actAs parties."
        )
      }
    }
    "submit fails on assertion failure" in {
      for {
        alice <- allocateParty
        clients <- getClients(List(alice))
        exception <- recoverToExceptionIf[Script.FailedCmd](
          run(
            clients,
            QualifiedName.assertFromString("ScriptTest:jsonFailingCreateAndExercise"),
            Some(JsString(alice)),
          )
        )
      } yield {
        exception.cause.getMessage should include(
          "Interpretation error: Error: Unhandled Daml exception: DA.Exception.AssertionFailed:AssertionFailed@3f4deaf1{ message = \"Assertion failed\" }."
        )
      }
    }
    "submitMustFail succeeds on assertion failure" in {
      for {
        alice <- allocateParty
        clients <- getClients(List(alice))
        result <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:jsonExpectedFailureCreateAndExercise"),
          Some(JsString(alice)),
        )
      } yield {
        assert(result == SUnit)
      }
    }
    "user management" in {
      for {
        alice <- allocateParty
        clients <- getClients(List(alice))
        result <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:jsonUserManagement"),
          Some(JsString(alice)),
        )
      } yield {
        assert(result == SUnit)
      }
    }
    "user management rights" in {
      for {
        alice <- allocateParty
        clients <- getClients(List(alice))
        result <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:jsonUserRightManagement"),
          Some(JsString(alice)),
        )
      } yield {
        assert(result == SUnit)
      }
    }
    "party management" in {
      for {
        clients <- getClients(List.empty)
        result <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:jsonAllocateParty"),
          Some(JsString("Eve")),
        )
      } yield {
        inside(result) { case SParty(party) => party should startWith("Eve::") }
      }
    }
    "multi-party" in {
      for {
        alice <- allocateParty
        bob <- allocateParty
        clients <- getClients(List(alice, bob))
        result <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:jsonMultiParty"),
          Some(JsArray(JsString(alice), JsString(bob))),
        )
      } yield {
        assert(result == SUnit)
      }
    }
    "missing template id" in {
      for {
        alice <- allocateParty
        clients <- getClients(List(alice))
        ex <- recoverToExceptionIf[RuntimeException](
          run(
            clients,
            QualifiedName.assertFromString("ScriptTest:jsonMissingTemplateId"),
            Some(JsString(alice)),
            dar = darNoLedger,
          )
        )
      } yield {
        assert(ex.getCause.toString.contains("Cannot resolve template ID"))
      }
    }
    "queryContractId" in {
      for {
        alice <- allocateParty
        clients <- getClients(List(alice))
        result <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:jsonQueryContractId"),
          Some(JsString(alice)),
        )
      } yield {
        assert(result == SUnit)
      }
    }
    "queryInterface" in {
      for {
        alice <- allocateParty
        clients <- getClients(List(alice), envIface = envIface)
        result <- run(
          clients,
          QualifiedName.assertFromString("TestInterfaces:jsonQueryInterface"),
          Some(JsString(alice)),
          dar = dar,
        )
      } yield {
        assert(result == SUnit)
      }
    }
    "queryContractKey" in {
      for {
        alice <- allocateParty
        clients <- getClients(List(alice))
        result <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:jsonQueryContractKey"),
          inputValue = Some(JsString(alice)),
        )
      } yield {
        assert(result == SUnit)
      }
    }
    "multiPartyQuery" in {
      for {
        party0 <- allocateParty
        party1 <- allocateParty
        clients <- getClients(List(party0, party1))
        cids <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:multiPartyQueryCreate"),
          inputValue = Some(JsArray(JsString(party0), JsString(party1))),
        )
        multiClients <- getMultiPartyClients(List(party0, party1))
        cids <- run(
          multiClients,
          QualifiedName.assertFromString("ScriptTest:multiPartyQueryQuery"),
          inputValue = Some(
            JsArray(
              JsArray(JsString(party0), JsString(party1)),
              ApiCodecCompressed.apiValueToJsValue(cids.toUnnormalizedValue),
            )
          ),
        ).transform(x => scala.util.Success(x))
        cids <- Future.fromTry(cids)
      } yield {
        assert(cids == SUnit)
      }
    }
    "multiPartySubmission" in {
      for {
        party1 <- allocateParty
        party2 <- allocateParty
        clients1 <- getClients(List(party1, party2))
        cidSingle <- run(
          clients1,
          QualifiedName.assertFromString("ScriptTest:jsonMultiPartySubmissionCreateSingle"),
          inputValue = Some(JsString(party1)),
        )
          .map(v => ApiCodecCompressed.apiValueToJsValue(v.toUnnormalizedValue))
        clientsBoth <- getMultiPartyClients(List(party1, party2))
        cidBoth <- run(
          clientsBoth,
          QualifiedName.assertFromString("ScriptTest:jsonMultiPartySubmissionCreate"),
          inputValue = Some(JsArray(JsString(party1), JsString(party2))),
        )
          .map(v => ApiCodecCompressed.apiValueToJsValue(v.toUnnormalizedValue))
        clients2 <- getMultiPartyClients(List(party2), List(party1))
        r <- run(
          clients2,
          QualifiedName.assertFromString("ScriptTest:jsonMultiPartySubmissionExercise"),
          inputValue = Some(JsArray(JsString(party1), JsString(party2), cidBoth, cidSingle)),
        )
      } yield {
        assert(r == SUnit)
      }
    }
    "party-set arguments" in {
      for {
        party1 <- allocateParty
        party2 <- allocateParty
        clients <- getMultiPartyClients(List(party1, party2))
        r <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:jsonMultiPartyPartySets"),
          inputValue = Some(JsArray(JsString(party1), JsString(party2))),
        )
      } yield {
        r shouldBe SUnit
      }
    }
    "user tokens" in {
      for {
        p1 <- allocateParty
        p2 <- allocateParty
        adminClient <- ledgerClient(config.adminToken)
        user <- adminClient.userManagementClient.createUser(
          User(UserId.assertFromString("u"), None),
          Seq(UserRight.CanActAs(p1), UserRight.CanActAs(p2)),
        )
        clients <- getUserClients(user.id)
        r <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:jsonMultiPartyPartySets"),
          inputValue = Some(JsArray(JsString(p1), JsString(p2))),
        )
      } yield r shouldBe SUnit
    }
    "invalid response" in {
      def withServer[A](f: ServerBinding => Future[A]) = {
        val bindingF: Future[ServerBinding] = Http().newServerAt("localhost", 0).bind(reject)
        val fa: Future[A] = bindingF.flatMap(f)
        fa.transformWith { ta =>
          Future
            .sequence(
              Seq(
                bindingF.flatMap(_.unbind())
              )
            )
            .transform(_ => ta)
        }
      }
      withServer { binding =>
        for {
          alice <- allocateParty
          participant = ApiParameters(
            "http://localhost",
            binding.localAddress.getPort,
            Some(getCustomToken(List(alice), List.empty, true)),
            None,
          )
          participants = Participants(Some(participant), Map.empty, Map.empty)
          clients <- Runner.jsonClients(participants, envIface)
          exc <- recoverToExceptionIf[Script.FailedCmd](
            run(
              clients,
              QualifiedName.assertFromString("ScriptTest:jsonBasic"),
              Some(JsString(alice)),
            )
          )
        } yield {
          exc.cause shouldBe a[JsonLedgerClient.FailedJsonApiRequest]
          val cause = exc.cause.asInstanceOf[JsonLedgerClient.FailedJsonApiRequest]
          cause.respStatus shouldBe StatusCodes.NotFound
          cause.errors shouldBe List("The requested resource could not be found.")
        }
      }
    }
  }
}
