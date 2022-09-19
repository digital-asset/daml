// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File
import java.nio.file.Files
import java.util.UUID

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCodes, Uri}
import AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData
import dbbackend.JdbcConfig
import json.JsonError
import util.Logging.instanceUUIDLogCtx
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.v1.{value => v}
import com.daml.lf.data.Ref
import com.daml.lf.value.test.TypedValueGenerators.{ValueAddend => VA}
import com.daml.scalautil.Statement.discard
import json.SprayJson.{decode => jdecode}
import com.daml.http.util.TestUtil.writeToFile
import org.scalacheck.Gen
import org.scalatest.{Assertion, BeforeAndAfterAll}
import scalaz.{-\/, \/-, EitherT, \/}
import scalaz.std.scalaFuture._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import shapeless.record.{Record => ShRecord}
import spray.json.JsValue

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class HttpServiceIntegrationTest
    extends AbstractHttpServiceIntegrationTestTokenIndependent
    with BeforeAndAfterAll {
  import HttpServiceIntegrationTest._
  import json.JsonProtocol._
  import AbstractHttpServiceIntegrationTestFuns.ciouDar

  private val staticContent: String = "static"

  private val staticContentDir: File =
    Files.createTempDirectory("integration-test-static-content").toFile

  override def staticContentConfig: Option[StaticContentConfig] =
    Some(StaticContentConfig(prefix = staticContent, directory = staticContentDir))

  override def jdbcConfig: Option[JdbcConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None

  private val expectedDummyContent: String = Gen
    .listOfN(100, Gen.identifier)
    .map(_.mkString(" "))
    .sample
    .getOrElse(throw new IllegalStateException(s"Cannot create dummy text content"))

  private val dummyFile: File =
    writeToFile(new File(staticContentDir, "dummy.txt"), expectedDummyContent).get
  require(dummyFile.exists)

  override protected def afterAll(): Unit = {
    // clean up temp directory
    discard { dummyFile.delete() }
    discard { staticContentDir.delete() }
    super.afterAll()
  }

  "query with invalid JSON query should return error" in withHttpService { fixture =>
    fixture
      .postJsonStringRequest(Uri.Path("/v1/query"), "{NOT A VALID JSON OBJECT")
      .parseResponse[JsValue]
      .map(inside(_) { case domain.ErrorResponse(_, _, StatusCodes.BadRequest, _) =>
        succeed
      }): Future[Assertion]
  }

  "create" - {
    import domain.ContractTypeId.OptionalPkg

    "succeeds with single party, proper argument" in withHttpService { fixture =>
      import fixture.encoder
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice)

        postCreateCommand(command, fixture, headers)
          .map(inside(_) { case domain.OkResponse(activeContract, _, StatusCodes.OK) =>
            assertActiveContract(activeContract)(command, encoder)
          }): Future[Assertion]
      }
    }

    // TEST_EVIDENCE: Authorization: reject requests with missing auth header
    "fails if authorization header is missing" in withHttpService { fixture =>
      import fixture.encoder
      val alice = getUniqueParty("Alice")
      val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice)
      val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

      fixture
        .postJsonRequest(Uri.Path("/v1/create"), input, List())
        .parseResponse[JsValue]
        .map(inside(_) { case domain.ErrorResponse(Seq(error), _, StatusCodes.Unauthorized, _) =>
          error should include(
            "missing Authorization header with OAuth 2.0 Bearer Token"
          )
        }): Future[Assertion]
    }

    "supports extra readAs parties" in withHttpService { fixture =>
      import fixture.encoder
      val alice = getUniqueParty("Alice")
      val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice)
      val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

      fixture
        .headersWithPartyAuth(actAs = List(alice.unwrap), readAs = List("Bob"))
        .flatMap(
          fixture
            .postJsonRequest(
              Uri.Path("/v1/create"),
              input,
              _,
            )
            .parseResponse[domain.ActiveContract[JsValue]]
        )
        .map(inside(_) { case domain.OkResponse(activeContract, _, StatusCodes.OK) =>
          assertActiveContract(activeContract)(command, encoder)
        }): Future[Assertion]
    }

    "with unsupported templateId should return proper error" in withHttpService { fixture =>
      import fixture.encoder
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val command: domain.CreateCommand[v.Record, OptionalPkg] =
          iouCreateCommand(alice).copy(templateId = domain.TemplateId(None, "Iou", "Dummy"))
        val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

        fixture
          .postJsonRequest(Uri.Path("/v1/create"), input, headers)
          .parseResponse[JsValue]
          .map(inside(_) { case domain.ErrorResponse(Seq(error), _, StatusCodes.BadRequest, _) =>
            val unknownTemplateId: OptionalPkg =
              domain
                .TemplateId(None, command.templateId.moduleName, command.templateId.entityName)
            error should include(
              s"Cannot resolve template ID, given: ${unknownTemplateId: OptionalPkg}"
            )
          }): Future[Assertion]
      }
    }

    "supports command deduplication" in withHttpService { fixture =>
      import fixture.encoder
      def genSubmissionId() = domain.SubmissionId(UUID.randomUUID().toString)
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val cmdId = domain.CommandId apply UUID.randomUUID().toString
        def cmd(
            submissionId: domain.SubmissionId
        ): domain.CreateCommand[v.Record, OptionalPkg] =
          iouCreateCommand(
            alice,
            amount = "19002.0",
            meta = Some(
              domain.CommandMeta(
                commandId = Some(cmdId),
                actAs = None,
                readAs = None,
                submissionId = Some(submissionId),
                deduplicationPeriod = Some(domain.DeduplicationPeriod.Duration(10000L)),
              )
            ),
          )

        val firstCreate: JsValue =
          encoder.encodeCreateCommand(cmd(genSubmissionId())).valueOr(e => fail(e.shows))

        fixture
          .postJsonRequest(Uri.Path("/v1/create"), firstCreate, headers)
          .parseResponse[domain.CreateCommandResponse[JsValue]]
          .map(inside(_) { case domain.OkResponse(result, _, _) =>
            result.completionOffset.unwrap should not be empty
          })
          .flatMap { _ =>
            val secondCreate: JsValue =
              encoder.encodeCreateCommand(cmd(genSubmissionId())).valueOr(e => fail(e.shows))
            fixture
              .postJsonRequest(Uri.Path("/v1/create"), secondCreate, headers)
              .map(inside(_) { case (StatusCodes.Conflict, _) =>
                succeed
              }): Future[Assertion]
          }
      }
    }
  }

  "create-and-exercise IOU_Transfer" in withHttpService { fixture =>
    import fixture.encoder
    fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
      val cmd
          : domain.CreateAndExerciseCommand[v.Record, v.Value, domain.ContractTypeId.OptionalPkg] =
        iouCreateAndExerciseTransferCommand(alice)

      val json: JsValue = encoder.encodeCreateAndExerciseCommand(cmd).valueOr(e => fail(e.shows))

      fixture
        .postJsonRequest(Uri.Path("/v1/create-and-exercise"), json, headers)
        .parseResponse[domain.ExerciseResponse[JsValue]]
        .flatMap(inside(_) { case domain.OkResponse(result, None, StatusCodes.OK) =>
          result.completionOffset.unwrap should not be empty
          inside(result.events) {
            case List(
                  domain.Contract(\/-(created0)),
                  domain.Contract(-\/(archived0)),
                  domain.Contract(\/-(created1)),
                ) =>
              assertTemplateId(created0.templateId, cmd.templateId)
              assertTemplateId(archived0.templateId, cmd.templateId)
              archived0.contractId shouldBe created0.contractId
              assertTemplateId(created1.templateId, TpId.Iou.IouTransfer)
              asContractId(result.exerciseResult) shouldBe created1.contractId
          }
        })
    }: Future[Assertion]
  }

  "should be able to serialize and deserialize domain commands" in withHttpService { fixture =>
    (testCreateCommandEncodingDecoding(fixture) *>
      testExerciseCommandEncodingDecoding(fixture)): Future[Assertion]
  }

  private def testCreateCommandEncodingDecoding(
      fixture: HttpServiceTestFixtureData
  ): Future[Assertion] = instanceUUIDLogCtx { implicit lc =>
    import fixture.{uri, encoder, decoder}
    import util.ErrorOps._
    import com.daml.jwt.domain.Jwt

    val command0: domain.CreateCommand[v.Record, domain.ContractTypeId.OptionalPkg] =
      iouCreateCommand(domain.Party("Alice"))

    type F[A] = EitherT[Future, JsonError, A]
    val x: F[Assertion] = for {
      jsVal <- EitherT.either(
        encoder.encodeCreateCommand(command0).liftErr(JsonError)
      ): F[JsValue]
      command1 <- (EitherT.rightT(jwt(uri)): F[Jwt])
        .flatMap(decoder.decodeCreateCommand(jsVal, _, fixture.ledgerId))
    } yield command1.bimap(removeRecordId, removePackageId) should ===(command0)

    (x.run: Future[JsonError \/ Assertion]).map(_.fold(e => fail(e.shows), identity))
  }

  private def testExerciseCommandEncodingDecoding(
      fixture: HttpServiceTestFixtureData
  ): Future[Assertion] = {
    import fixture.{uri, encoder, decoder}
    val command0 = iouExerciseTransferCommand(lar.ContractId("#a-contract-ID"))
    val jsVal: JsValue = encodeExercise(encoder)(command0)
    val command1 =
      jwt(uri).flatMap(decodeExercise(decoder, _, fixture.ledgerId)(jsVal))
    command1.map(_.bimap(removeRecordId, identity) should ===(command0))
  }

  "should serve static content from configured directory" in withHttpService {
    (uri: Uri, _, _, _) =>
      Http()
        .singleRequest(
          HttpRequest(
            method = HttpMethods.GET,
            uri = uri.withPath(Uri.Path(s"/$staticContent/${dummyFile.getName}")),
          )
        )
        .flatMap { resp =>
          discard { resp.status shouldBe StatusCodes.OK }
          val bodyF: Future[String] = util.TestUtil.getResponseDataBytes(resp, debug = false)
          bodyF.flatMap { body =>
            body shouldBe expectedDummyContent
          }
        }: Future[Assertion]
  }

  "exercise interface choices" - {
    import AbstractHttpServiceIntegrationTestFuns.{UriFixture, EncoderFixture}

    def createIouAndExerciseTransfer(
        fixture: UriFixture with EncoderFixture,
        initialTplId: domain.TemplateId.OptionalPkg,
        exerciseTid: domain.TemplateId.OptionalPkg,
        choice: TExercise[_] = tExercise(choiceArgType = echoTextVA)(echoTextSample),
    ) = for {
      aliceH <- fixture.getUniquePartyAndAuthHeaders("Alice")
      (alice, aliceHeaders) = aliceH
      createTest <- postCreateCommand(
        iouCommand(alice, initialTplId),
        fixture,
        aliceHeaders,
      )
      testIIouID = resultContractId(createTest)
      exerciseTest <- fixture
        .postJsonRequest(
          Uri.Path("/v1/exercise"),
          encodeExercise(fixture.encoder)(
            iouTransfer(
              domain.EnrichedContractId(Some(exerciseTid), testIIouID),
              choice,
            )
          ),
          aliceHeaders,
        )
        .parseResponse[domain.ExerciseResponse[JsValue]]
    } yield exerciseTest

    def exerciseSucceeded[A](
        exerciseTest: domain.SyncResponse[domain.ExerciseResponse[JsValue]]
    ) =
      inside(exerciseTest) { case domain.OkResponse(er, None, StatusCodes.OK) =>
        inside(jdecode[String](er.exerciseResult)) { case \/-(decoded) => decoded }
      }
    object Transferrable {
      val Transferrable: domain.ContractTypeId.Interface.OptionalPkg =
        domain.ContractTypeId.Interface(None, "Transferrable", "Transferrable")
    }

    "templateId = interface ID" in withHttpService { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        result <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = domain.TemplateId(None, "IIou", "TestIIou"),
          // whether we can exercise by interface-ID
          exerciseTid = TpId.IIou.IIou,
        ) map exerciseSucceeded
      } yield result should ===("Bob invoked IIou.Transfer")
    }

    // ideally we would upload IIou.daml, then force a reload, then upload ciou;
    // however tests currently don't play well with reload -SC
    "templateId = template ID" in withHttpService { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        result <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = CIou.CIou,
          // whether we can exercise inherited by concrete template ID
          exerciseTid = CIou.CIou,
        ) map exerciseSucceeded
      } yield result should ===("Bob invoked IIou.Transfer")
    }

    "templateId = template ID, choiceInterfaceId = interface ID" in withHttpService { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        result <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = CIou.CIou,
          exerciseTid = CIou.CIou,
          choice = tExercise(choiceInterfaceId = Some(TpId.IIou.IIou), choiceArgType = echoTextVA)(
            echoTextSample
          ),
        ) map exerciseSucceeded
      } yield result should ===("Bob invoked IIou.Transfer")
    }

    "templateId = template, no choiceInterfaceId, picks template Overridden" in withHttpService {
      fixture =>
        for {
          _ <- uploadPackage(fixture)(ciouDar)
          result <- createIouAndExerciseTransfer(
            fixture,
            initialTplId = CIou.CIou,
            exerciseTid = CIou.CIou,
            choice = tExercise(choiceName = "Overridden", choiceArgType = echoTextPairVA)(
              ShRecord(echo = ShRecord(_1 = "yes", _2 = "no"))
            ),
          ) map exerciseSucceeded
        } yield result should ===("(\"yes\",\"no\") invoked CIou.Overridden")
    }

    "templateId = template, choiceInterfaceId = interface, picks interface Overridden" in withHttpService {
      fixture =>
        for {
          _ <- uploadPackage(fixture)(ciouDar)
          result <- createIouAndExerciseTransfer(
            fixture,
            initialTplId = CIou.CIou,
            exerciseTid = CIou.CIou,
            choice = tExercise(Some(Transferrable.Transferrable), "Overridden", echoTextVA)(
              ShRecord(echo = "yesyes")
            ),
          ) map exerciseSucceeded
        } yield result should ===("yesyes invoked Transferrable.Overridden")
    }

    "templateId = template, no choiceInterfaceId, ambiguous" in withHttpService { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        response <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = CIou.CIou,
          exerciseTid = CIou.CIou,
          choice = tExercise(choiceName = "Ambiguous", choiceArgType = echoTextVA)(
            ShRecord(echo = "ambiguous-test")
          ),
        )
      } yield inside(response) {
        case domain.ErrorResponse(Seq(onlyError), None, StatusCodes.BadRequest, None) =>
          (onlyError should include regex
            raw"Cannot resolve Choice Argument type, given: \(TemplateId\([0-9a-f]{64},CIou,CIou\), Ambiguous\)")
      }
    }

    "templateId = template ID, retroactive implements choice" in withHttpService { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        result <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = CIou.CIou,
          exerciseTid = CIou.CIou,
          choice = tExercise(choiceName = "TransferPlease", choiceArgType = echoTextVA)(
            echoTextSample
          ),
        ) map exerciseSucceeded
      } yield result should ===("Bob invoked RIIou.TransferPlease")
    }
  }

  "fail to exercise by key with interface ID" in withHttpService { fixture =>
    import fixture.encoder
    for {
      _ <- uploadPackage(fixture)(ciouDar)
      aliceH <- fixture.getUniquePartyAndAuthHeaders("Alice")
      (alice, aliceHeaders) = aliceH
      createTest <- postCreateCommand(
        iouCommand(alice, domain.TemplateId(None, "CIou", "CIou")),
        fixture,
        aliceHeaders,
      )
      _ = createTest.status should ===(StatusCodes.OK)
      exerciseTest <- fixture
        .postJsonRequest(
          Uri.Path("/v1/exercise"),
          encodeExercise(encoder)(
            iouTransfer(
              domain.EnrichedContractKey(
                TpId.unsafeCoerce[domain.ContractTypeId.Template, Option[String]](TpId.IIou.IIou),
                v.Value(v.Value.Sum.Party(domain.Party unwrap alice)),
              ),
              tExercise()(ShRecord(echo = "bob")),
            )
          ),
          aliceHeaders,
        )
        .parseResponse[JsValue]
    } yield inside(exerciseTest) {
      case domain.ErrorResponse(Seq(lookup), None, StatusCodes.BadRequest, _) =>
        lookup should include regex raw"Cannot resolve Template Key type, given: InterfaceId\([0-9a-f]{64},IIou,IIou\)"
    }
  }

  private[this] def iouTransfer[Inj](
      locator: domain.ContractLocator[v.Value],
      choice: TExercise[Inj],
  ) = {
    import choice.{choiceInterfaceId, choiceName, choiceArgType, choiceArg}
    val payload = argToApi(choiceArgType)(choiceArg)
    domain.ExerciseCommand(
      locator,
      domain.Choice(choiceName),
      v.Value(v.Value.Sum.Record(payload)),
      choiceInterfaceId,
      None,
    )
  }
}

object HttpServiceIntegrationTest {
  private[this] val irrelevant = Ref.Identifier assertFromString "none:Discarded:Identifier"

  private val (_, echoTextVA) =
    VA.record(irrelevant, ShRecord(echo = VA.text))

  private val (_, echoTextPairVA) =
    VA.record(
      irrelevant,
      ShRecord(echo = VA.record(irrelevant, ShRecord(_1 = VA.text, _2 = VA.text))._2),
    )

  private val echoTextSample: echoTextVA.Inj = ShRecord(echo = "Bob")

  private def tExercise(
      choiceInterfaceId: Option[domain.ContractTypeId.Interface.OptionalPkg] = None,
      choiceName: String = "Transfer",
      choiceArgType: VA = echoTextVA,
  )(
      choiceArg: choiceArgType.Inj
  ): TExercise[choiceArgType.Inj] =
    TExercise(choiceInterfaceId, choiceName, choiceArgType, choiceArg)

  private final case class TExercise[Inj](
      choiceInterfaceId: Option[domain.ContractTypeId.Interface.OptionalPkg],
      choiceName: String,
      choiceArgType: VA.Aux[Inj],
      choiceArg: Inj,
  )
}

final class HttpServiceIntegrationTestCustomToken
    extends HttpServiceIntegrationTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken
