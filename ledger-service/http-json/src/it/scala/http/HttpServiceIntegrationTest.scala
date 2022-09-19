// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File
import java.nio.file.Files
import java.util.UUID

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{
  ContentTypes,
  HttpMethod,
  HttpMethods,
  HttpRequest,
  StatusCodes,
  Uri,
}
import AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData
import dbbackend.JdbcConfig
import json.JsonError
import util.ClientUtil.uniqueId
import util.Logging.instanceUUIDLogCtx
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.v1.{value => v}
import com.daml.lf.data.Ref
import com.daml.lf.value.test.TypedValueGenerators.{ValueAddend => VA}
import com.daml.scalautil.Statement.discard
import json.SprayJson, SprayJson.{decode => jdecode}
import com.daml.http.util.TestUtil.writeToFile
import org.scalacheck.Gen
import org.scalatest.{Assertion, BeforeAndAfterAll}
import scalaz.{-\/, \/-, EitherT, \/}
import scalaz.std.list._
import scalaz.std.scalaFuture._
import scalaz.std.vector._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import shapeless.record.{Record => ShRecord}
import spray.json.{JsValue, enrichAny => `sj enrichAny`}

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
  "request non-existent endpoint should return 404 with errors" in withHttpService { fixture =>
    val badPath = Uri.Path("/contracts/does-not-exist")
    val badUri = fixture.uri withPath badPath
    fixture
      .getRequestWithMinimumAuth[JsValue](badPath)
      .map(inside(_) { case domain.ErrorResponse(Seq(errorMsg), _, StatusCodes.NotFound, _) =>
        errorMsg shouldBe s"${HttpMethods.GET: HttpMethod}, uri: ${badUri: Uri}"
      }): Future[Assertion]
  }

  "parties endpoint should" - {
    "return all known parties" in withHttpService { fixture =>
      import fixture.client
      val partyIds = Vector("P1", "P2", "P3", "P4")
      val partyManagement = client.partyManagementClient

      partyIds
        .traverse { p =>
          partyManagement.allocateParty(Some(p), Some(s"$p & Co. LLC"))
        }
        .flatMap { allocatedParties =>
          fixture
            .getRequest(
              Uri.Path("/v1/parties"),
              headers = headersWithAdminAuth,
            )
            .parseResponse[List[domain.PartyDetails]]
            .map(inside(_) { case domain.OkResponse(result, None, StatusCodes.OK) =>
              val actualIds: Set[domain.Party] = result.view.map(_.identifier).toSet
              actualIds should contain allElementsOf domain.Party.subst(partyIds.toSet)
              result.toSet should contain allElementsOf
                allocatedParties.toSet.map(domain.PartyDetails.fromLedgerApi)
            })
        }: Future[Assertion]
    }

    "return only requested parties, unknown parties returned as warnings" in withHttpService {
      fixture =>
        import fixture.client
        val charlie = getUniqueParty("Charlie")
        val knownParties = Vector(getUniqueParty("Alice"), getUniqueParty("Bob")) :+ charlie
        val erin = getUniqueParty("Erin")
        val requestedPartyIds: Vector[domain.Party] = knownParties.filterNot(_ == charlie) :+ erin

        val partyManagement = client.partyManagementClient

        knownParties
          .traverse { p =>
            partyManagement.allocateParty(Some(p.unwrap), Some(s"${p.unwrap} & Co. LLC"))
          }
          .flatMap { allocatedParties =>
            fixture
              .postJsonRequest(
                Uri.Path("/v1/parties"),
                requestedPartyIds.toJson,
                headersWithAdminAuth,
              )
              .parseResponse[List[domain.PartyDetails]]
              .flatMap(inside(_) { case domain.OkResponse(result, Some(warnings), StatusCodes.OK) =>
                warnings shouldBe domain.UnknownParties(List(erin))
                val actualIds: Set[domain.Party] = result.view.map(_.identifier).toSet
                actualIds shouldBe requestedPartyIds.toSet - erin // Erin is not known
                val expected: Set[domain.PartyDetails] = allocatedParties.toSet
                  .map(domain.PartyDetails.fromLedgerApi)
                  .filterNot(_.identifier == charlie)
                result.toSet shouldBe expected
              })
          }: Future[Assertion]
    }

    "error if empty array passed as input" in withHttpService { fixture =>
      fixture
        .postJsonRequestWithMinimumAuth[JsValue](
          Uri.Path("/v1/parties"),
          Vector.empty[domain.Party].toJson,
        )
        .map(inside(_) {
          case domain.ErrorResponse(Seq(errorMsg), None, StatusCodes.BadRequest, _) =>
            errorMsg should include("Cannot read JSON: <[]>")
            errorMsg should include("must be a JSON array with at least 1 element")
        }): Future[Assertion]
    }

    "error if empty party string passed" in withHttpService { fixture =>
      val requestedPartyIds: Vector[domain.Party] = domain.Party.subst(Vector(""))

      fixture
        .postJsonRequestWithMinimumAuth[List[domain.PartyDetails]](
          Uri.Path("/v1/parties"),
          requestedPartyIds.toJson,
        )
        .map(inside(_) { case domain.ErrorResponse(List(error), None, StatusCodes.BadRequest, _) =>
          error should include("Daml-LF Party is empty")
        }): Future[Assertion]
    }

    "return empty result with warnings and OK status if nothing found" in withHttpService {
      fixture =>
        val requestedPartyIds: Vector[domain.Party] =
          Vector(getUniqueParty("Alice"), getUniqueParty("Bob"))

        fixture
          .postJsonRequest(
            Uri.Path("/v1/parties"),
            requestedPartyIds.toJson,
            headers = headersWithAdminAuth,
          )
          .parseResponse[List[domain.PartyDetails]]
          .map(inside(_) {
            case domain.OkResponse(
                  List(),
                  Some(domain.UnknownParties(unknownParties)),
                  StatusCodes.OK,
                ) =>
              unknownParties.toSet shouldBe requestedPartyIds.toSet
          }): Future[Assertion]
    }
  }

  "parties/allocate should" - {
    "allocate a new party" in withHttpService { fixture =>
      val request = domain.AllocatePartyRequest(
        Some(domain.Party(s"Carol${uniqueId()}")),
        Some("Carol & Co. LLC"),
      )
      val json = SprayJson.encode(request).valueOr(e => fail(e.shows))
      fixture
        .postJsonRequest(
          Uri.Path("/v1/parties/allocate"),
          json = json,
          headers = headersWithAdminAuth,
        )
        .parseResponse[domain.PartyDetails]
        .flatMap(inside(_) { case domain.OkResponse(newParty, _, StatusCodes.OK) =>
          Some(newParty.identifier) shouldBe request.identifierHint
          newParty.displayName shouldBe request.displayName
          newParty.isLocal shouldBe true
          fixture
            .getRequest(
              Uri.Path("/v1/parties"),
              headersWithAdminAuth,
            )
            .parseResponse[List[domain.PartyDetails]]
            .map(inside(_) { case domain.OkResponse(result, _, StatusCodes.OK) =>
              result should contain(newParty)
            })
        }): Future[Assertion]
    }

    "allocate a new party without any hints" in withHttpService { fixture =>
      fixture
        .postJsonRequest(
          Uri.Path("/v1/parties/allocate"),
          json = Map.empty[String, JsValue].toJson,
          headers = headersWithAdminAuth,
        )
        .parseResponse[domain.PartyDetails]
        .flatMap(inside(_) { case domain.OkResponse(newParty, _, StatusCodes.OK) =>
          newParty.identifier.unwrap.length should be > 0
          newParty.displayName shouldBe None
          newParty.isLocal shouldBe true

          fixture
            .getRequest(
              Uri.Path("/v1/parties"),
              headers = headersWithAdminAuth,
            )
            .parseResponse[List[domain.PartyDetails]]
            .map(inside(_) { case domain.OkResponse(result, _, StatusCodes.OK) =>
              result should contain(newParty)
            })
        }): Future[Assertion]
    }

    // TEST_EVIDENCE: Authorization: badly-authorized create is rejected
    "return BadRequest error if party ID hint is invalid PartyIdString" in withHttpService {
      fixture =>
        val request = domain.AllocatePartyRequest(
          Some(domain.Party(s"Carol-!")),
          Some("Carol & Co. LLC"),
        )
        val json = SprayJson.encode(request).valueOr(e => fail(e.shows))

        fixture
          .postJsonRequest(
            Uri.Path("/v1/parties/allocate"),
            json = json,
            headers = headersWithAdminAuth,
          )
          .parseResponse[JsValue]
          .map(inside(_) { case domain.ErrorResponse(errors, None, StatusCodes.BadRequest, _) =>
            errors.length shouldBe 1
          })
    }
  }

  "packages endpoint should" - {
    "return all known package IDs" in withHttpService { fixture =>
      getAllPackageIds(fixture).map { x =>
        inside(x) {
          case domain.OkResponse(ps, None, StatusCodes.OK) if ps.nonEmpty =>
            import org.scalatest.Inspectors.forAll
            forAll(ps)(_.length should be > 0)
        }
      }: Future[Assertion]
    }
  }

  "packages/packageId should" - {
    "return a requested package" in withHttpService { fixture =>
      import AbstractHttpServiceIntegrationTestFuns.sha256
      import scala.util.Success
      import fixture.uri
      getAllPackageIds(fixture).flatMap { okResp =>
        inside(okResp.result.headOption) { case Some(packageId) =>
          Http()
            .singleRequest(
              HttpRequest(
                method = HttpMethods.GET,
                uri = uri.withPath(Uri.Path(s"/v1/packages/$packageId")),
                headers = headersWithAdminAuth,
              )
            )
            .map { resp =>
              resp.status shouldBe StatusCodes.OK
              resp.entity.getContentType() shouldBe ContentTypes.`application/octet-stream`
              sha256(resp.entity.dataBytes) shouldBe Success(packageId)
            }
        }
      }: Future[Assertion]
    }

    "return NotFound if a non-existing package is requested" in withHttpService { fixture =>
      Http()
        .singleRequest(
          HttpRequest(
            method = HttpMethods.GET,
            uri = fixture.uri.withPath(Uri.Path(s"/v1/packages/12345678")),
            headers = headersWithAdminAuth,
          )
        )
        .map { resp =>
          resp.status shouldBe StatusCodes.NotFound
        }
    }
  }

  "packages upload endpoint" in withHttpService { fixture =>
    val newDar = AbstractHttpServiceIntegrationTestFuns.dar3

    getAllPackageIds(fixture).flatMap { okResp =>
      val existingPackageIds: Set[String] = okResp.result.toSet
      uploadPackage(fixture)(newDar)
        .flatMap { _ =>
          getAllPackageIds(fixture).map { okResp =>
            val newPackageIds: Set[String] = okResp.result.toSet -- existingPackageIds
            newPackageIds.size should be > 0
          }
        }
    }: Future[Assertion]
  }

  "package list is updated when a query request is made" in usingLedger(testId) {
    case (ledgerPort, _, _) =>
      withHttpServiceOnly(ledgerPort) { fixture =>
        for {
          alicePartyAndAuthHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
          (alice, headers) = alicePartyAndAuthHeaders
          _ <- withHttpServiceOnly(ledgerPort) { innerFixture =>
            val searchDataSet = genSearchDataSet(alice)
            searchDataSet.traverse(c => postCreateCommand(c, innerFixture, headers)).flatMap { rs =>
              rs.map(_.status) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)
            }
          }
          _ <- withHttpServiceOnly(ledgerPort) { innerFixture =>
            innerFixture
              .getRequest(Uri.Path("/v1/query"), headers)
              .parseResponse[Vector[JsValue]]
              .map(inside(_) { case domain.OkResponse(result, _, StatusCodes.OK) =>
                result should have length 4
              }): Future[Assertion]
          }
        } yield succeed
      }
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

  "metering-report endpoint should return metering report" in withHttpService { fixture =>
    import java.time.LocalDate
    import com.google.protobuf.struct.Struct
    import endpoints.MeteringReportEndpoint.MeteringReportDateRequest
    val isoDate = "2022-02-03"
    val request = MeteringReportDateRequest(
      from = LocalDate.parse(isoDate),
      to = None,
      application = None,
    )
    fixture
      .postJsonRequestWithMinimumAuth[Struct](
        Uri.Path("/v1/metering-report"),
        request.toJson,
      )
      .map(inside(_) { case domain.OkResponse(meteringReport, _, StatusCodes.OK) =>
        meteringReport
          .fields("request")
          .getStructValue
          .fields("from")
          .getStringValue shouldBe s"${isoDate}T00:00:00Z"
      })
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
