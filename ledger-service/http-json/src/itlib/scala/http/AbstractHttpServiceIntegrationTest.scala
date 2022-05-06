// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.time.{Instant, LocalDate}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import com.daml.api.util.TimestampConversion
import com.daml.lf.data.Ref
import com.daml.http.domain.ContractId
import com.daml.http.domain.TemplateId.OptionalPkg
import com.daml.http.endpoints.MeteringReportEndpoint.{
  MeteringReport,
  MeteringReportDateRequest,
  MeteringReportRequest,
}
import com.daml.http.json.SprayJson.{decode, decode1, objectField}
import com.daml.http.json._
import com.daml.http.util.ClientUtil.{boxedRecord, uniqueId}
import com.daml.http.util.FutureUtil
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.v1.{value => v}
import com.daml.ledger.service.MetadataReader
import com.daml.http.util.Logging.instanceUUIDLogCtx
import com.daml.ledger.api.domain.LedgerId
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.std.list._
import scalaz.std.vector._
import scalaz.std.scalaFuture._
import scalaz.syntax.apply._
import scalaz.syntax.bitraverse._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, \/, \/-}
import shapeless.record.{Record => ShRecord}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.{value => lfv}
import lfv.test.TypedValueGenerators.{ValueAddend => VA}

trait AbstractHttpServiceIntegrationTestFunsCustomToken
    extends AsyncFreeSpec
    with AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.CustomToken
    with Matchers
    with Inside {

  import json.JsonProtocol._

  protected def jwt(uri: Uri)(implicit ec: ExecutionContext): Future[Jwt] =
    jwtForParties(uri)(List("Alice"), List(), testId)

  protected def headersWithPartyAuthLegacyFormat(
      actAs: List[String],
      readAs: List[String] = List(),
  ) =
    HttpServiceTestFixture.headersWithPartyAuth(
      actAs,
      readAs,
      Some(testId),
      withoutNamespace = true,
    )

  "get all parties using the legacy token format" in withHttpServiceAndClient {
    (uri, _, _, client, _) =>
      val partyIds = Vector("P1", "P2", "P3", "P4").map(getUniqueParty(_).unwrap)
      val partyManagement = client.partyManagementClient
      partyIds
        .traverse { p =>
          partyManagement.allocateParty(Some(p), Some(s"$p & Co. LLC"))
        }
        .flatMap { allocatedParties =>
          getRequest(
            uri = uri.withPath(Uri.Path("/v1/parties")),
            headersWithPartyAuthLegacyFormat(List()),
          )
            .flatMap { case (status, output) =>
              status shouldBe StatusCodes.OK
              inside(
                decode1[domain.OkResponse, List[domain.PartyDetails]](output)
              ) { case \/-(response) =>
                response.status shouldBe StatusCodes.OK
                response.warnings shouldBe empty
                val actualIds: Set[domain.Party] = response.result.view.map(_.identifier).toSet
                actualIds should contain allElementsOf domain.Party.subst(partyIds.toSet)
                response.result.toSet should contain allElementsOf
                  allocatedParties.toSet.map(domain.PartyDetails.fromLedgerApi)
              }
            }
        }: Future[Assertion]
  }

  "create should fail with custom tokens that contain no ledger id" in withHttpService {
    (uri, encoder, _, _) =>
      val alice = getUniqueParty("Alice")
      val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
      val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

      val headers = HttpServiceTestFixture.authorizationHeader(
        HttpServiceTestFixture.jwtForParties(List("Alice"), List("Bob"), None, false, false)
      )

      postJsonRequest(
        uri.withPath(Uri.Path("/v1/create")),
        input,
        headers,
      )
        .flatMap { case (status, output) =>
          status shouldBe StatusCodes.Unauthorized
          assertStatus(output, StatusCodes.Unauthorized)
          HttpServiceTestFixture.getChild(
            output,
            "errors",
          ) shouldBe JsArray(JsString("ledgerId missing in access token"))

        }: Future[Assertion]
  }

  "metering-report endpoint should return metering report" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
      {
        val isoDate = "2022-02-03"
        val request = MeteringReportDateRequest(
          from = LocalDate.parse(isoDate),
          to = None,
          application = None,
        )
        val expected = MeteringReportRequest(
          from = Timestamp.assertFromString(s"${isoDate}T00:00:00Z"),
          to = None,
          application = None,
        )
        postJsonRequestWithMinimumAuth(
          uri.withPath(Uri.Path("/v1/metering-report")),
          request.toJson,
        ).map { case (status, value) =>
          status shouldBe StatusCodes.OK
          result(value).convertTo[MeteringReport].request shouldBe expected
        }
      }
  }

}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class AbstractHttpServiceIntegrationTestTokenIndependent
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with StrictLogging
    with AbstractHttpServiceIntegrationTestFuns {

  import AbstractHttpServiceIntegrationTestFuns.{ciouDar, VAx}
  import HttpServiceTestFixture._
  import json.JsonProtocol._

  override def useTls = UseTls.NoTls

  "query GET empty results" in withHttpService { (uri: Uri, _, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (_, headers) =>
      searchAllExpectOk(uri, headers).flatMap { case vector =>
        vector should have size 0L
      }
    }
  }

  protected def genSearchDataSet(
      party: domain.Party
  ): List[domain.CreateCommand[v.Record, OptionalPkg]] = {
    val partyName = party.unwrap
    List(
      iouCreateCommand(amount = "111.11", currency = "EUR", partyName = partyName),
      iouCreateCommand(amount = "222.22", currency = "EUR", partyName = partyName),
      iouCreateCommand(amount = "333.33", currency = "GBP", partyName = partyName),
      iouCreateCommand(amount = "444.44", currency = "BTC", partyName = partyName),
    )
  }

  "query GET" in withHttpService { (uri: Uri, encoder, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val searchDataSet = genSearchDataSet(alice)
      searchDataSet.traverse(c => postCreateCommand(c, encoder, uri, headers)).flatMap { rs =>
        rs.map(_._1) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

        getRequest(uri = uri.withPath(Uri.Path("/v1/query")), headers)
          .flatMap { case (status, output) =>
            status shouldBe StatusCodes.OK
            assertStatus(output, StatusCodes.OK)
            inside(output) { case JsObject(fields) =>
              inside(fields.get("result")) { case Some(JsArray(vector)) =>
                vector should have size searchDataSet.size.toLong
              }
            }
          }: Future[Assertion]
      }
    }
  }

  "multi-party query GET" in withHttpService { (uri, encoder, _, _) =>
    for {
      res1 <- getUniquePartyAndAuthHeaders(uri)("Alice")
      (alice, aliceHeaders) = res1
      res2 <- getUniquePartyAndAuthHeaders(uri)("Bob")
      (bob, bobHeaders) = res2
      _ <- postCreateCommand(
        accountCreateCommand(owner = alice, number = "42"),
        encoder,
        uri,
        headers = aliceHeaders,
      ).map(r => r._1 shouldBe StatusCodes.OK)
      _ <- postCreateCommand(
        accountCreateCommand(owner = bob, number = "23"),
        encoder,
        uri,
        headers = bobHeaders,
      ).map(r => r._1 shouldBe StatusCodes.OK)
      _ <- searchAllExpectOk(uri, aliceHeaders).map(cs => cs should have size 1)
      _ <- searchAllExpectOk(uri, bobHeaders).map(cs => cs should have size 1)
      _ <- headersWithPartyAuth(uri)(List(alice.unwrap, bob.unwrap))
        .flatMap(headers => searchAllExpectOk(uri, headers))
        .map(cs => cs should have size 2)
    } yield succeed
  }

  "query POST with empty query" in withHttpService { (uri, encoder, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val searchDataSet = genSearchDataSet(alice)
      searchExpectOk(
        searchDataSet,
        jsObject("""{"templateIds": ["Iou:Iou"]}"""),
        uri,
        encoder,
        headers,
      ).map { acl: List[domain.ActiveContract[JsValue]] =>
        acl.size shouldBe searchDataSet.size
      }
    }
  }

  "multi-party query POST with empty query" in withHttpService { (uri, encoder, _, _) =>
    for {
      res1 <- getUniquePartyAndAuthHeaders(uri)("Alice")
      res2 <- getUniquePartyAndAuthHeaders(uri)("Bob")
      (alice, aliceHeaders) = res1
      (bob, bobHeaders) = res2
      aliceAccountResp <- postCreateCommand(
        accountCreateCommand(owner = alice, number = "42"),
        encoder,
        uri,
        aliceHeaders,
      )
      _ = aliceAccountResp._1 shouldBe StatusCodes.OK
      bobAccountResp <- postCreateCommand(
        accountCreateCommand(owner = bob, number = "23"),
        encoder,
        uri,
        bobHeaders,
      )
      _ = bobAccountResp._1 shouldBe StatusCodes.OK
      _ <- searchExpectOk(
        List(),
        jsObject("""{"templateIds": ["Account:Account"]}"""),
        uri,
        encoder,
        aliceHeaders,
      )
        .map(acl => acl.size shouldBe 1)
      _ <- searchExpectOk(
        List(),
        jsObject("""{"templateIds": ["Account:Account"]}"""),
        uri,
        encoder,
        bobHeaders,
      )
        .map(acl => acl.size shouldBe 1)
      _ <- headersWithPartyAuth(uri)(List(alice.unwrap, bob.unwrap))
        .flatMap(headers =>
          searchExpectOk(
            List(),
            jsObject("""{"templateIds": ["Account:Account"]}"""),
            uri,
            encoder,
            headers,
          )
        )
        .map(acl => acl.size shouldBe 2)
    } yield {
      assert(true)
    }
  }

  "query with query, one field" in withHttpService { (uri, encoder, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val searchDataSet = genSearchDataSet(alice)
      searchExpectOk(
        searchDataSet,
        jsObject("""{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR"}}"""),
        uri,
        encoder,
        headers,
      ).map { acl: List[domain.ActiveContract[JsValue]] =>
        acl.size shouldBe 2
        acl.map(a => objectField(a.payload, "currency")) shouldBe List.fill(2)(
          Some(JsString("EUR"))
        )
      }
    }
  }

  "query returns unknown Template IDs as warnings" in withHttpService { (uri, encoder, _, _) =>
    val query =
      jsObject(
        """{"templateIds": ["Iou:Iou", "UnknownModule:UnknownEntity"], "query": {"currency": "EUR"}}"""
      )
    // TODO VM(#12922) https://github.com/digital-asset/daml/pull/12922#discussion_r815234434
    logger.info("query returns unknown Template IDs")
    headersWithPartyAuth(uri)(List("UnknownParty")).flatMap(headers =>
      search(List(), query, uri, encoder, headers).map { response =>
        inside(response) { case domain.OkResponse(acl, warnings, StatusCodes.OK) =>
          acl.size shouldBe 0
          warnings shouldBe Some(
            domain.UnknownTemplateIds(
              List(domain.TemplateId(None, "UnknownModule", "UnknownEntity"))
            )
          )
        }
      }
    )
  }

  "query returns unknown Template IDs as warnings and error" in withHttpService {
    (uri, encoder, _, _) =>
      getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
        search(
          genSearchDataSet(alice),
          jsObject("""{"templateIds": ["AAA:BBB", "XXX:YYY"]}"""),
          uri,
          encoder,
          headers,
        ).map { response =>
          inside(response) {
            case domain.ErrorResponse(errors, warnings, StatusCodes.BadRequest, _) =>
              errors shouldBe List(ErrorMessages.cannotResolveAnyTemplateId)
              inside(warnings) { case Some(domain.UnknownTemplateIds(unknownTemplateIds)) =>
                unknownTemplateIds.toSet shouldBe Set(
                  domain.TemplateId(None, "AAA", "BBB"),
                  domain.TemplateId(None, "XXX", "YYY"),
                )
              }
          }
        }
      }
  }

  "query with query, can use number or string for numeric field" in withHttpService {
    (uri, encoder, _, _) =>
      getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
        val searchDataSet = genSearchDataSet(alice)
        searchDataSet.traverse(c => postCreateCommand(c, encoder, uri, headers)).flatMap {
          rs: List[(StatusCode, JsValue)] =>
            rs.map(_._1) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

            def queryAmountAs(s: String) =
              jsObject(s"""{"templateIds": ["Iou:Iou"], "query": {"amount": $s}}""")

            val queryAmountAsString = queryAmountAs("\"111.11\"")
            val queryAmountAsNumber = queryAmountAs("111.11")

            List(
              postJsonRequest(uri.withPath(Uri.Path("/v1/query")), queryAmountAsString, headers),
              postJsonRequest(uri.withPath(Uri.Path("/v1/query")), queryAmountAsNumber, headers),
            ).sequence.flatMap { rs: List[(StatusCode, JsValue)] =>
              rs.map(_._1) shouldBe List.fill(2)(StatusCodes.OK)
              inside(rs.map(_._2)) { case List(jsVal1, jsVal2) =>
                jsVal1 shouldBe jsVal2
                val acl1: List[domain.ActiveContract[JsValue]] = activeContractList(jsVal1)
                val acl2: List[domain.ActiveContract[JsValue]] = activeContractList(jsVal2)
                acl1 shouldBe acl2
                inside(acl1) { case List(ac) =>
                  objectField(ac.payload, "amount") shouldBe Some(JsString("111.11"))
                }
              }
            }
        }: Future[Assertion]
      }
  }

  private[this] def randomTextN(n: Int) = {
    import org.scalacheck.Gen
    Gen
      .buildableOfN[String, Char](n, Gen.alphaNumChar)
      .sample
      .getOrElse(sys.error(s"can't generate ${n}b string"))
  }

  Seq(
    "& " -> "& bar",
    "1kb of data" -> randomTextN(1000),
    "2kb of data" -> randomTextN(2000),
    "3kb of data" -> randomTextN(3000),
    "4kb of data" -> randomTextN(4000),
    "5kb of data" -> randomTextN(5000),
  ).foreach { case (testLbl, testCurrency) =>
    s"query record contains handles '$testLbl' strings properly" in withHttpService {
      (uri, encoder, _, _) =>
        getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
          searchExpectOk(
            genSearchDataSet(alice) :+ iouCreateCommand(
              currency = testCurrency,
              partyName = alice.unwrap,
            ),
            jsObject(
              s"""{"templateIds": ["Iou:Iou"], "query": {"currency": ${testCurrency.toJson}}}"""
            ),
            uri,
            encoder,
            headers,
          ).map(inside(_) { case Seq(domain.ActiveContract(_, _, _, JsObject(fields), _, _, _)) =>
            fields.get("currency") should ===(Some(JsString(testCurrency)))
          })
        }
    }
  }

  "query with query, two fields" in withHttpService { (uri, encoder, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val searchDataSet = genSearchDataSet(alice)
      searchExpectOk(
        searchDataSet,
        jsObject(
          """{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR", "amount": "111.11"}}"""
        ),
        uri,
        encoder,
        headers,
      ).map { acl: List[domain.ActiveContract[JsValue]] =>
        acl.size shouldBe 1
        acl.map(a => objectField(a.payload, "currency")) shouldBe List(Some(JsString("EUR")))
        acl.map(a => objectField(a.payload, "amount")) shouldBe List(Some(JsString("111.11")))
      }
    }
  }

  "query with query, no results" in withHttpService { (uri, encoder, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val searchDataSet = genSearchDataSet(alice)
      searchExpectOk(
        searchDataSet,
        jsObject(
          """{"templateIds": ["Iou:Iou"], "query": {"currency": "RUB", "amount": "666.66"}}"""
        ),
        uri,
        encoder,
        headers,
      ).map { acl: List[domain.ActiveContract[JsValue]] =>
        acl.size shouldBe 0
      }
    }
  }

  "query with invalid JSON query should return error" in withHttpService { (uri, _, _, _) =>
    postJsonStringRequest(uri.withPath(Uri.Path("/v1/query")), "{NOT A VALID JSON OBJECT")
      .flatMap { case (status, output) =>
        status shouldBe StatusCodes.BadRequest
        assertStatus(output, StatusCodes.BadRequest)
      }: Future[Assertion]
  }

  "fail to query by interface ID" in withHttpService { (uri, encoder, _, _) =>
    for {
      _ <- uploadPackage(uri)(ciouDar)
      aliceH <- getUniquePartyAndAuthHeaders(uri)("Alice")
      (alice, aliceHeaders) = aliceH
      searchResp <- search(
        List.empty,
        Map(
          "templateIds" -> Seq(TpId.IIou.IIou).toJson,
          "query" -> spray.json.JsObject(),
        ).toJson.asJsObject,
        uri,
        encoder,
        aliceHeaders,
      )
    } yield inside(searchResp) {
      case domain.ErrorResponse(
            Seq(_),
            Some(domain.UnknownTemplateIds(Seq(TpId.IIou.IIou))),
            StatusCodes.BadRequest,
            _,
          ) =>
        succeed
    }
  }

  protected def searchAllExpectOk(
      uri: Uri,
      headers: List[HttpHeader],
  ): Future[List[domain.ActiveContract[JsValue]]] =
    searchAll(uri, headers).map(expectOk(_))

  protected def searchAllExpectOk(
      uri: Uri
  ): Future[List[domain.ActiveContract[JsValue]]] =
    headersWithAuth(uri).flatMap(searchAllExpectOk(uri, _))

  protected def searchAll(
      uri: Uri,
      headers: List[HttpHeader],
  ): Future[domain.SyncResponse[List[domain.ActiveContract[JsValue]]]] = {
    getRequest(uri = uri.withPath(Uri.Path("/v1/query")), headers)
      .flatMap { case (_, output) =>
        FutureUtil.toFuture(
          decode1[domain.SyncResponse, List[domain.ActiveContract[JsValue]]](output)
        )
      }
  }

  "create IOU" in withHttpService { (uri, encoder, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)

      postCreateCommand(command, encoder, uri, headers).flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val activeContract = getResult(output)
        assertActiveContract(activeContract)(command, encoder)
      }: Future[Assertion]
    }
  }

  // TEST_EVIDENCE: Authorization: reject requests with missing auth header
  "create IOU should fail if authorization header is missing" in withHttpService {
    (uri, encoder, _, _) =>
      val alice = getUniqueParty("Alice")
      val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
      val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

      postJsonRequest(uri.withPath(Uri.Path("/v1/create")), input, List()).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.Unauthorized
          assertStatus(output, StatusCodes.Unauthorized)
          expectedOneErrorMessage(output) should include(
            "missing Authorization header with OAuth 2.0 Bearer Token"
          )
      }: Future[Assertion]
  }

  "create IOU should support extra readAs parties" in withHttpService { (uri, encoder, _, _) =>
    val alice = getUniqueParty("Alice")
    val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
    val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

    headersWithPartyAuth(uri)(actAs = List(alice.unwrap), readAs = List("Bob"))
      .flatMap(
        postJsonRequest(
          uri.withPath(Uri.Path("/v1/create")),
          input,
          _,
        )
      )
      .flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val activeContract = getResult(output)
        assertActiveContract(activeContract)(command, encoder)
      }: Future[Assertion]
  }

  "create IOU with unsupported templateId should return proper error" in withHttpService {
    (uri, encoder, _, _) =>
      getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
        val command: domain.CreateCommand[v.Record, OptionalPkg] =
          iouCreateCommand(alice.unwrap).copy(templateId = domain.TemplateId(None, "Iou", "Dummy"))
        val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

        postJsonRequest(uri.withPath(Uri.Path("/v1/create")), input, headers).flatMap {
          case (status, output) =>
            status shouldBe StatusCodes.BadRequest
            assertStatus(output, StatusCodes.BadRequest)
            val unknownTemplateId: OptionalPkg =
              domain.TemplateId(None, command.templateId.moduleName, command.templateId.entityName)
            expectedOneErrorMessage(output) should include(
              s"Cannot resolve template ID, given: ${unknownTemplateId: OptionalPkg}"
            )
        }: Future[Assertion]
      }
  }

  "exercise IOU_Transfer" in withHttpService { (uri, encoder, decoder, ledgerId) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val create: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
      postCreateCommand(create, encoder, uri, headers)
        .flatMap { case (createStatus, createOutput) =>
          createStatus shouldBe StatusCodes.OK
          assertStatus(createOutput, StatusCodes.OK)

          val contractId = getContractId(getResult(createOutput))
          val exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId] =
            iouExerciseTransferCommand(contractId)
          val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

          postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), exerciseJson, headers)
            .flatMap { case (exerciseStatus, exerciseOutput) =>
              exerciseStatus shouldBe StatusCodes.OK
              assertStatus(exerciseOutput, StatusCodes.OK)
              assertExerciseResponseNewActiveContract(
                getResult(exerciseOutput),
                create,
                exercise,
                decoder,
                uri,
                ledgerId,
                headers,
              )
            }
        }: Future[Assertion]
    }
  }

  "create-and-exercise IOU_Transfer" in withHttpService { (uri, encoder, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val cmd: domain.CreateAndExerciseCommand[v.Record, v.Value, OptionalPkg] =
        iouCreateAndExerciseTransferCommand(alice.unwrap)

      val json: JsValue = encoder.encodeCreateAndExerciseCommand(cmd).valueOr(e => fail(e.shows))

      postJsonRequest(uri.withPath(Uri.Path("/v1/create-and-exercise")), json, headers)
        .flatMap { case (status, output) =>
          status shouldBe StatusCodes.OK
          inside(
            decode1[domain.OkResponse, domain.ExerciseResponse[JsValue]](output)
          ) { case \/-(response) =>
            response.status shouldBe StatusCodes.OK
            response.warnings shouldBe empty
            inside(response.result.events) {
              case List(
                    domain.Contract(\/-(created0)),
                    domain.Contract(-\/(archived0)),
                    domain.Contract(\/-(created1)),
                  ) =>
                assertTemplateId(created0.templateId, cmd.templateId)
                assertTemplateId(archived0.templateId, cmd.templateId)
                archived0.contractId shouldBe created0.contractId
                assertTemplateId(created1.templateId, TpId.Iou.IouTransfer)
                asContractId(response.result.exerciseResult) shouldBe created1.contractId
            }
          }
        }: Future[Assertion]
    }
  }

  private def assertExerciseResponseNewActiveContract(
      exerciseResponse: JsValue,
      createCmd: domain.CreateCommand[v.Record, OptionalPkg],
      exerciseCmd: domain.ExerciseCommand[v.Value, domain.EnrichedContractId],
      decoder: DomainJsonDecoder,
      uri: Uri,
      ledgerId: LedgerId,
      headers: List[HttpHeader],
  ): Future[Assertion] = {
    inside(SprayJson.decode[domain.ExerciseResponse[JsValue]](exerciseResponse)) {
      case \/-(domain.ExerciseResponse(JsString(exerciseResult), List(contract1, contract2))) =>
        // checking contracts
        inside(contract1) { case domain.Contract(-\/(archivedContract)) =>
          Future {
            (archivedContract.contractId.unwrap: String) shouldBe (exerciseCmd.reference.contractId.unwrap: String)
          }
        } *>
          inside(contract2) { case domain.Contract(\/-(activeContract)) =>
            assertActiveContract(uri)(decoder, activeContract, createCmd, exerciseCmd, ledgerId)
          } *>
          // checking exerciseResult
          {
            exerciseResult.length should be > (0)
            val newContractLocator = domain.EnrichedContractId(
              Some(TpId.Iou.IouTransfer),
              domain.ContractId(exerciseResult),
            )
            postContractsLookup(newContractLocator, uri, headers).flatMap { case (status, output) =>
              status shouldBe StatusCodes.OK
              assertStatus(output, StatusCodes.OK)
              getContractId(getResult(output)) shouldBe newContractLocator.contractId
            }: Future[Assertion]
          }
    }
  }

  "exercise IOU_Transfer with unknown contractId should return proper error" in withHttpService {
    (uri, encoder, _, _) =>
      val contractIdString = "0" * 66
      val contractId = lar.ContractId(contractIdString)
      val exerciseJson: JsValue = encodeExercise(encoder)(iouExerciseTransferCommand(contractId))
      postJsonRequestWithMinimumAuth(uri.withPath(Uri.Path("/v1/exercise")), exerciseJson)
        .flatMap { case (status, output) =>
          status shouldBe StatusCodes.NotFound
          assertStatus(output, StatusCodes.NotFound)
          expectedOneErrorMessage(output) should include(
            s"Contract could not be found with id $contractIdString"
          )
          val ledgerApiError =
            output.asJsObject.fields("ledgerApiError").convertTo[domain.LedgerApiError]
          ledgerApiError.message should include("CONTRACT_NOT_FOUND")
          ledgerApiError.message should include(
            "Contract could not be found with id 000000000000000000000000000000000000000000000000000000000000000000"
          )
          import org.scalatest.Inspectors._
          forExactly(1, ledgerApiError.details) {
            case domain.ErrorInfoDetail(errorCodeId, _) =>
              errorCodeId shouldBe "CONTRACT_NOT_FOUND"
            case _ => fail()
          }
          forExactly(1, ledgerApiError.details) {
            case domain.RequestInfoDetail(_) => succeed
            case _ => fail()
          }
          forExactly(1, ledgerApiError.details) {
            case domain.ResourceInfoDetail(name, typ) =>
              name shouldBe "000000000000000000000000000000000000000000000000000000000000000000"
              typ shouldBe "CONTRACT_ID"
            case _ => fail()
          }
        }: Future[Assertion]
  }

  "exercise Archive" in withHttpService { (uri, encoder, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val create: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
      postCreateCommand(create, encoder, uri, headers)
        .flatMap { case (createStatus, createOutput) =>
          createStatus shouldBe StatusCodes.OK
          assertStatus(createOutput, StatusCodes.OK)

          val contractId = getContractId(getResult(createOutput))
          val reference = domain.EnrichedContractId(Some(TpId.Iou.Iou), contractId)
          val exercise = archiveCommand(reference)
          val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

          postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), exerciseJson, headers)
            .flatMap { case (exerciseStatus, exerciseOutput) =>
              exerciseStatus shouldBe StatusCodes.OK
              assertStatus(exerciseOutput, StatusCodes.OK)
              val exercisedResponse: JsObject = getResult(exerciseOutput).asJsObject
              assertExerciseResponseArchivedContract(exercisedResponse, exercise)
            }
        }: Future[Assertion]
    }
  }

  "should support multi-party command submissions" in withHttpService { (uri, encoder, _, _) =>
    for {
      // multi-party actAs on create
      cid <- headersWithPartyAuth(uri)(List("Alice", "Bob"))
        .flatMap(
          postCreateCommand(multiPartyCreateCommand(List("Alice", "Bob"), ""), encoder, uri, _)
        )
        .map { case (status, output) =>
          status shouldBe StatusCodes.OK
          getContractId(getResult(output))
        }
      // multi-party actAs on exercise
      cidMulti <- headersWithPartyAuth(uri)(List("Alice", "Bob", "Charlie", "David"))
        .flatMap(
          postJsonRequest(
            uri.withPath(Uri.Path("/v1/exercise")),
            encodeExercise(encoder)(multiPartyAddSignatories(cid, List("Charlie", "David"))),
            _,
          )
        )
        .map { case (status, output) =>
          status shouldBe StatusCodes.OK
          inside(getChild(getResult(output), "exerciseResult")) { case JsString(c) =>
            lar.ContractId(c)
          }
        }
      // create a contract only visible to Alice
      cid <- headersWithPartyAuth(uri)(List("Alice"))
        .flatMap(
          postCreateCommand(
            multiPartyCreateCommand(List("Alice"), ""),
            encoder,
            uri,
            _,
          )
        )
        .map { case (status, output) =>
          status shouldBe StatusCodes.OK
          getContractId(getResult(output))
        }
      _ <- headersWithPartyAuth(uri)(List("Charlie"), readAs = List("Alice"))
        .flatMap(
          postJsonRequest(
            uri.withPath(Uri.Path("/v1/exercise")),
            encodeExercise(encoder)(multiPartyFetchOther(cidMulti, cid, List("Charlie"))),
            _,
          )
        )
        .map { case (status, _) =>
          status shouldBe StatusCodes.OK
        }
    } yield succeed
  }

  private def assertExerciseResponseArchivedContract(
      exerciseResponse: JsValue,
      exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId],
  ): Assertion = {
    inside(exerciseResponse) { case result @ JsObject(_) =>
      inside(SprayJson.decode[domain.ExerciseResponse[JsValue]](result)) {
        case \/-(domain.ExerciseResponse(exerciseResult, List(contract1))) =>
          exerciseResult shouldBe JsObject()
          inside(contract1) { case domain.Contract(-\/(archivedContract)) =>
            (archivedContract.contractId.unwrap: String) shouldBe (exercise.reference.contractId.unwrap: String)
          }
      }
    }
  }

  "should be able to serialize and deserialize domain commands" in withHttpServiceAndClient {
    (uri, _, _, client, ledgerId) =>
      instanceUUIDLogCtx(implicit lc =>
        jsonCodecs(client, ledgerId, Some(jwtAdminNoParty)).flatMap { case (encoder, decoder) =>
          testCreateCommandEncodingDecoding(uri)(encoder, decoder, ledgerId) *>
            testExerciseCommandEncodingDecoding(uri)(
              encoder,
              decoder,
              ledgerId,
            )
        }: Future[Assertion]
      )
  }

  private def testCreateCommandEncodingDecoding(uri: Uri)(
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder,
      ledgerId: LedgerId,
  ): Future[Assertion] = instanceUUIDLogCtx { implicit lc =>
    import json.JsonProtocol._
    import util.ErrorOps._

    val command0: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand("Alice")

    type F[A] = EitherT[Future, JsonError, A]
    val x: F[Assertion] = for {
      jsVal <- EitherT.either(
        encoder.encodeCreateCommand(command0).liftErr(JsonError)
      ): F[JsValue]
      command1 <- (EitherT.rightT(jwt(uri)): F[Jwt])
        .flatMap(decoder.decodeCreateCommand(jsVal, _, ledgerId))
    } yield command1.bimap(removeRecordId, removePackageId) should ===(command0)

    (x.run: Future[JsonError \/ Assertion]).map(_.fold(e => fail(e.shows), identity))
  }

  private def testExerciseCommandEncodingDecoding(uri: Uri)(
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder,
      ledgerId: LedgerId,
  ): Future[Assertion] = {
    val command0 = iouExerciseTransferCommand(lar.ContractId("#a-contract-ID"))
    val jsVal: JsValue = encodeExercise(encoder)(command0)
    val command1 =
      jwt(uri).flatMap(decodeExercise(decoder, _, ledgerId)(jsVal))
    command1.map(_.bimap(removeRecordId, identity) should ===(command0))
  }

  "request non-existent endpoint should return 404 with errors" in withHttpService {
    (uri: Uri, _, _, _) =>
      val badUri = uri.withPath(Uri.Path("/contracts/does-not-exist"))
      getRequestWithMinimumAuth(uri = badUri)
        .flatMap { case (status, output) =>
          status shouldBe StatusCodes.NotFound
          assertStatus(output, StatusCodes.NotFound)
          expectedOneErrorMessage(
            output
          ) shouldBe s"${HttpMethods.GET: HttpMethod}, uri: ${badUri: Uri}"
        }: Future[Assertion]
  }

  "parties endpoint should return all known parties" in withHttpServiceAndClient {
    (uri, _, _, client, _) =>
      val partyIds = Vector("P1", "P2", "P3", "P4")
      val partyManagement = client.partyManagementClient

      partyIds
        .traverse { p =>
          partyManagement.allocateParty(Some(p), Some(s"$p & Co. LLC"))
        }
        .flatMap { allocatedParties =>
          getRequest(
            uri = uri.withPath(Uri.Path("/v1/parties")),
            headers = headersWithAdminAuth,
          )
            .flatMap { case (status, output) =>
              status shouldBe StatusCodes.OK
              inside(
                decode1[domain.OkResponse, List[domain.PartyDetails]](output)
              ) { case \/-(response) =>
                response.status shouldBe StatusCodes.OK
                response.warnings shouldBe empty
                val actualIds: Set[domain.Party] = response.result.view.map(_.identifier).toSet
                actualIds should contain allElementsOf domain.Party.subst(partyIds.toSet)
                response.result.toSet should contain allElementsOf
                  allocatedParties.toSet.map(domain.PartyDetails.fromLedgerApi)
              }
            }
        }: Future[Assertion]
  }

  "parties endpoint should return only requested parties, unknown parties returned as warnings" in withHttpServiceAndClient {
    (uri, _, _, client, _) =>
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
          postJsonRequest(
            uri = uri.withPath(Uri.Path("/v1/parties")),
            JsArray(requestedPartyIds.map(x => JsString(x.unwrap))),
            headersWithAdminAuth,
          ).flatMap { case (status, output) =>
            status shouldBe StatusCodes.OK
            inside(
              decode1[domain.OkResponse, List[domain.PartyDetails]](output)
            ) { case \/-(response) =>
              response.status shouldBe StatusCodes.OK
              response.warnings shouldBe Some(domain.UnknownParties(List(erin)))
              val actualIds: Set[domain.Party] = response.result.view.map(_.identifier).toSet
              actualIds shouldBe requestedPartyIds.toSet - erin // Erin is not known
              val expected: Set[domain.PartyDetails] = allocatedParties.toSet
                .map(domain.PartyDetails.fromLedgerApi)
                .filterNot(_.identifier == charlie)
              response.result.toSet shouldBe expected
            }
          }
        }: Future[Assertion]
  }

  "parties endpoint should error if empty array passed as input" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
      postJsonRequestWithMinimumAuth(
        uri = uri.withPath(Uri.Path("/v1/parties")),
        JsArray(Vector.empty),
      ).flatMap { case (status, output) =>
        status shouldBe StatusCodes.BadRequest
        assertStatus(output, StatusCodes.BadRequest)
        val errorMsg = expectedOneErrorMessage(output)
        errorMsg should include("Cannot read JSON: <[]>")
        errorMsg should include("must be a JSON array with at least 1 element")
      }: Future[Assertion]
  }

  "parties endpoint returns error if empty party string passed" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
      val requestedPartyIds: Vector[domain.Party] = domain.Party.subst(Vector(""))

      postJsonRequestWithMinimumAuth(
        uri = uri.withPath(Uri.Path("/v1/parties")),
        JsArray(requestedPartyIds.map(x => JsString(x.unwrap))),
      ).flatMap { case (status, output) =>
        status shouldBe StatusCodes.BadRequest
        inside(decode1[domain.SyncResponse, List[domain.PartyDetails]](output)) {
          case \/-(domain.ErrorResponse(List(error), None, StatusCodes.BadRequest, _)) =>
            error should include("Daml-LF Party is empty")
        }
      }: Future[Assertion]
  }

  "parties endpoint returns empty result with warnings and OK status if nothing found" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
      val requestedPartyIds: Vector[domain.Party] =
        Vector(getUniqueParty("Alice"), getUniqueParty("Bob"))

      postJsonRequest(
        uri = uri.withPath(Uri.Path("/v1/parties")),
        JsArray(requestedPartyIds.map(x => JsString(x.unwrap))),
        headers = headersWithAdminAuth,
      ).flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        inside(decode1[domain.SyncResponse, List[domain.PartyDetails]](output)) {
          case \/-(domain.OkResponse(List(), Some(warnings), StatusCodes.OK)) =>
            inside(warnings) { case domain.UnknownParties(unknownParties) =>
              unknownParties.toSet shouldBe requestedPartyIds.toSet
            }
        }
      }: Future[Assertion]
  }

  "parties/allocate should allocate a new party" in withHttpServiceAndClient { (uri, _, _, _, _) =>
    val request = domain.AllocatePartyRequest(
      Some(domain.Party(s"Carol${uniqueId()}")),
      Some("Carol & Co. LLC"),
    )
    val json = SprayJson.encode(request).valueOr(e => fail(e.shows))
    postJsonRequest(
      uri = uri.withPath(Uri.Path("/v1/parties/allocate")),
      json = json,
      headers = headersWithAdminAuth,
    )
      .flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        inside(decode1[domain.OkResponse, domain.PartyDetails](output)) { case \/-(response) =>
          response.status shouldBe StatusCodes.OK
          val newParty = response.result
          Some(newParty.identifier) shouldBe request.identifierHint
          newParty.displayName shouldBe request.displayName
          newParty.isLocal shouldBe true
          getRequest(
            uri = uri.withPath(Uri.Path("/v1/parties")),
            headersWithAdminAuth,
          )
            .flatMap { case (status, output) =>
              status shouldBe StatusCodes.OK
              inside(decode1[domain.OkResponse, List[domain.PartyDetails]](output)) {
                case \/-(response) =>
                  response.status shouldBe StatusCodes.OK
                  response.result should contain(newParty)
              }
            }
        }
      }: Future[Assertion]
  }

  "parties/allocate should allocate a new party without any hints" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
      postJsonRequest(
        uri = uri.withPath(Uri.Path("/v1/parties/allocate")),
        json = JsObject(),
        headers = headersWithAdminAuth,
      )
        .flatMap { case (status, output) =>
          status shouldBe StatusCodes.OK
          inside(decode1[domain.OkResponse, domain.PartyDetails](output)) { case \/-(response) =>
            response.status shouldBe StatusCodes.OK
            val newParty = response.result
            newParty.identifier.unwrap.length should be > 0
            newParty.displayName shouldBe None
            newParty.isLocal shouldBe true

            getRequest(uri = uri.withPath(Uri.Path("/v1/parties")), headers = headersWithAdminAuth)
              .flatMap { case (status, output) =>
                status shouldBe StatusCodes.OK
                inside(decode1[domain.OkResponse, List[domain.PartyDetails]](output)) {
                  case \/-(response) =>
                    response.status shouldBe StatusCodes.OK
                    response.result should contain(newParty)
                }
              }
          }
        }: Future[Assertion]
  }

  // TEST_EVIDENCE: Authorization: badly-authorized create is rejected
  "parties/allocate should return BadRequest error if party ID hint is invalid PartyIdString" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
      val request = domain.AllocatePartyRequest(
        Some(domain.Party(s"Carol-!")),
        Some("Carol & Co. LLC"),
      )
      val json = SprayJson.encode(request).valueOr(e => fail(e.shows))

      postJsonRequest(
        uri = uri.withPath(Uri.Path("/v1/parties/allocate")),
        json = json,
        headers = headersWithAdminAuth,
      )
        .flatMap { case (status, output) =>
          status shouldBe StatusCodes.BadRequest
          inside(decode[domain.ErrorResponse](output)) { case \/-(response) =>
            response.status shouldBe StatusCodes.BadRequest
            response.warnings shouldBe empty
            response.errors.length shouldBe 1
          }
        }
  }

  "fetch by contractId" in withHttpService { (uri, encoder, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)

      postCreateCommand(command, encoder, uri, headers).flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val contractId: ContractId = getContractId(getResult(output))
        val locator = domain.EnrichedContractId(None, contractId)
        lookupContractAndAssert(locator, contractId, command, encoder, uri, headers)
      }: Future[Assertion]
    }
  }

  "fetch returns {status:200, result:null} when contract is not found" in withHttpService {
    (uri, _, _, _) =>
      getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
        val accountNumber = "abc123"
        val locator = domain.EnrichedContractKey(
          TpId.Account.Account,
          JsArray(JsString(alice.unwrap), JsString(accountNumber)),
        )
        postContractsLookup(locator, uri.withPath(Uri.Path("/v1/fetch")), headers).flatMap {
          case (status, output) =>
            status shouldBe StatusCodes.OK
            assertStatus(output, StatusCodes.OK)
            output
              .asJsObject(s"expected JsObject, got: $output")
              .fields
              .get("result") shouldBe Some(JsNull)
        }: Future[Assertion]
      }
  }

  // TEST_EVIDENCE: Authorization: fetch fails when readAs not authed, even if prior fetch succeeded
  "fetch fails when readAs not authed, even if prior fetch succeeded" in withHttpService {
    (uri, encoder, _, _) =>
      for {
        res <- getUniquePartyAndAuthHeaders(uri)("Alice")
        (alice, aliceHeaders) = res
        command = iouCreateCommand(alice.unwrap)
        createStatusOutput <- postCreateCommand(command, encoder, uri, aliceHeaders)
        contractId = {
          val (status, output) = createStatusOutput
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          getContractId(getResult(output))
        }
        locator = domain.EnrichedContractId(None, contractId)
        // will cache if DB configured
        _ <- lookupContractAndAssert(locator, contractId, command, encoder, uri, aliceHeaders)
        charlie = getUniqueParty("Charlie")
        badLookup <- postContractsLookup(
          locator,
          uri.withPath(Uri.Path("/v1/fetch")),
          aliceHeaders,
          readAs = Some(List(charlie)),
        )
        _ = {
          val (status, output) = badLookup
          status shouldBe StatusCodes.Unauthorized
          assertStatus(output, StatusCodes.Unauthorized)
          output
            .asJsObject(s"expected JsObject, got: $output")
            .fields
            .keySet should ===(Set("errors", "status"))
        }
      } yield succeed
  }

  "fetch by key" in withHttpService { (uri, encoder, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val accountNumber = "abc123"
      val command: domain.CreateCommand[v.Record, OptionalPkg] =
        accountCreateCommand(alice, accountNumber)

      postCreateCommand(command, encoder, uri, headers).flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val contractId: ContractId = getContractId(getResult(output))
        val locator = domain.EnrichedContractKey(
          TpId.Account.Account,
          JsArray(JsString(alice.unwrap), JsString(accountNumber)),
        )
        lookupContractAndAssert(locator, contractId, command, encoder, uri, headers)
      }: Future[Assertion]
    }
  }

  "commands/exercise Archive by key" in withHttpService { (uri, encoder, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val accountNumber = "abc123"
      val create: domain.CreateCommand[v.Record, OptionalPkg] =
        accountCreateCommand(alice, accountNumber)

      val keyRecord = v.Record(
        fields = Seq(
          v.RecordField(value = Some(v.Value(v.Value.Sum.Party(alice.unwrap)))),
          v.RecordField(value = Some(v.Value(v.Value.Sum.Text(accountNumber)))),
        )
      )
      val locator = domain.EnrichedContractKey[v.Value](
        TpId.Account.Account,
        v.Value(v.Value.Sum.Record(keyRecord)),
      )
      val archive: domain.ExerciseCommand[v.Value, domain.EnrichedContractKey[v.Value]] =
        archiveCommand(locator)
      val archiveJson: JsValue = encodeExercise(encoder)(archive)

      postCreateCommand(create, encoder, uri, headers).flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)

        postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), archiveJson, headers).flatMap {
          case (exerciseStatus, exerciseOutput) =>
            exerciseStatus shouldBe StatusCodes.OK
            assertStatus(exerciseOutput, StatusCodes.OK)
        }
      }: Future[Assertion]
    }
  }

  "fetch by key containing variant and record, encoded as array with number num" in withHttpService {
    (uri, _, _, _) =>
      getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
        testFetchByCompositeKey(
          uri,
          jsObject(s"""{
            "templateId": "Account:KeyedByVariantAndRecord",
            "key": [
              "$alice",
              {"tag": "Bar", "value": 42},
              {"baz": "another baz value"}
            ]
          }"""),
          alice,
          headers,
        )
      }
  }

  "fetch by key containing variant and record, encoded as record with string num" in withHttpService {
    (uri, _, _, _) =>
      getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
        testFetchByCompositeKey(
          uri,
          jsObject(s"""{
            "templateId": "Account:KeyedByVariantAndRecord",
            "key": {
              "_1": "$alice",
              "_2": {"tag": "Bar", "value": "42"},
              "_3": {"baz": "another baz value"}
            }
          }"""),
          alice,
          headers,
        )
      }
  }

  private def testFetchByCompositeKey(
      uri: Uri,
      request: JsObject,
      party: domain.Party,
      headers: List[HttpHeader],
  ) = {
    val createCommand = jsObject(s"""{
        "templateId": "Account:KeyedByVariantAndRecord",
        "payload": {
          "name": "ABC DEF",
          "party": "${party.unwrap}",
          "age": 123,
          "fooVariant": {"tag": "Bar", "value": 42},
          "bazRecord": {"baz": "another baz value"}
        }
      }""")
    postJsonRequest(uri.withPath(Uri.Path("/v1/create")), createCommand, headers).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val contractId: ContractId = getContractId(getResult(output))

        postJsonRequest(uri.withPath(Uri.Path("/v1/fetch")), request, headers).flatMap {
          case (status, output) =>
            status shouldBe StatusCodes.OK
            assertStatus(output, StatusCodes.OK)
            activeContract(output).contractId shouldBe contractId
        }
    }: Future[Assertion]
  }

  "query by a variant field" in withHttpService { (uri, encoder, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      val accountNumber = "abc123"
      val now = TimestampConversion.roundInstantToMicros(Instant.now)
      val nowStr = TimestampConversion.microsToInstant(now).toString
      val command: domain.CreateCommand[v.Record, OptionalPkg] =
        accountCreateCommand(alice, accountNumber, now)

      val packageId: Ref.PackageId = MetadataReader
        .templateByName(metadata2)(Ref.QualifiedName.assertFromString("Account:Account"))
        .headOption
        .map(_._1)
        .getOrElse(fail(s"Cannot retrieve packageId"))

      postCreateCommand(command, encoder, uri, headers).flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val contractId: ContractId = getContractId(getResult(output))

        val query = jsObject(s"""{
             "templateIds": ["$packageId:Account:Account"],
             "query": {
                 "number" : "abc123",
                 "status" : {"tag": "Enabled", "value": "${nowStr: String}"}
             }
          }""")

        postJsonRequest(uri.withPath(Uri.Path("/v1/query")), query, headers).map {
          case (searchStatus, searchOutput) =>
            searchStatus shouldBe StatusCodes.OK
            assertStatus(searchOutput, StatusCodes.OK)
            inside(activeContractList(searchOutput)) { case List(ac) =>
              ac.contractId shouldBe contractId
            }
        }
      }: Future[Assertion]
    }
  }

  "packages endpoint should return all known package IDs" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
      getAllPackageIds(uri).map { x =>
        inside(x) {
          case domain.OkResponse(ps, None, StatusCodes.OK) if ps.nonEmpty =>
            Inspectors.forAll(ps)(_.length should be > 0)
        }
      }: Future[Assertion]
  }

  "packages/packageId should return a requested package" in withHttpServiceAndClient {
    import AbstractHttpServiceIntegrationTestFuns.sha256
    (uri, _, _, _, _) =>
      getAllPackageIds(uri).flatMap { okResp =>
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

  "packages/packageId should return NotFound if a non-existing package is requested" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
      Http()
        .singleRequest(
          HttpRequest(
            method = HttpMethods.GET,
            uri = uri.withPath(Uri.Path(s"/v1/packages/12345678")),
            headers = headersWithAdminAuth,
          )
        )
        .map { resp =>
          resp.status shouldBe StatusCodes.NotFound
        }
  }

  "packages upload endpoint" in withHttpServiceAndClient { (uri, _, _, _, _) =>
    val newDar = AbstractHttpServiceIntegrationTestFuns.dar3

    getAllPackageIds(uri).flatMap { okResp =>
      val existingPackageIds: Set[String] = okResp.result.toSet
      uploadPackage(uri)(newDar)
        .flatMap { _ =>
          getAllPackageIds(uri).map { okResp =>
            val newPackageIds: Set[String] = okResp.result.toSet -- existingPackageIds
            newPackageIds.size should be > 0
          }
        }
    }: Future[Assertion]
  }

  "package list is updated when a query request is made" in usingLedger(testId) {
    case (ledgerPort, _, _) =>
      HttpServiceTestFixture.withHttpService(
        testId,
        ledgerPort,
        jdbcConfig,
        staticContentConfig,
        useTls = useTls,
        wsConfig = wsConfig,
        token = Some(jwtAdminNoParty),
      ) { case (uri, _, _, _) =>
        for {
          alicePartyAndAuthHeaders <- getUniquePartyAndAuthHeaders(uri)("Alice")
          (alice, headers) = alicePartyAndAuthHeaders
          _ <- withHttpServiceOnly(ledgerPort) { (uri, encoder, _) =>
            val searchDataSet = genSearchDataSet(alice)
            searchDataSet.traverse(c => postCreateCommand(c, encoder, uri, headers)).flatMap { rs =>
              rs.map(_._1) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)
            }
          }
          _ <- withHttpServiceOnly(ledgerPort) { (uri, _, _) =>
            getRequest(uri = uri.withPath(Uri.Path("/v1/query")), headers)
              .flatMap { case (status, output) =>
                status shouldBe StatusCodes.OK
                assertStatus(output, StatusCodes.OK)
                inside(getResult(output)) { case JsArray(result) =>
                  result should have length 4
                }
              }: Future[Assertion]
          }
        } yield succeed
      }
  }

  // TEST_EVIDENCE: Performance: archiving a large number of contracts should succeed
  "archiving a large number of contracts should succeed" in withHttpServiceAndClient(
    StartSettings.DefaultMaxInboundMessageSize * 10
  ) { (uri, encoder, _, _, _) =>
    getUniquePartyAndAuthHeaders(uri)("Alice").flatMap { case (alice, headers) =>
      // The numContracts size should test for https://github.com/digital-asset/daml/issues/10339
      val numContracts: Long = 2000
      val helperId = domain.TemplateId(None, "Account", "Helper")
      val payload = recordFromFields(ShRecord(owner = v.Value.Sum.Party(alice.unwrap)))
      val createCmd: domain.CreateAndExerciseCommand[v.Record, v.Value, OptionalPkg] =
        domain.CreateAndExerciseCommand(
          templateId = helperId,
          payload = payload,
          choice = lar.Choice("CreateN"),
          argument = boxedRecord(recordFromFields(ShRecord(n = v.Value.Sum.Int64(numContracts)))),
          meta = None,
        )

      def encode(cmd: domain.CreateAndExerciseCommand[v.Record, v.Value, OptionalPkg]): JsValue =
        encoder.encodeCreateAndExerciseCommand(cmd).valueOr(e => fail(e.shows))

      def archiveCmd(cids: List[String]) =
        domain.CreateAndExerciseCommand(
          templateId = helperId,
          payload = payload,
          choice = lar.Choice("ArchiveAll"),
          argument = boxedRecord(
            recordFromFields(
              ShRecord(cids =
                lfToApi(
                  VAx
                    .seq(VA.contractId)
                    .inj(cids map lfv.Value.ContractId.assertFromString)
                ).sum
              )
            )
          ),
          meta = None,
        )

      def queryN(n: Long): Future[Assertion] = postJsonRequest(
        uri.withPath(Uri.Path("/v1/query")),
        jsObject("""{"templateIds": ["Account:Account"]}"""),
        headers,
      ).flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        inside(getResult(output)) { case JsArray(result) =>
          result should have length n
        }
      }

      for {
        resp <- postJsonRequest(
          uri.withPath(Uri.Path("/v1/create-and-exercise")),
          encode(createCmd),
          headers,
        )
        (status, output) = resp
        _ = {
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
        }
        created = getChild(getResult(output), "exerciseResult").convertTo[List[String]]
        _ = created should have length numContracts

        _ <- queryN(numContracts)

        status <- postJsonRequest(
          uri.withPath(Uri.Path("/v1/create-and-exercise")),
          encode(archiveCmd(created)),
          headers,
        ).map(_._1)
        _ = {
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
        }

        _ <- queryN(0)
      } yield succeed
    }
  }

  "Should ignore conflicts on contract key hash constraint violation" in withHttpServiceAndClient {
    (uri, encoder, _, _, _) =>
      import com.daml.ledger.api.refinements.{ApiTypes => lar}
      import shapeless.record.{Record => ShRecord}

      val partyIds = Vector("Alice", "Bob").map(getUniqueParty)
      val packageId: Ref.PackageId = MetadataReader
        .templateByName(metadataUser)(Ref.QualifiedName.assertFromString("User:User"))
        .collectFirst { case (pkgid, _) => pkgid }
        .getOrElse(fail(s"Cannot retrieve packageId"))

      def userCreateCommand(
          username: domain.Party,
          following: Seq[domain.Party] = Seq.empty,
      ): domain.CreateCommand[v.Record, domain.TemplateId.OptionalPkg] = {
        val followingList = lfToApi(
          VAx.seq(VAx.partyStr).inj(domain.Party unsubst following)
        ).sum
        val arg = recordFromFields(
          ShRecord(
            username = v.Value.Sum.Party(username.unwrap),
            following = followingList,
          )
        )

        domain.CreateCommand(TpId.User.User, arg, None)
      }

      def userExerciseFollowCommand(
          contractId: lar.ContractId,
          toFollow: domain.Party,
      ): domain.ExerciseCommand[v.Value, domain.EnrichedContractId] = {
        val reference = domain.EnrichedContractId(Some(TpId.User.User), contractId)
        val arg = recordFromFields(ShRecord(userToFollow = v.Value.Sum.Party(toFollow.unwrap)))
        val choice = lar.Choice("Follow")

        domain.ExerciseCommand(reference, choice, boxedRecord(arg), None)
      }

      def followUser(contractId: lar.ContractId, actAs: domain.Party, toFollow: domain.Party) = {
        val exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId] =
          userExerciseFollowCommand(contractId, toFollow)
        val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

        headersWithPartyAuth(uri)(actAs = List(actAs.unwrap))
          .flatMap(headers =>
            postJsonRequest(
              uri.withPath(Uri.Path("/v1/exercise")),
              exerciseJson,
              headers,
            )
          )
          .map { case (exerciseStatus, exerciseOutput) =>
            exerciseStatus shouldBe StatusCodes.OK
            assertStatus(exerciseOutput, StatusCodes.OK)
            ()
          }

      }

      def queryUsers(fromPerspectiveOfParty: domain.Party) = {
        val query = jsObject(s"""{
             "templateIds": ["$packageId:User:User"],
             "query": {}
          }""")

        headersWithPartyAuth(uri)(actAs = List(fromPerspectiveOfParty.unwrap))
          .flatMap(headers =>
            postJsonRequest(
              uri.withPath(Uri.Path("/v1/query")),
              query,
              headers,
            )
          )
          .map { case (searchStatus, searchOutput) =>
            searchStatus shouldBe StatusCodes.OK
            assertStatus(searchOutput, StatusCodes.OK)
          }
      }

      val commands = partyIds.map { p =>
        (p, userCreateCommand(p))
      }

      for {
        users <- commands.traverse { case (party, command) =>
          val fut = headersWithPartyAuth(uri)(actAs = List(party.unwrap))
            .flatMap(headers =>
              postCreateCommand(
                command,
                encoder,
                uri,
                headers,
              )
            )
            .map { case (status, output) =>
              status shouldBe StatusCodes.OK
              assertStatus(output, StatusCodes.OK)
              getContractId(getResult(output))
            }: Future[ContractId]
          fut.map(cid => (party, cid))
        }
        (alice, aliceUserId) = users(0)
        (bob, bobUserId) = users(1)
        _ <- followUser(aliceUserId, alice, bob)
        _ <- queryUsers(bob)
        _ <- followUser(bobUserId, bob, alice)
        _ <- queryUsers(alice)
      } yield succeed
  }

}
