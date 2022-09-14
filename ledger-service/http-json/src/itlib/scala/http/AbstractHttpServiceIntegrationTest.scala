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
import com.daml.http.endpoints.MeteringReportEndpoint.MeteringReportDateRequest
import com.daml.http.json.SprayJson.objectField
import com.daml.http.json._
import com.daml.http.util.ClientUtil.{boxedRecord, uniqueId}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.v1.{value => v}
import com.daml.ledger.service.MetadataReader
import com.daml.http.util.Logging.instanceUUIDLogCtx
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
import com.daml.lf.{value => lfv}
import com.daml.scalautil.Statement.discard
import com.google.protobuf.struct.Struct
import lfv.test.TypedValueGenerators.{ValueAddend => VA}

import java.util.UUID

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

  "get all parties using the legacy token format" in withHttpService { fixture =>
    import fixture.client
    val partyIds = Vector("P1", "P2", "P3", "P4").map(getUniqueParty(_).unwrap)
    val partyManagement = client.partyManagementClient
    partyIds
      .traverse { p =>
        partyManagement.allocateParty(Some(p), Some(s"$p & Co. LLC"))
      }
      .flatMap { allocatedParties =>
        fixture
          .getRequest(
            Uri.Path("/v1/parties"),
            headersWithPartyAuthLegacyFormat(List()),
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

  "create should fail with custom tokens that contain no ledger id" in withHttpService { fixture =>
    import fixture.encoder
    val alice = getUniqueParty("Alice")
    val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice)
    val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

    val headers = HttpServiceTestFixture.authorizationHeader(
      HttpServiceTestFixture.jwtForParties(List("Alice"), List("Bob"), None, false, false)
    )

    fixture
      .postJsonRequest(
        Uri.Path("/v1/create"),
        input,
        headers,
      )
      .parseResponse[JsValue]
      .map(inside(_) { case domain.ErrorResponse(Seq(error), _, StatusCodes.Unauthorized, _) =>
        error shouldBe "ledgerId missing in access token"
      }): Future[Assertion]
  }

  "metering-report endpoint should return metering report" in withHttpService { fixture =>
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

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class AbstractHttpServiceIntegrationTestTokenIndependent
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with StrictLogging
    with AbstractHttpServiceIntegrationTestFuns {

  import AbstractHttpServiceIntegrationTestFuns.{VAx, UriFixture, HttpServiceTestFixtureData}
  import HttpServiceTestFixture.{UseTls, accountCreateCommand, archiveCommand}
  import json.JsonProtocol._
  import AbstractHttpServiceIntegrationTestFuns.ciouDar

  object CIou {
    val CIou: domain.TemplateId.OptionalPkg = domain.TemplateId(None, "CIou", "CIou")
  }

  override def useTls = UseTls.NoTls

  protected def genSearchDataSet(
      party: domain.Party
  ): List[domain.CreateCommand[v.Record, OptionalPkg]] =
    List(
      iouCreateCommand(amount = "111.11", currency = "EUR", partyName = party),
      iouCreateCommand(amount = "222.22", currency = "EUR", partyName = party),
      iouCreateCommand(amount = "333.33", currency = "GBP", partyName = party),
      iouCreateCommand(amount = "444.44", currency = "BTC", partyName = party),
    )

  "query GET" - {
    "empty results" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        fixture.searchAllExpectOk(headers).map { vector =>
          vector should have size 0L
        }
      }
    }

    "single-party with results" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val searchDataSet = genSearchDataSet(alice)
        searchDataSet.traverse(c => postCreateCommand(c, fixture, headers)).flatMap { rs =>
          rs.map(_.status) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

          fixture
            .getRequest(Uri.Path("/v1/query"), headers)
            .parseResponse[Vector[JsValue]]
            .map(inside(_) { case domain.OkResponse(vector, None, StatusCodes.OK) =>
              vector should have size searchDataSet.size.toLong
            }): Future[Assertion]
        }
      }
    }

    "multi-party" in withHttpService { fixture =>
      for {
        res1 <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, aliceHeaders) = res1
        res2 <- fixture.getUniquePartyAndAuthHeaders("Bob")
        (bob, bobHeaders) = res2
        _ <- postCreateCommand(
          accountCreateCommand(owner = alice, number = "42"),
          fixture,
          headers = aliceHeaders,
        ).map(r => r.status shouldBe StatusCodes.OK)
        _ <- postCreateCommand(
          accountCreateCommand(owner = bob, number = "23"),
          fixture,
          headers = bobHeaders,
        ).map(r => r.status shouldBe StatusCodes.OK)
        _ <- fixture.searchAllExpectOk(aliceHeaders).map(cs => cs should have size 1)
        _ <- fixture.searchAllExpectOk(bobHeaders).map(cs => cs should have size 1)
        _ <- fixture
          .headersWithPartyAuth(List(alice.unwrap, bob.unwrap))
          .flatMap(headers => fixture.searchAllExpectOk(headers))
          .map(cs => cs should have size 2)
      } yield succeed
    }
  }

  "query POST with empty query" - {
    "single party" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val searchDataSet = genSearchDataSet(alice)
        searchExpectOk(
          searchDataSet,
          jsObject("""{"templateIds": ["Iou:Iou"]}"""),
          fixture,
          headers,
        ).map { acl: List[domain.ActiveContract[JsValue]] =>
          acl.size shouldBe searchDataSet.size
        }
      }
    }

    "multi-party" in withHttpService { fixture =>
      for {
        res1 <- fixture.getUniquePartyAndAuthHeaders("Alice")
        res2 <- fixture.getUniquePartyAndAuthHeaders("Bob")
        (alice, aliceHeaders) = res1
        (bob, bobHeaders) = res2
        aliceAccountResp <- postCreateCommand(
          accountCreateCommand(owner = alice, number = "42"),
          fixture,
          aliceHeaders,
        )
        _ = aliceAccountResp.status shouldBe StatusCodes.OK
        bobAccountResp <- postCreateCommand(
          accountCreateCommand(owner = bob, number = "23"),
          fixture,
          bobHeaders,
        )
        _ = bobAccountResp.status shouldBe StatusCodes.OK
        _ <- searchExpectOk(
          List(),
          jsObject("""{"templateIds": ["Account:Account"]}"""),
          fixture,
          aliceHeaders,
        )
          .map(acl => acl.size shouldBe 1)
        _ <- searchExpectOk(
          List(),
          jsObject("""{"templateIds": ["Account:Account"]}"""),
          fixture,
          bobHeaders,
        )
          .map(acl => acl.size shouldBe 1)
        _ <- fixture
          .headersWithPartyAuth(List(alice.unwrap, bob.unwrap))
          .flatMap(headers =>
            searchExpectOk(
              List(),
              jsObject("""{"templateIds": ["Account:Account"]}"""),
              fixture,
              headers,
            )
          )
          .map(acl => acl.size shouldBe 2)
      } yield {
        assert(true)
      }
    }

    "with an interface ID" in withHttpService { fixture =>
      import com.daml.http.json.JsonProtocol._
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        aliceH <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, aliceHeaders) = aliceH
        _ <- postCreateCommand(
          iouCommand(alice, CIou.CIou),
          fixture,
          aliceHeaders,
        )

        searchResp <- search(
          List.empty,
          Map(
            "templateIds" -> Seq(TpId.IIou.IIou).toJson,
            "query" -> spray.json.JsObject(),
          ).toJson.asJsObject,
          fixture,
          aliceHeaders,
        )
      } yield inside(searchResp) {
        case domain.OkResponse(Seq(ac), None, StatusCodes.OK) => {
          discard {
            ac.templateId shouldBe TpId.IIou.IIou.copy(packageId = ac.templateId.packageId)
          }
          ac.payload shouldBe JsObject("amount" -> JsString("42"))
        }
      }
    }
  }

  "query with unknown Template IDs" - {
    "warns if some are known" in withHttpService { fixture =>
      val query =
        jsObject(
          """{"templateIds": ["Iou:Iou", "UnknownModule:UnknownEntity"], "query": {"currency": "EUR"}}"""
        )
      // TODO VM(#12922) https://github.com/digital-asset/daml/pull/12922#discussion_r815234434
      logger.info("query returns unknown Template IDs")
      fixture
        .headersWithPartyAuth(List("UnknownParty"))
        .flatMap(headers =>
          search(List(), query, fixture, headers).map { response =>
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

    "fails if all are unknown" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        search(
          genSearchDataSet(alice),
          jsObject("""{"templateIds": ["AAA:BBB", "XXX:YYY"]}"""),
          fixture,
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
  }

  "query record contains handles" - {
    def randomTextN(n: Int) = {
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
      s"'$testLbl' strings properly" in withHttpService { fixture =>
        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
          searchExpectOk(
            genSearchDataSet(alice) :+ iouCreateCommand(
              currency = testCurrency,
              partyName = alice,
            ),
            jsObject(
              s"""{"templateIds": ["Iou:Iou"], "query": {"currency": ${testCurrency.toJson}}}"""
            ),
            fixture,
            headers,
          ).map(inside(_) { case Seq(domain.ActiveContract(_, _, _, JsObject(fields), _, _, _)) =>
            fields.get("currency") should ===(Some(JsString(testCurrency)))
          })
        }
      }
    }
  }

  "query with filter" - {
    "one field" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val searchDataSet = genSearchDataSet(alice)
        searchExpectOk(
          searchDataSet,
          jsObject("""{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR"}}"""),
          fixture,
          headers,
        ).map { acl: List[domain.ActiveContract[JsValue]] =>
          acl.size shouldBe 2
          acl.map(a => objectField(a.payload, "currency")) shouldBe List.fill(2)(
            Some(JsString("EUR"))
          )
        }
      }
    }

    "can use number or string for numeric field" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val searchDataSet = genSearchDataSet(alice)
        searchDataSet.traverse(c => postCreateCommand(c, fixture, headers)).flatMap {
          rs: List[domain.SyncResponse[_]] =>
            rs.map(_.status) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

            def queryAmountAs(s: String) =
              jsObject(s"""{"templateIds": ["Iou:Iou"], "query": {"amount": $s}}""")

            val queryAmountAsString = queryAmountAs("\"111.11\"")
            val queryAmountAsNumber = queryAmountAs("111.11")

            List(queryAmountAsString, queryAmountAsNumber)
              .map(q =>
                fixture
                  .postJsonRequest(Uri.Path("/v1/query"), q, headers)
                  .parseResponse[List[domain.ActiveContract[JsValue]]]
              )
              .sequence
              .map(inside(_) {
                case Seq(
                      jsVal1 @ domain.OkResponse(acl1 @ List(ac), _, StatusCodes.OK),
                      jsVal2 @ domain.OkResponse(acl2, _, _),
                    ) =>
                  acl1 shouldBe acl2
                  jsVal1 shouldBe jsVal2
                  objectField(ac.payload, "amount") shouldBe Some(JsString("111.11"))
              })
        }: Future[Assertion]
      }
    }

    "two fields" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val searchDataSet = genSearchDataSet(alice)
        searchExpectOk(
          searchDataSet,
          jsObject(
            """{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR", "amount": "111.11"}}"""
          ),
          fixture,
          headers,
        ).map { acl: List[domain.ActiveContract[JsValue]] =>
          acl.size shouldBe 1
          acl.map(a => objectField(a.payload, "currency")) shouldBe List(Some(JsString("EUR")))
          acl.map(a => objectField(a.payload, "amount")) shouldBe List(Some(JsString("111.11")))
        }
      }
    }

    "no results" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val searchDataSet = genSearchDataSet(alice)
        searchExpectOk(
          searchDataSet,
          jsObject(
            """{"templateIds": ["Iou:Iou"], "query": {"currency": "RUB", "amount": "666.66"}}"""
          ),
          fixture,
          headers,
        ).map { acl: List[domain.ActiveContract[JsValue]] =>
          acl.size shouldBe 0
        }
      }
    }

    "by a variant field" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
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

        postCreateCommand(command, fixture, headers).flatMap(inside(_) {
          case domain.OkResponse(result, _, StatusCodes.OK) =>
            val contractId: ContractId = result.contractId

            val query = jsObject(s"""{
             "templateIds": ["$packageId:Account:Account"],
             "query": {
                 "number" : "abc123",
                 "status" : {"tag": "Enabled", "value": "${nowStr: String}"}
             }
          }""")

            fixture
              .postJsonRequest(Uri.Path("/v1/query"), query, headers)
              .parseResponse[List[domain.ActiveContract[JsValue]]]
              .map(inside(_) { case domain.OkResponse(List(ac), _, StatusCodes.OK) =>
                ac.contractId shouldBe contractId
              })
        }): Future[Assertion]
      }
    }

    "nested comparison filters" - {
      import shapeless.Coproduct, shapeless.syntax.singleton._
      val irrelevant = Ref.Identifier assertFromString "none:Discarded:Identifier"
      val (_, bazRecordVA) = VA.record(irrelevant, ShRecord(baz = VA.text))
      val (_, fooVA) =
        VA.variant(irrelevant, ShRecord(Bar = VA.int64, Baz = bazRecordVA, Qux = VA.unit))
      val fooVariant = Coproduct[fooVA.Inj]
      val (_, kbvarVA) = VA.record(
        irrelevant,
        ShRecord(
          name = VA.text,
          party = VAx.partyDomain,
          age = VA.int64,
          fooVariant = fooVA,
          bazRecord = bazRecordVA,
        ),
      )

      def withBazRecord(bazRecord: VA.text.Inj)(p: domain.Party): kbvarVA.Inj =
        ShRecord(
          name = "ABC DEF",
          party = p,
          age = 123L,
          fooVariant = fooVariant(Symbol("Bar") ->> 42L),
          bazRecord = ShRecord(baz = bazRecord),
        )

      def withFooVariant(v: VA.int64.Inj)(p: domain.Party): kbvarVA.Inj =
        ShRecord(
          name = "ABC DEF",
          party = p,
          age = 123L,
          fooVariant = fooVariant(Symbol("Bar") ->> v),
          bazRecord = ShRecord(baz = "another baz value"),
        )

      final class Scenario[Inj](
          val label: String,
          val ctId: domain.ContractTypeId.OptionalPkg,
          val va: VA.Aux[Inj],
          val query: Map[String, JsValue],
          val matches: domain.Party => Inj,
          val doesNotMatch: domain.Party => Inj,
      )
      def Scenario(
          label: String,
          ctId: domain.ContractTypeId.OptionalPkg,
          va: VA,
          query: Map[String, JsValue],
      )(
          matches: domain.Party => va.Inj,
          doesNotMatch: domain.Party => va.Inj,
      ): Scenario[va.Inj] =
        new Scenario(label, ctId, va, query, matches, doesNotMatch)

      val kbvarId = TpId.Account.KeyedByVariantAndRecord
      Seq(
        Scenario(
          "gt string",
          kbvarId,
          kbvarVA,
          Map("bazRecord" -> Map("baz" -> Map("%gt" -> "b")).toJson),
        )(
          withBazRecord("c"),
          withBazRecord("a"),
        ),
        Scenario(
          "gt int",
          kbvarId,
          kbvarVA,
          Map("fooVariant" -> Map("tag" -> "Bar".toJson, "value" -> Map("%gt" -> 2).toJson).toJson),
        )(withFooVariant(3), withFooVariant(1)),
      ).zipWithIndex.foreach { case (scenario, ix) =>
        import scenario._
        s"$label (scenario $ix)" in withHttpService { fixture =>
          for {
            (alice, headers) <- fixture.getUniquePartyAndAuthHeaders("Alice")
            contracts <- searchExpectOk(
              List(matches, doesNotMatch).map { payload =>
                domain.CreateCommand(ctId, argToApi(va)(payload(alice)), None)
              },
              JsObject(Map("templateIds" -> Seq(ctId).toJson, "query" -> query.toJson)),
              fixture,
              headers,
            )
          } yield contracts.map(_.payload) should contain theSameElementsAs Seq(
            LfValueCodec.apiValueToJsValue(va.inj(matches(alice)))
          )
        }
      }
    }
  }

  "query with invalid JSON query should return error" in withHttpService { fixture =>
    fixture
      .postJsonStringRequest(Uri.Path("/v1/query"), "{NOT A VALID JSON OBJECT")
      .parseResponse[JsValue]
      .map(inside(_) { case domain.ErrorResponse(_, _, StatusCodes.BadRequest, _) =>
        succeed
      }): Future[Assertion]
  }

  protected implicit final class `AHS TI uri funs`(private val fixture: UriFixture) {

    def searchAllExpectOk(
        headers: List[HttpHeader]
    ): Future[List[domain.ActiveContract[JsValue]]] =
      searchAll(headers).map(expectOk(_))

    def searchAllExpectOk(
    ): Future[List[domain.ActiveContract[JsValue]]] =
      fixture.headersWithAuth.flatMap(searchAllExpectOk(_))

    def searchAll(
        headers: List[HttpHeader]
    ): Future[domain.SyncResponse[List[domain.ActiveContract[JsValue]]]] =
      fixture
        .getRequest(Uri.Path("/v1/query"), headers)
        .parseResponse[List[domain.ActiveContract[JsValue]]]

  }

  "create" - {
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

  "exercise" - {
    "succeeds normally" in withHttpService { fixture =>
      import fixture.encoder
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val create: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice)
        postCreateCommand(create, fixture, headers)
          .flatMap(inside(_) { case domain.OkResponse(createResult, _, StatusCodes.OK) =>
            val exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId] =
              iouExerciseTransferCommand(createResult.contractId)
            val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

            fixture
              .postJsonRequest(Uri.Path("/v1/exercise"), exerciseJson, headers)
              .parseResponse[domain.ExerciseResponse[JsValue]]
              .flatMap(inside(_) { case domain.OkResponse(result, _, StatusCodes.OK) =>
                assertExerciseResponseNewActiveContract(
                  result,
                  create,
                  exercise,
                  fixture,
                  headers,
                )
              })
          }): Future[Assertion]
      }
    }

    "with unknown contractId should return proper error" in withHttpService { fixture =>
      import fixture.encoder
      val contractIdString = "0" * 66
      val contractId = lar.ContractId(contractIdString)
      val exerciseJson: JsValue = encodeExercise(encoder)(iouExerciseTransferCommand(contractId))
      fixture
        .postJsonRequestWithMinimumAuth[JsValue](Uri.Path("/v1/exercise"), exerciseJson)
        .map(inside(_) {
          case domain
                .ErrorResponse(Seq(errorMsg), None, StatusCodes.NotFound, Some(ledgerApiError)) =>
            errorMsg should include(
              s"Contract could not be found with id $contractIdString"
            )
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
        }): Future[Assertion]
    }

    "Archive" in withHttpService { fixture =>
      import fixture.encoder
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val create: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice)
        postCreateCommand(create, fixture, headers)
          .flatMap(inside(_) { case domain.OkResponse(createResult, _, StatusCodes.OK) =>
            val reference = domain.EnrichedContractId(Some(TpId.Iou.Iou), createResult.contractId)
            val exercise = archiveCommand(reference)
            val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

            fixture
              .postJsonRequest(Uri.Path("/v1/exercise"), exerciseJson, headers)
              .parseResponse[domain.ExerciseResponse[JsValue]]
              .flatMap(inside(_) { case domain.OkResponse(exercisedResponse, _, StatusCodes.OK) =>
                assertExerciseResponseArchivedContract(exercisedResponse, exercise)
              })
          }): Future[Assertion]
      }
    }

    "Archive by key" in withHttpService { fixture =>
      import fixture.encoder
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
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

        postCreateCommand(create, fixture, headers).flatMap(inside(_) {
          case domain.OkResponse(_, _, StatusCodes.OK) =>
            fixture
              .postJsonRequest(Uri.Path("/v1/exercise"), archiveJson, headers)
              .parseResponse[JsValue]
              .map(inside(_) { case domain.OkResponse(_, _, StatusCodes.OK) =>
                succeed
              })
        }): Future[Assertion]
      }
    }
  }

  "create-and-exercise IOU_Transfer" in withHttpService { fixture =>
    import fixture.encoder
    fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
      val cmd: domain.CreateAndExerciseCommand[v.Record, v.Value, OptionalPkg] =
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

  private def assertExerciseResponseNewActiveContract(
      exerciseResponse: domain.ExerciseResponse[JsValue],
      createCmd: domain.CreateCommand[v.Record, OptionalPkg],
      exerciseCmd: domain.ExerciseCommand[v.Value, domain.EnrichedContractId],
      fixture: HttpServiceTestFixtureData,
      headers: List[HttpHeader],
  ): Future[Assertion] = {
    import fixture.{uri, decoder}
    inside(exerciseResponse) {
      case domain.ExerciseResponse(
            JsString(exerciseResult),
            List(contract1, contract2),
            completionOffset,
          ) =>
        completionOffset.unwrap should not be empty
        // checking contracts
        inside(contract1) { case domain.Contract(-\/(archivedContract)) =>
          Future {
            (archivedContract.contractId: domain.ContractId) shouldBe (exerciseCmd.reference.contractId: domain.ContractId)
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
            postContractsLookup(newContractLocator, uri, headers).map(inside(_) {
              case domain.OkResponse(Some(contract), _, StatusCodes.OK) =>
                contract.contractId shouldBe newContractLocator.contractId
            }): Future[Assertion]
          }
    }
  }

  "should support multi-party command submissions" in withHttpService { fixture =>
    import fixture.encoder
    for {
      // multi-party actAs on create
      cid <- fixture
        .headersWithPartyAuth(List("Alice", "Bob"))
        .flatMap(
          postCreateCommand(multiPartyCreateCommand(List("Alice", "Bob"), ""), fixture, _)
        )
        .map(resultContractId)
      // multi-party actAs on exercise
      cidMulti <- fixture
        .headersWithPartyAuth(List("Alice", "Bob", "Charlie", "David"))
        .flatMap(
          fixture.postJsonRequest(
            Uri.Path("/v1/exercise"),
            encodeExercise(encoder)(multiPartyAddSignatories(cid, List("Charlie", "David"))),
            _,
          )
        )
        .parseResponse[domain.ExerciseResponse[JsValue]]
        .map(inside(_) {
          case domain.OkResponse(domain.ExerciseResponse(JsString(c), _, _), _, StatusCodes.OK) =>
            lar.ContractId(c)
        })
      // create a contract only visible to Alice
      cid <- fixture
        .headersWithPartyAuth(List("Alice"))
        .flatMap(
          postCreateCommand(
            multiPartyCreateCommand(List("Alice"), ""),
            fixture,
            _,
          )
        )
        .map(resultContractId)
      _ <- fixture
        .headersWithPartyAuth(List("Charlie"), readAs = List("Alice"))
        .flatMap(
          fixture.postJsonRequest(
            Uri.Path("/v1/exercise"),
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
      exerciseResponse: domain.ExerciseResponse[JsValue],
      exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId],
  ): Assertion =
    inside(exerciseResponse) { case domain.ExerciseResponse(exerciseResult, List(contract1), _) =>
      exerciseResult shouldBe JsObject()
      inside(contract1) { case domain.Contract(-\/(archivedContract)) =>
        (archivedContract.contractId.unwrap: String) shouldBe (exercise.reference.contractId.unwrap: String)
      }
    }

  "should be able to serialize and deserialize domain commands" in withHttpService { fixture =>
    (testCreateCommandEncodingDecoding(fixture) *>
      testExerciseCommandEncodingDecoding(fixture)): Future[Assertion]
  }

  private def testCreateCommandEncodingDecoding(
      fixture: HttpServiceTestFixtureData
  ): Future[Assertion] = instanceUUIDLogCtx { implicit lc =>
    import fixture.{uri, encoder, decoder}
    import json.JsonProtocol._
    import util.ErrorOps._

    val command0: domain.CreateCommand[v.Record, OptionalPkg] =
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
                JsArray(requestedPartyIds.map(x => JsString(x.unwrap))),
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
          JsArray(Vector.empty),
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
          JsArray(requestedPartyIds.map(x => JsString(x.unwrap))),
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
            JsArray(requestedPartyIds.map(x => JsString(x.unwrap))),
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
          json = JsObject(),
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

  "fetch by contractId" - {
    "succeeds normally" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice)

        postCreateCommand(command, fixture, headers).flatMap(inside(_) {
          case domain.OkResponse(result, _, StatusCodes.OK) =>
            val contractId: ContractId = result.contractId
            val locator = domain.EnrichedContractId(None, contractId)
            lookupContractAndAssert(locator, contractId, command, fixture, headers)
        }): Future[Assertion]
      }
    }

    "succeeds normally with an interface ID" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCommand(alice, CIou.CIou)

        postCreateCommand(command, fixture, headers).flatMap(inside(_) {
          case domain.OkResponse(result, _, StatusCodes.OK) =>
            val contractId: ContractId = result.contractId
            val locator = domain.EnrichedContractId(Some(TpId.IIou.IIou), contractId)
            postContractsLookup(locator, fixture.uri, headers).map(inside(_) {
              case domain.OkResponse(Some(resultContract), _, StatusCodes.OK) =>
                contractId shouldBe resultContract.contractId
                assertJsPayload(resultContract)(JsObject("amount" -> JsString("42")))
            })
        }): Future[Assertion]
      }
    }

    "returns {status:200, result:null} when contract is not found" in withHttpService { fixture =>
      import fixture.uri
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val accountNumber = "abc123"
        val locator = domain.EnrichedContractKey(
          TpId.Account.Account,
          JsArray(JsString(alice.unwrap), JsString(accountNumber)),
        )
        postContractsLookup(locator, uri.withPath(Uri.Path("/v1/fetch")), headers).map(inside(_) {
          case domain.OkResponse(None, _, StatusCodes.OK) =>
            succeed
        }): Future[Assertion]
      }
    }

    // TEST_EVIDENCE: Authorization: fetch fails when readAs not authed, even if prior fetch succeeded
    "fails when readAs not authed, even if prior fetch succeeded" in withHttpService { fixture =>
      import fixture.uri
      for {
        res <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, aliceHeaders) = res
        command = iouCreateCommand(alice)
        createStatusOutput <- postCreateCommand(command, fixture, aliceHeaders)
        contractId = inside(createStatusOutput) {
          case domain.OkResponse(result, _, StatusCodes.OK) =>
            result.contractId
        }
        locator = domain.EnrichedContractId(None, contractId)
        // will cache if DB configured
        _ <- lookupContractAndAssert(locator, contractId, command, fixture, aliceHeaders)
        charlie = getUniqueParty("Charlie")
        badLookup <- postContractsLookup(
          locator,
          uri.withPath(Uri.Path("/v1/fetch")),
          aliceHeaders,
          readAs = Some(List(charlie)),
        )
      } yield inside(badLookup) {
        case domain.ErrorResponse(_, None, StatusCodes.Unauthorized, None) =>
          succeed
      }
    }
  }

  "fetch by key" - {
    "succeeds normally" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val accountNumber = "abc123"
        val command: domain.CreateCommand[v.Record, OptionalPkg] =
          accountCreateCommand(alice, accountNumber)

        postCreateCommand(command, fixture, headers).flatMap(inside(_) {
          case domain.OkResponse(result, _, StatusCodes.OK) =>
            val contractId: ContractId = result.contractId
            val locator = domain.EnrichedContractKey(
              TpId.Account.Account,
              JsArray(JsString(alice.unwrap), JsString(accountNumber)),
            )
            lookupContractAndAssert(locator, contractId, command, fixture, headers)
        }): Future[Assertion]
      }
    }

    "containing variant and record" - {
      "encoded as array with number num" in withHttpService { fixture =>
        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
          testFetchByCompositeKey(
            fixture,
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

      "encoded as record with string num" in withHttpService { fixture =>
        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
          testFetchByCompositeKey(
            fixture,
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
    }
  }

  private def testFetchByCompositeKey(
      fixture: UriFixture,
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
    fixture
      .postJsonRequest(Uri.Path("/v1/create"), createCommand, headers)
      .parseResponse[domain.ActiveContract[JsValue]]
      .flatMap(inside(_) { case domain.OkResponse(c, _, StatusCodes.OK) =>
        val contractId: ContractId = c.contractId

        fixture
          .postJsonRequest(Uri.Path("/v1/fetch"), request, headers)
          .parseResponse[domain.ActiveContract[JsValue]]
          .flatMap(inside(_) { case domain.OkResponse(c, _, StatusCodes.OK) =>
            c.contractId shouldBe contractId
          })
      }): Future[Assertion]
  }

  "packages endpoint should" - {
    "return all known package IDs" in withHttpService { fixture =>
      getAllPackageIds(fixture).map { x =>
        inside(x) {
          case domain.OkResponse(ps, None, StatusCodes.OK) if ps.nonEmpty =>
            Inspectors.forAll(ps)(_.length should be > 0)
        }
      }: Future[Assertion]
    }
  }

  "packages/packageId should" - {
    "return a requested package" in withHttpService { fixture =>
      import AbstractHttpServiceIntegrationTestFuns.sha256
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

  // TEST_EVIDENCE: Performance: archiving a large number of contracts should succeed
  "archiving a large number of contracts should succeed" in withHttpService(
    maxInboundMessageSize = StartSettings.DefaultMaxInboundMessageSize * 10
  ) { fixture =>
    import fixture.encoder
    import org.scalacheck.{Arbitrary, Gen}
    fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
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
          choiceInterfaceId = None,
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
                    .seq(VA.contractId(Arbitrary(Gen.fail)))
                    .inj(cids map lfv.Value.ContractId.assertFromString)
                ).sum
              )
            )
          ),
          choiceInterfaceId = None,
          meta = None,
        )

      def queryN(n: Long): Future[Assertion] = fixture
        .postJsonRequest(
          Uri.Path("/v1/query"),
          jsObject("""{"templateIds": ["Account:Account"]}"""),
          headers,
        )
        .parseResponse[Vector[JsValue]]
        .map(inside(_) { case domain.OkResponse(result, _, StatusCodes.OK) =>
          result should have length n
        })

      for {
        resp <- fixture
          .postJsonRequest(Uri.Path("/v1/create-and-exercise"), encode(createCmd), headers)
          .parseResponse[domain.ExerciseResponse[JsValue]]
        result = inside(resp) { case domain.OkResponse(result, _, StatusCodes.OK) =>
          result
        }
        created = result.exerciseResult.convertTo[List[String]]
        _ = created should have length numContracts

        _ <- queryN(numContracts)

        archiveResponse <- fixture
          .postJsonRequest(
            Uri.Path("/v1/create-and-exercise"),
            encode(archiveCmd(created)),
            headers,
          )
          .parseResponse[JsValue]
        _ = inside(archiveResponse) { case domain.OkResponse(_, _, StatusCodes.OK) =>
        }

        _ <- queryN(0)
      } yield succeed
    }
  }

  "Should ignore conflicts on contract key hash constraint violation" in withHttpService {
    fixture =>
      import fixture.encoder
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
          VAx.seq(VAx.partyDomain).inj(following)
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

        domain.ExerciseCommand(reference, choice, boxedRecord(arg), None, None)
      }

      def followUser(contractId: lar.ContractId, actAs: domain.Party, toFollow: domain.Party) = {
        val exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId] =
          userExerciseFollowCommand(contractId, toFollow)
        val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

        fixture
          .headersWithPartyAuth(actAs = List(actAs.unwrap))
          .flatMap(headers =>
            fixture.postJsonRequest(Uri.Path("/v1/exercise"), exerciseJson, headers)
          )
          .parseResponse[JsValue]
          .map(inside(_) { case domain.OkResponse(_, _, StatusCodes.OK) =>
          })

      }

      def queryUsers(fromPerspectiveOfParty: domain.Party) = {
        val query = jsObject(s"""{
             "templateIds": ["$packageId:User:User"],
             "query": {}
          }""")

        fixture
          .headersWithPartyAuth(actAs = List(fromPerspectiveOfParty.unwrap))
          .flatMap(headers => fixture.postJsonRequest(Uri.Path("/v1/query"), query, headers))
          .parseResponse[JsValue]
          .map(inside(_) { case domain.OkResponse(_, _, StatusCodes.OK) =>
          })
      }

      val commands = partyIds.map { p =>
        (p, userCreateCommand(p))
      }

      for {
        users <- commands.traverse { case (party, command) =>
          val fut = fixture
            .headersWithPartyAuth(actAs = List(party.unwrap))
            .flatMap(headers =>
              postCreateCommand(
                command,
                fixture,
                headers,
              )
            )
            .map(resultContractId): Future[ContractId]
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
