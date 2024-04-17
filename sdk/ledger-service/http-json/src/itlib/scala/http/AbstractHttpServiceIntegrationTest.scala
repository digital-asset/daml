// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.time.{Instant, LocalDate}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.stream.scaladsl.Sink
import com.daml.api.util.TimestampConversion
import com.daml.fetchcontracts.domain.ContractTypeId
import com.daml.lf.data.Ref
import com.daml.http.domain.{Base64, ContractId}
import com.daml.http.endpoints.MeteringReportEndpoint.MeteringReportDateRequest
import com.daml.http.json.SprayJson.objectField
import com.daml.http.json._
import com.daml.http.util.ClientUtil.{boxedRecord, uniqueCommandId, uniqueId}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  InterfaceFilter,
  TemplateFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v1.{value => v}
import com.daml.ledger.service.MetadataReader
import com.daml.test.evidence.tag.Security.SecurityTest.Property.{Authorization, Availability}
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues._
import scalaz.std.list._
import scalaz.std.vector._
import scalaz.std.scalaFuture._
import scalaz.std.tuple._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.{-\/, \/-}
import shapeless.record.{Record => ShRecord}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success
import com.daml.lf.{value => lfv}
import com.daml.scalautil.Statement.discard
import com.google.protobuf.ByteString
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

  override protected lazy val maxPartiesPageSize = Some(100)

  protected def jwt(uri: Uri)(implicit ec: ExecutionContext): Future[Jwt] =
    jwtForParties(uri)(domain.Party subst List("Alice"), List(), config.ledgerIds.headOption.value)

  protected def headersWithPartyAuthLegacyFormat(
      actAs: List[domain.Party],
      readAs: List[domain.Party] = List(),
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
            result.toSet should contain allElementsOf
              allocatedParties.toSet.map(domain.PartyDetails.fromLedgerApi)
          })
      }: Future[Assertion]
  }

  "create should fail with custom tokens that contain no ledger id" in withHttpService { fixture =>
    import fixture.encoder
    val alice = getUniqueParty("Alice")
    val command = iouCreateCommand(alice)
    val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

    val headers = HttpServiceTestFixture.authorizationHeader(
      HttpServiceTestFixture.jwtForParties(
        domain.Party subst List("Alice"),
        domain.Party subst List("Bob"),
        None,
        false,
        false,
      )
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

}

trait WithQueryStoreSetTest extends QueryStoreAndAuthDependentIntegrationTest {
  import HttpServiceTestFixture.archiveCommand
  import json.JsonProtocol._

  "refresh cache endpoint" - {
    "should return latest offset when the cache is outdated" in withHttpService { fixture =>
      import fixture.encoder
      def archiveIou(headers: List[HttpHeader], contractId: domain.ContractId) = {
        val reference = domain.EnrichedContractId(Some(TpId.Iou.Iou), contractId)
        val exercise = archiveCommand(reference)
        val exerciseJson: JsValue = encodeExercise(encoder)(exercise)
        fixture
          .postJsonRequest(Uri.Path("/v1/exercise"), exerciseJson, headers)
          .parseResponse[domain.ExerciseResponse[JsValue]]
          .flatMap(inside(_) { case domain.OkResponse(exercisedResponse, _, StatusCodes.OK) =>
            assertExerciseResponseArchivedContract(exercisedResponse, exercise)
          })
      }

      for {
        (alice, aliceHeaders) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        searchDataSet = genSearchDataSet(alice)
        contractIds <- searchExpectOk(
          searchDataSet,
          jsObject(
            """{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR", "amount": "111.11"}}"""
          ),
          fixture,
          aliceHeaders,
        ).map { acl: List[domain.ActiveContract.ResolvedCtTyId[JsValue]] =>
          acl.size shouldBe 1
          acl.map(a => objectField(a.payload, "currency")) shouldBe List(Some(JsString("EUR")))
          acl.map(a => objectField(a.payload, "amount")) shouldBe List(Some(JsString("111.11")))
          acl.map(_.contractId)
        }

        _ <- contractIds.traverse(archiveIou(aliceHeaders, _))

        res <-
          fixture
            .postJsonRequest(Uri.Path("/v1/refresh/cache"), jsObject("{}"), aliceHeaders)
            .parseResponse[JsValue]

      } yield {
        inside(res) { case domain.OkResponse(s, _, StatusCodes.OK) =>
          assert(s.toString.matches("""\[\{\"refreshedAt\":\"[0-9a-f]*\"\}\]"""))
        }
      }
    }

    "should return latest offset when the cache was up to date" in withHttpService { fixture =>
      for {
        res1 <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, aliceHeaders) = res1
        searchDataSet = genSearchDataSet(alice)
        _ <- searchExpectOk(
          searchDataSet,
          jsObject(
            """{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR", "amount": "111.11"}}"""
          ),
          fixture,
          aliceHeaders,
        ).map { acl: List[domain.ActiveContract.ResolvedCtTyId[JsValue]] =>
          acl.size shouldBe 1
          acl.map(a => objectField(a.payload, "currency")) shouldBe List(Some(JsString("EUR")))
          acl.map(a => objectField(a.payload, "amount")) shouldBe List(Some(JsString("111.11")))
          acl.map(_.contractId)
        }
        res <-
          fixture
            .postJsonRequest(Uri.Path("/v1/refresh/cache"), jsObject("{}"), aliceHeaders)
            .parseResponse[JsValue]

      } yield {
        inside(res) { case domain.OkResponse(s, _, StatusCodes.OK) =>
          assert(s.toString.matches("""\[\{\"refreshedAt\":\"[0-9a-f]*\"\}\]"""))
        }
      }
    }
  }
}

/** Tests that may behave differently depending on
  *
  * 1. whether custom or user tokens are used, ''and''
  * 2. the query store configuration
  */
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class QueryStoreAndAuthDependentIntegrationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with AbstractHttpServiceIntegrationTestFuns {

  import AbstractHttpServiceIntegrationTestFuns.{
    VAx,
    UriFixture,
    EncoderFixture,
    HttpServiceTestFixtureData,
  }
  import HttpServiceTestFixture.{UseTls, accountCreateCommand, archiveCommand}
  import json.JsonProtocol._
  import AbstractHttpServiceIntegrationTestFuns.{ciouDar, riouDar}

  val authorizationSecurity: SecurityTest =
    SecurityTest(property = Authorization, asset = "HTTP JSON API Service")

  val availabilitySecurity: SecurityTest =
    SecurityTest(property = Availability, asset = "HTTP JSON API Service")

  object CIou {
    val CIou: domain.ContractTypeId.Template.OptionalPkg =
      domain.ContractTypeId.Template(None, "CIou", "CIou")
  }

  override def useTls = UseTls.NoTls

  protected def genSearchDataSet(
      party: domain.Party,
      usePackageName: Boolean = false,
  ): List[domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg]] =
    List(
      iouCreateCommand(
        amount = "111.11",
        currency = "EUR",
        party = party,
        usePackageName = usePackageName,
      ),
      iouCreateCommand(
        amount = "222.22",
        currency = "EUR",
        party = party,
        usePackageName = usePackageName,
      ),
      iouCreateCommand(
        amount = "333.33",
        currency = "GBP",
        party = party,
        usePackageName = usePackageName,
      ),
      iouCreateCommand(
        amount = "444.44",
        currency = "BTC",
        party = party,
        usePackageName = usePackageName,
      ),
    )

  // Whether the underlying JSON query engine place unavoidable limits on the JSON queries that are supportable.
  protected def constrainedJsonQueries = false

  private implicit final class OraclePayloadIndexSupport(private val label: String) {

    /** While Oracle's JSON index makes queries a lot faster, it does place some limitations on the JSON queries it can service.
      * Tests for such queries can be guarded with unlessConstrainedJsonQueries_-
      * so for Oracle, they will only be run when DisableContractPayloadIndexing=true
      * and will be run for all other configurations.
      */
    def unlessConstrainedJsonQueries_-(fun: => Unit): Unit =
      if (constrainedJsonQueries) () else label - fun
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
        ).map { acl: List[domain.ActiveContract.ResolvedCtTyId[JsValue]] =>
          acl.size shouldBe searchDataSet.size
        }
      }
    }

    "single party with package name" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val searchDataSet = genSearchDataSet(alice, usePackageName = true)
        searchExpectOk(
          searchDataSet,
          jsObject(s"""{"templateIds": ["${TpId.Iou.PkgName}:Iou:Iou"]}"""),
          fixture,
          headers,
        ).map { acl: List[domain.ActiveContract.ResolvedCtTyId[JsValue]] =>
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
          .headersWithPartyAuth(List(alice, bob))
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

    "multi-view" - {
      val amountsCurrencies = Vector(("42.0", "USD"), ("84.0", "CHF"))
      val expectedAmountsCurrencies = amountsCurrencies.map { case (a, c) => (a.toDouble, c) }

      def testMultiView[ExParties](
          fixture: HttpServiceTestFixtureData,
          allocateParties: Future[ExParties],
      )(
          observers: ExParties => Vector[domain.Party],
          queryHeaders: (domain.Party, List[HttpHeader], ExParties) => Future[List[HttpHeader]],
      ) = for {
        _ <- uploadPackage(fixture)(riouDar)
        (alice, aliceHeaders) <- fixture.getUniquePartyAndAuthHeaders("alice")
        exParties <- allocateParties

        // create all contracts
        exObservers = observers(exParties)
        cids <- amountsCurrencies.traverse { case (amount, currency) =>
          postCreateCommand(
            iouCreateCommand(
              alice,
              amount = amount,
              currency = currency,
              observers = exObservers,
            ),
            fixture,
            aliceHeaders,
          ) map resultContractId
        }
        queryAsBoth <- queryHeaders(alice, aliceHeaders, exParties)
        queryAtCtId = {
          (ctid: domain.ContractTypeId.OptionalPkg, amountKey: String, currencyKey: String) =>
            searchExpectOk(
              List.empty,
              Map("templateIds" -> List(ctid)).toJson.asJsObject,
              fixture,
              queryAsBoth,
            ) map { resACs =>
              inside(resACs map (inside(_) {
                case domain.ActiveContract(cid, _, _, payload, Seq(`alice`), `exObservers`, _) =>
                  // ensure the contract metadata is right, then discard
                  (cid, payload.asJsObject.fields)
              })) { case Seq((cid0, payload0), (cid1, payload1)) =>
                Seq(cid0, cid1) should contain theSameElementsAs cids
                // check the actual payloads match the contract IDs from creates
                val actualAmountsCurrencies = (if (cid0 == cids.head) Seq(payload0, payload1)
                                               else Seq(payload1, payload0))
                  .map(payload =>
                    inside((payload get amountKey, payload get currencyKey)) {
                      case (Some(JsString(amount)), Some(JsString(currency))) =>
                        (amount.toDouble, currency)
                    }
                  )
                actualAmountsCurrencies should ===(expectedAmountsCurrencies)
              }
            }
        }
        // run (inserting when query store) on template ID; then interface ID
        // (thereby duplicating contract IDs)
        _ <- queryAtCtId(TpId.Iou.Iou, "amount", "currency")
        _ <- queryAtCtId(TpId.RIou.RIou, "iamount", "icurrency")
        // then try template ID again, in case interface ID mangled the results
        // for template ID by way of stakeholder join or something even odder
        _ <- queryAtCtId(TpId.Iou.Iou, "amount", "currency")
      } yield succeed

      // multi-party and single-party are handled significantly differently
      // in Oracle, so we check behavior with both varieties

      "multi-party" in withHttpService { fixture =>
        testMultiView(
          fixture,
          fixture.getUniquePartyAndAuthHeaders("bob").map(_._1),
        )(
          bob => Vector(bob),
          (alice, _, bob) => fixture.headersWithPartyAuth(List(alice), List(bob)),
        )
      }

      "single party" in withHttpService { fixture =>
        testMultiView(
          fixture,
          Future successful (()),
        )(
          _ => Vector.empty,
          (_, aliceHeaders, _) => Future successful aliceHeaders,
        )
      }

    }
  }

  "query with unknown Template IDs" - {
    "warns if some are known" in withHttpService { fixture =>
      val query =
        jsObject(
          """{"templateIds": ["Iou:Iou", "UnknownModule:UnknownEntity"], "query": {"currency": "EUR"}}"""
        )
      fixture
        .getUniquePartyAndAuthHeaders("UnknownParty")
        .flatMap { case (_, headers) =>
          search(List(), query, fixture, headers).map { response =>
            inside(response) { case domain.OkResponse(acl, warnings, StatusCodes.OK) =>
              acl.size shouldBe 0
              warnings shouldBe Some(
                domain.UnknownTemplateIds(
                  List(domain.ContractTypeId(None, "UnknownModule", "UnknownEntity"))
                )
              )
            }
          }
        }
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
                  domain.ContractTypeId(None, "AAA", "BBB"),
                  domain.ContractTypeId(None, "XXX", "YYY"),
                )
              }
          }
        }
      }
    }
  }

  "query record contains handles small tokens with " - {
    Seq(
      "& " -> "& bar",
      "255 bytes" -> randomTextN(255),
      "256 bytes" -> randomTextN(256),
    ).foreach { case (testLbl, testCurrency) => testQueryingWithToken(testLbl, testCurrency) }
  }

  "query record contains larger tokens with " unlessConstrainedJsonQueries_- {
    Seq(257, 1000, 2000, 3000, 4000, 5000).foreach { case len =>
      testQueryingWithToken(s"$len bytes", randomTextN(len))
    }
  }

  def testQueryingWithToken(testLabel: String, testCurrency: String) =
    testLabel in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        searchExpectOk(
          genSearchDataSet(alice) :+ iouCreateCommand(
            currency = testCurrency,
            party = alice,
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

  "query multiple observers:" - {
    Seq(
      0 -> 1,
      1 -> 5,
      10 -> 75,
      50 -> 76, // Allows space to encode content into a JSON array of strings within 4k limit.
      50 -> 80, // The content is the exact 4k limit, no additional room for JSON array syntax.
      1000 -> 185,
    ).foreach { case (numSubs, partySize) =>
      (s"$numSubs observers of $partySize chars") in withHttpService { fixture =>
        val subscribers = (1 to numSubs).map(_ => domain.Party(randomTextN(partySize))).toList
        for {
          (publisher, headers) <- fixture.getUniquePartyAndAuthHeaders("Alice")
          subscriberPartyDetails <- subscribers.traverse { p =>
            fixture.client.partyManagementClient.allocateParty(Some(p.unwrap), Some(s"${p} & Co"))
          }
          subscriberParties = domain.Party subst subscriberPartyDetails.map(p => p.party: String)
          found <- searchExpectOk(
            List(pubSubCreateCommand(publisher, subscriberParties)),
            jsObject(
              s"""{"templateIds": ["Account:PubSub"], "query": {"publisher": "$publisher"}}"""
            ),
            fixture,
            headers,
          )
        } yield {
          found.size shouldBe 1
        }
      }
    }
  }

  "query long field names" - {
    "int field with name of 251 chars" in withHttpService { fixture =>
      createAndFindWithLongFieldName(
        longFieldNamesCreateCommand(_, intFieldWith251Chars = 1),
        "intFieldWith251Chars_______________________________________________________________________________________________________________________________________________________________________________________________________________________________________",
        "1",
      )(fixture)
    }
    "text field with name of 512 chars" in withHttpService { fixture =>
      createAndFindWithLongFieldName(
        longFieldNamesCreateCommand(_, textFieldWith512Chars = "hello"),
        "textFieldWith512Chars___________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________",
        "\"hello\"",
      )(fixture)
    }
  }

  "query longer field names" unlessConstrainedJsonQueries_- {
    "int field with name of 252 chars" in withHttpService { fixture =>
      createAndFindWithLongFieldName(
        longFieldNamesCreateCommand(_, intFieldWith252Chars = 1),
        "intFieldWith252Chars________________________________________________________________________________________________________________________________________________________________________________________________________________________________________",
        "1",
      )(fixture)
    }
    "int field with name of 512 chars" in withHttpService { fixture =>
      createAndFindWithLongFieldName(
        longFieldNamesCreateCommand(_, intFieldWith512Chars = 1),
        "intFieldWith512Chars____________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________",
        "1",
      )(fixture)
    }
  }

  def createAndFindWithLongFieldName(
      createCommand: (
          domain.Party => domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg]
      ),
      fieldName: String,
      jsonValue: String,
  )(fixture: UriFixture with EncoderFixture) = {
    for {
      (alice, headers) <- fixture.getUniquePartyAndAuthHeaders("Alice")
      found <- searchExpectOk(
        List(createCommand(alice)),
        jsObject(
          s"""{
              "templateIds": ["Account:LongFieldNames"],
              "query": {
                "party": "$alice",
                "$fieldName" : $jsonValue
              }
            }"""
        ),
        fixture,
        headers,
      )
    } yield {
      found.size shouldBe 1
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
        ).map { acl: List[domain.ActiveContract.ResolvedCtTyId[JsValue]] =>
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
                  .parseResponse[List[domain.ActiveContract.ResolvedCtTyId[JsValue]]]
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
        ).map { acl: List[domain.ActiveContract.ResolvedCtTyId[JsValue]] =>
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
        ).map { acl: List[domain.ActiveContract.ResolvedCtTyId[JsValue]] =>
          acl.size shouldBe 0
        }
      }
    }

    "by a variant field" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val accountNumber = "abc123"
        val now = TimestampConversion.roundInstantToMicros(Instant.now)
        val nowStr = TimestampConversion.microsToInstant(now).toString
        val command = accountCreateCommand(alice, accountNumber, now)

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
              .parseResponse[List[domain.ActiveContract.ResolvedCtTyId[JsValue]]]
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

      val kbvarId = TpId.Account.KeyedByVariantAndRecord
      import FilterDiscriminatorScenario.Scenario
      Seq(
        Scenario(
          "gt string",
          kbvarId,
          kbvarVA,
          Map("bazRecord" -> Map("baz" -> Map("%gt" -> "b")).toJson),
        )(
          matches = Seq(withBazRecord("c")),
          doesNotMatch = Seq(withBazRecord("a")),
        ),
        Scenario(
          "gt string with sketchy value",
          kbvarId,
          kbvarVA,
          Map("bazRecord" -> Map("baz" -> Map("%gt" -> "bobby'); DROP TABLE Students;--")).toJson),
        )(
          matches = Seq(withBazRecord("c")),
          doesNotMatch = Seq(withBazRecord("a")),
        ),
        Scenario(
          "lt string with sketchy value which is a single quote",
          kbvarId,
          kbvarVA,
          Map("bazRecord" -> Map("baz" -> Map("%lt" -> "'")).toJson),
        )(
          matches = Seq(withBazRecord(" ")), // Less than '
          doesNotMatch = Seq(withBazRecord("A")), // Not less than '
        ),
        Scenario(
          "lt string with sketchy value which uses unicode quote char",
          kbvarId,
          kbvarVA,
          Map("bazRecord" -> Map("baz" -> Map("%lt" -> "O\u02bcReilly")).toJson),
        )(
          matches = Seq(withBazRecord("A")),
          doesNotMatch = Seq(withBazRecord("Z")),
        ),
        Scenario(
          "eq empty string matches just that",
          kbvarId,
          kbvarVA,
          Map("bazRecord" -> Map("baz" -> "").toJson),
        )(
          matches = Seq(withBazRecord("")),
          doesNotMatch = Seq(withBazRecord("a")),
        ),
        Scenario(
          "lt empty string matches nothing",
          kbvarId,
          kbvarVA,
          Map("bazRecord" -> Map("baz" -> Map("%lt" -> "")).toJson),
        )(
          matches = Seq.empty,
          doesNotMatch = Seq(withBazRecord("a"), withBazRecord("")),
        ),
        Scenario(
          "lte empty string only matches empty string",
          kbvarId,
          kbvarVA,
          Map("bazRecord" -> Map("baz" -> Map("%lte" -> "")).toJson),
        )(
          matches = Seq(withBazRecord("")),
          doesNotMatch = Seq(withBazRecord("a")),
        ),
        Scenario(
          "gt empty string only matches non-empty string",
          kbvarId,
          kbvarVA,
          Map("bazRecord" -> Map("baz" -> Map("%gt" -> "")).toJson),
        )(
          matches = Seq(withBazRecord("a")),
          doesNotMatch = Seq(withBazRecord("")),
        ),
        Scenario(
          "gte empty string matches everything",
          kbvarId,
          kbvarVA,
          Map("bazRecord" -> Map("baz" -> Map("%gte" -> "")).toJson),
        )(
          matches = Seq(withBazRecord("a"), withBazRecord("")),
          doesNotMatch = Seq.empty,
        ),
        Scenario(
          "gt int",
          kbvarId,
          kbvarVA,
          Map("fooVariant" -> Map("tag" -> "Bar".toJson, "value" -> Map("%gt" -> 2).toJson).toJson),
        )(matches = Seq(withFooVariant(10)), doesNotMatch = Seq(withFooVariant(1))),
      ).zipWithIndex.foreach { case (scenario, ix) =>
        import scenario._
        s"$label (scenario $ix)" in withHttpService { fixture =>
          for {
            (alice, headers) <- fixture.getUniquePartyAndAuthHeaders("Alice")
            contracts <- searchExpectOk(
              (matches ++ doesNotMatch).toList.map { payload =>
                domain.CreateCommand(ctId, argToApi(va)(payload(alice)), None)
              },
              JsObject(Map("templateIds" -> Seq(ctId).toJson, "query" -> query.toJson)),
              fixture,
              headers,
            )
          } yield contracts.map(_.payload) should contain theSameElementsAs matches.map { m =>
            LfValueCodec.apiValueToJsValue(va.inj(m(alice)))
          }
        }
      }
    }
  }

  protected implicit final class `AHS TI uri funs`(private val fixture: UriFixture) {

    def searchAllExpectOk(
        headers: List[HttpHeader]
    ): Future[List[domain.ActiveContract.ResolvedCtTyId[JsValue]]] =
      searchAll(headers).map(expectOk(_))

    def searchAllExpectOk(
    ): Future[List[domain.ActiveContract.ResolvedCtTyId[JsValue]]] =
      fixture.headersWithAuth.flatMap(searchAllExpectOk(_))

    def searchAll(
        headers: List[HttpHeader]
    ): Future[domain.SyncResponse[List[domain.ActiveContract.ResolvedCtTyId[JsValue]]]] =
      fixture
        .getRequest(Uri.Path("/v1/query"), headers)
        .parseResponse[List[domain.ActiveContract.ResolvedCtTyId[JsValue]]]

  }

  "exercise" - {
    "succeeds normally" in withHttpService { fixture =>
      import fixture.encoder
      for {
        (alice, headers) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (bob, _) <- fixture.getUniquePartyAndAuthHeaders("Bob")
        create = iouCreateCommand(alice)
        res <- postCreateCommand(create, fixture, headers)
        _ <- inside(res) { case domain.OkResponse(createResult, _, StatusCodes.OK) =>
          val exercise = iouExerciseTransferCommand(createResult.contractId, bob)
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
                fixture.ledgerId,
              )
            })
        }
      } yield succeed
    }

    "with unknown contractId should return proper error" in withHttpService { fixture =>
      import fixture.encoder
      // 66 for the contract id + 2 for an arbitrary suffix (as per `requireV1ContractIdSuffix`)
      val contractIdString = "0" * 68
      val contractId = lar.ContractId(contractIdString)
      for {
        (bob, headers) <- fixture.getUniquePartyAndAuthHeaders("Bob")
        exerciseJson: JsValue =
          encodeExercise(encoder)(iouExerciseTransferCommand(contractId, bob))
        _ <- fixture
          .postJsonRequest(Uri.Path("/v1/exercise"), exerciseJson, headers)
          .parseResponse[domain.ExerciseResponse[JsValue]]
          .map(inside(_) {
            case domain
                  .ErrorResponse(Seq(errorMsg), None, StatusCodes.NotFound, Some(ledgerApiError)) =>
              errorMsg should include(
                s"Contract could not be found with id $contractIdString"
              )
              ledgerApiError.message should include("CONTRACT_NOT_FOUND")
              ledgerApiError.message should include(
                "Contract could not be found with id 00000000000000000000000000000000000000000000000000000000000000000000"
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
                  name shouldBe "00000000000000000000000000000000000000000000000000000000000000000000"
                  typ shouldBe "CONTRACT_ID"
                case _ => fail()
              }
          })
      } yield succeed
    }

    "Archive" in withHttpService { fixture =>
      import fixture.encoder
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val create = iouCreateCommand(alice)
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
        val create: domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg] =
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
        val archive = archiveCommand(locator)
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

    "passes along disclosed contracts in" - {
      import util.IdentifierConverters.{lfIdentifier, refApiIdentifier}
      import com.daml.ledger.api.{v1 => lav1}
      import lav1.command_service.SubmitAndWaitRequest
      import lav1.commands.{Commands, Command}
      import domain.{DisclosedContract => DC}

      // we assume Disclosed is in the main dalf
      lazy val inferredPkgId = {
        import com.daml.lf.{archive, typesig}
        val dar =
          archive.UniversalArchiveReader.assertReadFile(AbstractHttpServiceIntegrationTestFuns.dar2)
        typesig.PackageSignature.read(dar.main)._2.packageId
      }

      def inDar2Main[CtId[P] <: domain.ContractTypeId.Ops[CtId, P]](
          tid: CtId[Option[String]]
      ): CtId[String] =
        tid.copy(packageId = inferredPkgId)

      lazy val ToDisclose = inDar2Main(TpId.Disclosure.ToDisclose)
      lazy val AnotherToDisclose = inDar2Main(TpId.Disclosure.AnotherToDisclose)
      lazy val HasGarbage = inDar2Main(TpId.Disclosure.HasGarbage)

      lazy val (_, toDiscloseVA) =
        VA.record(lfIdentifier(ToDisclose), ShRecord(owner = VAx.partyDomain, junk = VA.text))

      lazy val (_, anotherToDiscloseVA) =
        VA.record(lfIdentifier(ToDisclose), ShRecord(owner = VAx.partyDomain, garbage = VA.text))

      val (_, viewportVA) =
        VA.record(
          lfIdentifier(TpId.Disclosure.Viewport.copy(packageId = "ignored")),
          ShRecord(owner = VAx.partyDomain),
        )

      val (_, checkVisibilityVA) =
        VA.record(
          Ref.Identifier assertFromString "ignored:Disclosure:CheckVisibility",
          ShRecord(disclosed = VAx.contractIdDomain, ifaceDisclosed = VAx.contractIdDomain),
        )

      final case class ContractsToDisclose(
          alice: domain.Party,
          toDiscloseCid: domain.ContractId,
          toDiscloseBlob: ByteString,
          anotherToDiscloseCid: domain.ContractId,
          anotherToDiscloseBlob: ByteString,
      )

      def filterWithBlobsFor(
          interfaceId: ContractTypeId.Interface[String],
          templateId: ContractTypeId.Template[String],
          party: domain.Party,
      ) = {
        TransactionFilter(
          Map(
            party.unwrap -> Filters(
              Some(
                InclusiveFilters(
                  interfaceFilters = Seq(
                    InterfaceFilter(
                      interfaceId = Some(refApiIdentifier(interfaceId).unwrap),
                      includeCreatedEventBlob = true,
                    )
                  ),
                  templateFilters = Seq(
                    TemplateFilter(
                      templateId = Some(refApiIdentifier(templateId).unwrap),
                      includeCreatedEventBlob = true,
                    )
                  ),
                )
              )
            )
          )
        )
      }
      def contractsToDisclose(
          fixture: HttpServiceTestFixtureData,
          junkMessage: String,
          garbageMessage: String,
      ): Future[ContractsToDisclose] = for {
        (alice, jwt, applicationId, _) <- fixture.getUniquePartyTokenAppIdAndAuthHeaders("Alice")
        // we're using the ledger API for the initial create because timestamp
        // is required in the metadata
        toDisclosePayload = argToApi(toDiscloseVA)(ShRecord(owner = alice, junk = junkMessage))
        anotherToDisclosePayload = argToApi(anotherToDiscloseVA)(
          ShRecord(owner = alice, garbage = garbageMessage)
        )
        createCommands = Seq(
          (ToDisclose, toDisclosePayload),
          (AnotherToDisclose, anotherToDisclosePayload),
        ) map { case (tpid, payload) =>
          Command(util.Commands.create(refApiIdentifier(tpid), payload))
        }
        initialCreate = SubmitAndWaitRequest(
          Some(
            Commands(
              commandId = uniqueCommandId().unwrap,
              applicationId = applicationId.unwrap,
              actAs = domain.Party unsubst Seq(alice),
              commands = createCommands,
            )
          )
        )
        createResp <- fixture.client.commandServiceClient
          .submitAndWaitForTransaction(initialCreate, Some(jwt.value))
        // fetch what we can from the command service transaction
        (ceAtOffset, (toDiscloseCid, atdCid)) = inside(createResp.transaction) { case Some(tx) =>
          import lav1.event.Event, Event.Event.Created
          inside(tx.events) { case Seq(Event(Created(ce0)), Event(Created(ce1))) =>
            val EntityTD = ToDisclose.entityName
            val EntityATD = AnotherToDisclose.entityName
            val orderedCes = inside((ce0, ce1) umap (_.templateId.map(_.entityName))) {
              case (Some(EntityTD), Some(EntityATD)) => (ce0, ce1)
              case (Some(EntityATD), Some(EntityTD)) => (ce1, ce0)
            }
            (
              tx.offset,
              orderedCes umap { ce => domain.ContractId(ce.contractId) },
            )
          }
        }
        // use the transaction service to get the blob, which submit-and-wait
        // doesn't include in the response
        blobsToDisclose <- {
          import com.daml.fetchcontracts.util.{LedgerBegin, AbsoluteBookmark}
          import lav1.event.Event, Event.Event.Created
          fixture.client.transactionClient
            .getTransactions(
              LedgerBegin.toLedgerApi,
              Some(AbsoluteBookmark(domain.Offset(ceAtOffset)).toLedgerApi),
              filterWithBlobsFor(HasGarbage, ToDisclose, alice),
              com.daml.ledger.api.domain.LedgerId(""),
              token = Some(jwt.value),
            )
            .mapConcat(_.events)
            .collect {
              case Event(Created(ce))
                  if ce.contractId == toDiscloseCid || ce.contractId == atdCid =>
                domain.Base64(ce.createdEventBlob)
            }
            .runWith(Sink.seq)
        }
        (firstBlob, anotherBlob) = inside(blobsToDisclose) { case Seq(first, second) =>
          first -> second
        }
      } yield ContractsToDisclose(
        alice,
        toDiscloseCid,
        firstBlob.unwrap,
        atdCid,
        anotherBlob.unwrap,
      )

      def runDisclosureTestCase[Setup](
          fixture: HttpServiceTestFixtureData
      )(exerciseEndpoint: Uri.Path, setupBob: (domain.Party, List[HttpHeader]) => Future[Setup])(
          exerciseVaryingOnlyMeta: (
              Setup,
              ContractsToDisclose,
              Option[domain.CommandMeta[domain.ContractTypeId.Template.OptionalPkg]],
          ) => JsValue
      ): Future[Assertion] = {
        val junkMessage = s"some test junk ${uniqueId()}"
        val garbageMessage = s"some test garbage ${uniqueId()}"
        for {
          // first, set up something for alice to disclose to bob
          toDisclose @ ContractsToDisclose(
            alice,
            toDiscloseCid,
            toDiscloseBlob,
            atdCid,
            anotherToDiscloseBlob,
          ) <-
            contractsToDisclose(fixture, junkMessage, garbageMessage)

          // next, onboard bob to try to interact with the disclosed contract
          (bob, bobHeaders) <- fixture.getUniquePartyAndAuthHeaders("Bob")
          setup <- setupBob(bob, bobHeaders)

          // exercise CheckVisibility with different disclosure options
          checkVisibility = { disclosure: List[DC[domain.ContractTypeId.Template.OptionalPkg]] =>
            val meta = disclosure.nonEmpty option domain.CommandMeta(
              None,
              None,
              None,
              None,
              None,
              disclosedContracts = Some(disclosure),
            )
            fixture
              .postJsonRequest(
                exerciseEndpoint,
                exerciseVaryingOnlyMeta(setup, toDisclose, meta),
                bobHeaders,
              )
              .parseResponse[domain.ExerciseResponse[JsValue]]
          }

          // ensure that bob can't interact with alice's contract unless it's disclosed
          _ <- checkVisibility(List.empty)
            .map(inside(_) {
              case domain.ErrorResponse(
                    _,
                    _,
                    StatusCodes.NotFound,
                    Some(domain.LedgerApiError(lapiCode, errorMessage, _)),
                  ) =>
                lapiCode should ===(com.google.rpc.Code.NOT_FOUND_VALUE)
                errorMessage should include(toDiscloseCid.unwrap)
            })

          _ <- checkVisibility(
            List(
              DC(
                toDiscloseCid,
                TpId.Disclosure.ToDisclose,
                Base64(toDiscloseBlob),
              ),
              DC(
                atdCid,
                TpId.Disclosure.AnotherToDisclose,
                Base64(anotherToDiscloseBlob),
              ),
            )
          )
            .map(inside(_) {
              case domain.OkResponse(domain.ExerciseResponse(JsString(exResp), _, _), _, _) =>
                exResp should ===(s"'$bob' can see from '$alice': $junkMessage, $garbageMessage")
            })
        } yield succeed
      }

      val checkVisibilityChoice = domain.Choice("CheckVisibility")

      "exercise" in withHttpService { fixture =>
        runDisclosureTestCase(fixture)(
          Uri.Path("/v1/exercise"),
          { (bob, bobHeaders) =>
            postCreateCommand(
              domain
                .CreateCommand(
                  TpId.Disclosure.Viewport,
                  argToApi(viewportVA)(ShRecord(owner = bob)),
                  None,
                ),
              fixture,
              bobHeaders,
            ) map resultContractId
          },
        ) { (viewportCid, toDisclose, meta) =>
          encodeExercise(fixture.encoder)(
            domain.ExerciseCommand(
              domain.EnrichedContractId(Some(TpId.Disclosure.Viewport), viewportCid),
              checkVisibilityChoice,
              boxedRecord(
                argToApi(checkVisibilityVA)(
                  ShRecord(
                    disclosed = toDisclose.toDiscloseCid,
                    ifaceDisclosed = toDisclose.anotherToDiscloseCid,
                  )
                )
              ),
              None,
              meta,
            )
          )
        }
      }

      "create-and-exercise" in withHttpService { fixture =>
        runDisclosureTestCase(fixture)(
          Uri.Path("/v1/create-and-exercise"),
          { (bob, _) =>
            Future successful bob
          },
        ) { (bob, toDisclose, meta) =>
          fixture.encoder
            .encodeCreateAndExerciseCommand(
              domain.CreateAndExerciseCommand(
                TpId.Disclosure.Viewport,
                argToApi(viewportVA)(ShRecord(owner = bob)),
                checkVisibilityChoice,
                boxedRecord(
                  argToApi(checkVisibilityVA)(
                    ShRecord(
                      disclosed = toDisclose.toDiscloseCid,
                      ifaceDisclosed = toDisclose.anotherToDiscloseCid,
                    )
                  )
                ),
                None,
                meta,
              )
            )
            .valueOr(e => fail(e.shows))
        }
      }
    }
  }

  private def assertExerciseResponseNewActiveContract(
      exerciseResponse: domain.ExerciseResponse[JsValue],
      createCmd: domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg],
      exerciseCmd: domain.ExerciseCommand[Any, v.Value, domain.EnrichedContractId],
      fixture: HttpServiceTestFixtureData,
      headers: List[HttpHeader],
      ledgerId: LedgerId,
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
    import fixture.{client, encoder}
    val knownPartyNames = List("Alice", "Bob", "Charlie", "David").map(getUniqueParty)
    val partyManagement = client.partyManagementClient
    for {
      knownParties @ List(alice, bob, charlie, david) <-
        knownPartyNames.traverse { p =>
          domain.Party subst partyManagement
            .allocateParty(Some(p.unwrap), Some(s"${p} & Co. LLC"))
            .map(pd => pd.party: String)
        }
      // multi-party actAs on create
      cid <- fixture
        .headersWithPartyAuth(List(alice, bob))
        .flatMap(
          postCreateCommand(multiPartyCreateCommand(List(alice, bob), ""), fixture, _)
        )
        .map(resultContractId)
      // multi-party actAs on exercise
      cidMulti <- fixture
        .headersWithPartyAuth(knownParties)
        .flatMap(
          fixture.postJsonRequest(
            Uri.Path("/v1/exercise"),
            encodeExercise(encoder)(multiPartyAddSignatories(cid, List(charlie, david))),
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
        .headersWithPartyAuth(List(alice))
        .flatMap(
          postCreateCommand(
            multiPartyCreateCommand(List(alice), ""),
            fixture,
            _,
          )
        )
        .map(resultContractId)
      _ <- fixture
        .headersWithPartyAuth(List(charlie), readAs = List(alice))
        .flatMap(
          fixture.postJsonRequest(
            Uri.Path("/v1/exercise"),
            encodeExercise(encoder)(multiPartyFetchOther(cidMulti, cid, List(charlie))),
            _,
          )
        )
        .map { case (status, _) =>
          status shouldBe StatusCodes.OK
        }
    } yield succeed
  }

  "fetch by contractId" - {
    "succeeds normally" in withHttpService { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val command = iouCreateCommand(alice)

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
        val command = iouCommand(alice, CIou.CIou)

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

    "fails when readAs not authed, even if prior fetch succeeded" taggedAs authorizationSecurity
      .setAttack(
        Attack(
          "Ledger client",
          "fetches by contractId but readAs is not authorized",
          "refuse request with UNAUTHORIZED",
        )
      ) in withHttpService { fixture =>
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
        val command = accountCreateCommand(alice, accountNumber)

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

    "containing a decimal " - {
      Seq(
        "300000",
        "300000.0",
        "300000.000001",
        "300000.00000000000001", // Note this is more than the 6 decimal places allowed by the type
      ).foreach { numStr =>
        s"with value $numStr" in withHttpService { fixture =>
          fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
            testCreateAndFetchDecimalKey(fixture, numStr, alice, headers)
          }
        }
      }
    }
  }

  private def testCreateAndFetchDecimalKey(
      fixture: UriFixture,
      decimal: String,
      party: domain.Party,
      headers: List[HttpHeader],
  ) = {
    val createCommand = jsObject(s"""{
      "templateId": "Account:KeyedByDecimal",
      "payload": { "party": "$party", "amount": "$decimal" }
    }""")
    val fetchRequest = jsObject(s"""{
      "templateId": "Account:KeyedByDecimal",
      "key": [ "$party", "$decimal" ]
    }""")

    fixture
      .postJsonRequest(Uri.Path("/v1/create"), createCommand, headers)
      .parseResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]
      .flatMap(inside(_) {
        case domain.OkResponse(created, _, StatusCodes.OK) => {
          fixture
            .postJsonRequest(Uri.Path("/v1/fetch"), fetchRequest, headers)
            .parseResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]
            .flatMap(inside(_) { case domain.OkResponse(fetched, _, StatusCodes.OK) =>
              created.contractId shouldBe fetched.contractId
            })
        }
      }): Future[Assertion]
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
      .parseResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]
      .flatMap(inside(_) { case domain.OkResponse(c, _, StatusCodes.OK) =>
        val contractId: ContractId = c.contractId

        fixture
          .postJsonRequest(Uri.Path("/v1/fetch"), request, headers)
          .parseResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]
          .flatMap(inside(_) { case domain.OkResponse(c, _, StatusCodes.OK) =>
            c.contractId shouldBe contractId
          })
      }): Future[Assertion]
  }

  "archiving a large number of contracts should succeed" taggedAs availabilitySecurity.setHappyCase(
    "A ledger client can archive a large number of contracts"
  ) in withHttpService(
    maxInboundMessageSize = StartSettings.DefaultMaxInboundMessageSize * 10
  ) { fixture =>
    import fixture.encoder
    import org.scalacheck.{Arbitrary, Gen}
    fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
      // The numContracts size should test for https://github.com/digital-asset/daml/issues/10339
      val numContracts: Long = 2000
      val helperId = domain.ContractTypeId.Template(None, "Account", "Helper")
      val payload = recordFromFields(ShRecord(owner = v.Value.Sum.Party(alice.unwrap)))
      val createCmd: domain.CreateAndExerciseCommand.LAVUnresolved =
        domain.CreateAndExerciseCommand(
          templateId = helperId,
          payload = payload,
          choice = lar.Choice("CreateN"),
          argument = boxedRecord(recordFromFields(ShRecord(n = v.Value.Sum.Int64(numContracts)))),
          choiceInterfaceId = None,
          meta = None,
        )

      def encode(
          cmd: domain.CreateAndExerciseCommand.LAVUnresolved
      ): JsValue =
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
      import fixture.{client, encoder}
      import com.daml.ledger.api.refinements.{ApiTypes => lar}
      import shapeless.record.{Record => ShRecord}
      val partyManagement = client.partyManagementClient

      val partyIds = Vector("Alice", "Bob").map(getUniqueParty)
      val packageId: Ref.PackageId = MetadataReader
        .templateByName(metadataUser)(Ref.QualifiedName.assertFromString("User:User"))
        .collectFirst { case (pkgid, _) => pkgid }
        .getOrElse(fail(s"Cannot retrieve packageId"))

      def userCreateCommand(
          username: domain.Party,
          following: Seq[domain.Party] = Seq.empty,
      ): domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg] = {
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
      ): domain.ExerciseCommand[Nothing, v.Value, domain.EnrichedContractId] = {
        val reference = domain.EnrichedContractId(Some(TpId.User.User), contractId)
        val arg = recordFromFields(ShRecord(userToFollow = v.Value.Sum.Party(toFollow.unwrap)))
        val choice = lar.Choice("Follow")

        domain.ExerciseCommand(reference, choice, boxedRecord(arg), None, None)
      }

      def followUser(contractId: lar.ContractId, actAs: domain.Party, toFollow: domain.Party) = {
        val exercise = userExerciseFollowCommand(contractId, toFollow)
        val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

        fixture
          .headersWithPartyAuth(actAs = List(actAs))
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
          .headersWithPartyAuth(actAs = List(fromPerspectiveOfParty))
          .flatMap(headers => fixture.postJsonRequest(Uri.Path("/v1/query"), query, headers))
          .parseResponse[JsValue]
          .map(inside(_) { case domain.OkResponse(_, _, StatusCodes.OK) =>
          })
      }

      for {
        partyDetails <- partyIds.traverse { p =>
          partyManagement.allocateParty(Some(p.unwrap), None)
        }
        parties = domain.Party subst partyDetails.map(p => p.party: String)
        users <- parties.traverse { party =>
          val command = userCreateCommand(party)
          val fut = fixture
            .headersWithPartyAuth(actAs = List(party))
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

/** Tests that don't exercise the query store at all, but exercise different
  * paths due to authentication method.
  */
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class AbstractHttpServiceIntegrationTestQueryStoreIndependent
    extends QueryStoreAndAuthDependentIntegrationTest {
  import HttpServiceTestFixture.accountCreateCommand
  import json.JsonProtocol._

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
          .headersWithPartyAuth(List(alice, bob))
          .flatMap(headers => fixture.searchAllExpectOk(headers))
          .map(cs => cs should have size 2)
      } yield succeed
    }
  }

  "create" - {
    "succeeds with single party, proper argument" in withHttpService { fixture =>
      import fixture.encoder
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val command = iouCreateCommand(alice)

        postCreateCommand(command, fixture, headers)
          .map(inside(_) { case domain.OkResponse(activeContract, _, StatusCodes.OK) =>
            assertActiveContract(activeContract)(command, encoder)
          }): Future[Assertion]
      }
    }

    "fails if authorization header is missing" taggedAs authorizationSecurity.setAttack(
      Attack(
        "Ledger client",
        "calls /create without authorization",
        "refuse request with UNAUTHORIZED",
      )
    ) in withHttpService { fixture =>
      import fixture.encoder
      val alice = getUniqueParty("Alice")
      val command = iouCreateCommand(alice)
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
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (bob, _) <- fixture.getUniquePartyAndAuthHeaders("Bob")
        command = iouCreateCommand(alice)
        input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))
        headers <- fixture
          .headersWithPartyAuth(actAs = List(alice), readAs = List(bob))
        activeContractResponse <- fixture
          .postJsonRequest(
            Uri.Path("/v1/create"),
            input,
            headers,
          )
          .parseResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]
      } yield inside(activeContractResponse) {
        case domain.OkResponse(activeContract, _, StatusCodes.OK) =>
          assertActiveContract(activeContract)(command, encoder)
      }
    }

    "with unsupported templateId should return proper error" in withHttpService { fixture =>
      import fixture.encoder
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val command: domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg] =
          iouCreateCommand(alice)
            .copy(templateId = domain.ContractTypeId.Template(None, "Iou", "Dummy"))
        val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

        fixture
          .postJsonRequest(Uri.Path("/v1/create"), input, headers)
          .parseResponse[JsValue]
          .map(inside(_) { case domain.ErrorResponse(Seq(error), _, StatusCodes.BadRequest, _) =>
            val unknownTemplateId: domain.ContractTypeId.Template.OptionalPkg =
              command.templateId.copy(packageId = None)
            error should include(
              s"Cannot resolve template ID, given: ${unknownTemplateId}"
            )
          }): Future[Assertion]
      }
    }

    "supports command deduplication" in withHttpService { fixture =>
      import fixture.encoder
      def genSubmissionId() = domain.SubmissionId(UUID.randomUUID().toString)
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val cmdId = domain.CommandId apply UUID.randomUUID().toString
        def cmd(submissionId: domain.SubmissionId) =
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
                disclosedContracts = None,
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
    for {
      (alice, headers) <- fixture.getUniquePartyAndAuthHeaders("Alice")
      (bob, _) <- fixture.getUniquePartyAndAuthHeaders("Bob")
      cmd = iouCreateAndExerciseTransferCommand(alice, bob)
      json: JsValue = encoder.encodeCreateAndExerciseCommand(cmd).valueOr(e => fail(e.shows))

      res <- fixture
        .postJsonRequest(Uri.Path("/v1/create-and-exercise"), json, headers)
        .parseResponse[domain.ExerciseResponse[JsValue]]
      _ <- inside(res) { case domain.OkResponse(result, None, StatusCodes.OK) =>
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
      }
    } yield succeed
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
              val allocatedIds: Set[domain.Party] =
                domain.Party.subst(allocatedParties.map(p => p.party: String)).toSet
              actualIds should contain allElementsOf allocatedIds
              result.toSet should contain allElementsOf
                allocatedParties.toSet.map(domain.PartyDetails.fromLedgerApi)
              maxPartiesPageSize.fold(succeed)(pageSize => result.size should be > pageSize)
            })
        }: Future[Assertion]
    }

    "return only requested parties, unknown parties returned as warnings" in withHttpService {
      fixture =>
        import fixture.client
        val List(aliceName, bobName, charlieName, erinName) =
          List("Alice", "Bob", "Charlie", "Erin").map(getUniqueParty)
        // We do not allocate erin
        val namesToAllocate = List(aliceName, bobName, charlieName)
        val partyManagement = client.partyManagementClient

        namesToAllocate
          .traverse { p =>
            partyManagement.allocateParty(Some(p.unwrap), Some(s"${p.unwrap} & Co. LLC"))
          }
          .flatMap { allocatedParties =>
            {
              val allocatedPartiesHttpApi: List[domain.PartyDetails] =
                allocatedParties.map(domain.PartyDetails.fromLedgerApi)
              // Get alice, bob and charlies real party names
              val List(alice, bob, charlie) = allocatedPartiesHttpApi.map(_.identifier)
              fixture
                .postJsonRequest(
                  Uri.Path("/v1/parties"),
                  // Request alice and bob as normal, erin by name (as unallocated, she has no hash)
                  JsArray(Vector(alice, bob, erinName).map(x => JsString(x.unwrap))),
                  headersWithAdminAuth,
                )
                .parseResponse[List[domain.PartyDetails]]
                .flatMap(inside(_) {
                  case domain.OkResponse(result, Some(warnings), StatusCodes.OK) =>
                    warnings shouldBe domain.UnknownParties(List(erinName))
                    val actualIds: Set[domain.Party] = result.view.map(_.identifier).toSet
                    actualIds shouldBe Set(alice, bob) // Erin is not known
                    val expected: Set[domain.PartyDetails] = allocatedPartiesHttpApi.toSet
                      .filterNot(_.identifier == charlie)
                    result.toSet shouldBe expected
                })
            }
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
          newParty.identifier.toString should startWith(request.identifierHint.value.toString)
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

    "return BadRequest error if party ID hint is invalid PartyIdString" taggedAs authorizationSecurity
      .setAttack(
        Attack(
          "Ledger client",
          "tries to allocate a party with invalid Party ID",
          "refuse request with BAD_REQUEST",
        )
      ) in withHttpService { fixture =>
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

  "package list is updated when a query request is made" in usingLedger() {
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

  "metering-report endpoint should return metering report" in withHttpService { fixture =>
    val isoDate = "2022-02-03"
    val request = MeteringReportDateRequest(
      from = LocalDate.parse(isoDate),
      to = None,
      application = None,
    )
    for {
      reportStruct <- fixture
        .postJsonRequest(
          Uri.Path("/v1/metering-report"),
          request.toJson,
          headersWithAdminAuth,
        )
        .parseResponse[Struct]
    } yield inside(reportStruct) { case domain.OkResponse(meteringReport, _, StatusCodes.OK) =>
      meteringReport
        .fields("request")
        .getStructValue
        .fields("from")
        .getStringValue shouldBe s"${isoDate}T00:00:00Z"
    }
  }
}
