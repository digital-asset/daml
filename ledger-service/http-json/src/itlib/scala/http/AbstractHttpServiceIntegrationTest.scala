// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.time.{LocalDate}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model._
import com.daml.http.domain.{ContractId}
import com.daml.http.endpoints.MeteringReportEndpoint.MeteringReportDateRequest
import com.daml.http.json.SprayJson.objectField
import com.daml.http.json._
import com.daml.http.util.ClientUtil.{uniqueId}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.v1.{value => v}
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
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.{-\/, \/-}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success
import com.google.protobuf.struct.Struct

import java.util.UUID

trait AbstractHttpServiceIntegrationTestFunsCustomToken
    extends AsyncFreeSpec
    with AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.CustomToken
    with Matchers
    with Inside {

  import json.JsonProtocol._

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

  import AbstractHttpServiceIntegrationTestFuns.{UriFixture, EncoderFixture}
  import HttpServiceTestFixture.{UseTls, accountCreateCommand}
  import json.JsonProtocol._

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
      party: domain.Party
  ): List[domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg]] =
    List(
      iouCreateCommand(amount = "111.11", currency = "EUR", party = party),
      iouCreateCommand(amount = "222.22", currency = "EUR", party = party),
      iouCreateCommand(amount = "333.33", currency = "GBP", party = party),
      iouCreateCommand(amount = "444.44", currency = "BTC", party = party),
    )

  // Whether the underlying JSON query engine place unavoidable limits on the JSON queries that are supportable.
  protected def constrainedJsonQueries = false

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

  protected def assertExerciseResponseArchivedContract(
      exerciseResponse: domain.ExerciseResponse[JsValue],
      exercise: domain.ExerciseCommand.OptionalPkg[v.Value, domain.EnrichedContractId],
  ): Assertion =
    inside(exerciseResponse) { case domain.ExerciseResponse(exerciseResult, List(contract1), _) =>
      exerciseResult shouldBe JsObject()
      inside(contract1) { case domain.Contract(-\/(archivedContract)) =>
        (archivedContract.contractId.unwrap: String) shouldBe (exercise.reference.contractId.unwrap: String)
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

//    "containing variant and record" - {
//      "encoded as array with number num" in withHttpService { fixture =>
//        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
//          testFetchByCompositeKey(
//            fixture,
//            jsObject(s"""{
//            "templateId": "Account:KeyedByVariantAndRecord",
//            "key": [
//              "$alice",
//              {"tag": "Bar", "value": 42},
//              {"baz": "another baz value"}
//            ]
//          }"""),
//            alice,
//            headers,
//          )
//        }
//      }
//
//      "encoded as record with string num" in withHttpService { fixture =>
//        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
//          testFetchByCompositeKey(
//            fixture,
//            jsObject(s"""{
//            "templateId": "Account:KeyedByVariantAndRecord",
//            "key": {
//              "_1": "$alice",
//              "_2": {"tag": "Bar", "value": "42"},
//              "_3": {"baz": "another baz value"}
//            }
//          }"""),
//            alice,
//            headers,
//          )
//        }
//      }
//    }

//    "containing a decimal " - {
//      Seq(
//        "300000",
//        "300000.0",
//        "300000.000001",
//        "300000.00000000000001", // Note this is more than the 6 decimal places allowed by the type
//      ).foreach { numStr =>
//        s"with value $numStr" in withHttpService { fixture =>
//          fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
//            testCreateAndFetchDecimalKey(fixture, numStr, alice, headers)
//          }
//        }
//      }
//    }
  }

//  def testCreateAndFetchDecimalKey(
//      fixture: UriFixture,
//      decimal: String,
//      party: domain.Party,
//      headers: List[HttpHeader],
//  ) = {
//    val createCommand = jsObject(s"""{
//      "templateId": "Account:KeyedByDecimal",
//      "payload": { "party": "$party", "amount": "$decimal" }
//    }""")
//    val fetchRequest = jsObject(s"""{
//      "templateId": "Account:KeyedByDecimal",
//      "key": [ "$party", "$decimal" ]
//    }""")
//
//    fixture
//      .postJsonRequest(Uri.Path("/v1/create"), createCommand, headers)
//      .parseResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]
//      .flatMap(inside(_) {
//        case domain.OkResponse(created, _, StatusCodes.OK) => {
//          fixture
//            .postJsonRequest(Uri.Path("/v1/fetch"), fetchRequest, headers)
//            .parseResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]
//            .flatMap(inside(_) { case domain.OkResponse(fetched, _, StatusCodes.OK) =>
//              created.contractId shouldBe fetched.contractId
//            })
//        }
//      }): Future[Assertion]
//  }
//
//  def testFetchByCompositeKey(
//      fixture: UriFixture,
//      request: JsObject,
//      party: domain.Party,
//      headers: List[HttpHeader],
//  ) = {
//    val createCommand = jsObject(s"""{
//        "templateId": "Account:KeyedByVariantAndRecord",
//        "payload": {
//          "name": "ABC DEF",
//          "party": "${party.unwrap}",
//          "age": 123,
//          "fooVariant": {"tag": "Bar", "value": 42},
//          "bazRecord": {"baz": "another baz value"}
//        }
//      }""")
//    fixture
//      .postJsonRequest(Uri.Path("/v1/create"), createCommand, headers)
//      .parseResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]
//      .flatMap(inside(_) { case domain.OkResponse(c, _, StatusCodes.OK) =>
//        val contractId: ContractId = c.contractId
//
//        fixture
//          .postJsonRequest(Uri.Path("/v1/fetch"), request, headers)
//          .parseResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]
//          .flatMap(inside(_) { case domain.OkResponse(c, _, StatusCodes.OK) =>
//            c.contractId shouldBe contractId
//          })
//      }): Future[Assertion]
//  }

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
