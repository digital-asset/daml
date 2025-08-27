// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.ledger.api.v2.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.transaction_filter.*
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse.Update
import com.daml.ledger.api.v2.value as v
import com.daml.scalautil.Statement.discard
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.{Authorization, Availability}
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import com.digitalasset.canton.http.json.*
import com.digitalasset.canton.http.json.JsonProtocol.*
import com.digitalasset.canton.http.util.ClientUtil.{boxedRecord, uniqueCommandId, uniqueId}
import com.digitalasset.canton.http.{
  ActiveContract,
  AllocatePartyRequest as HttpAllocatePartyRequest,
  Base64,
  Choice,
  CommandId,
  CommandMeta,
  Contract,
  ContractId,
  ContractTypeId,
  CreateAndExerciseCommand,
  CreateCommand,
  CreateCommandResponse,
  DeduplicationPeriod,
  EnrichedContractId,
  EnrichedContractKey,
  ErrorInfoDetail,
  ErrorMessages,
  ErrorResponse,
  ExerciseCommand,
  ExerciseResponse,
  LedgerApiError,
  Offset,
  OkResponse,
  Party,
  PartyDetails as HttpPartyDetails,
  RequestInfoDetail,
  ResourceInfoDetail,
  SubmissionId,
  SyncResponse,
  UnknownParties,
  UnknownTemplateIds,
  WorkflowId,
  util,
}
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.digitalasset.canton.ledger.service.MetadataReader
import com.digitalasset.canton.logging.NamedLogging.loggerWithoutTracing
import com.digitalasset.canton.protocol.ExampleContractFactory
import com.digitalasset.canton.tracing.{SerializableTraceContextConverter, W3CTraceContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.test.TypedValueGenerators.ValueAddend as VA
import com.google.protobuf.ByteString
import org.apache.commons.lang3.RandomStringUtils
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.*
import scalaz.std.list.*
import scalaz.std.scalaFuture.*
import scalaz.std.tuple.*
import scalaz.std.vector.*
import scalaz.syntax.apply.*
import scalaz.syntax.bifunctor.*
import scalaz.syntax.show.*
import scalaz.syntax.std.boolean.*
import scalaz.syntax.tag.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, \/-}
import shapeless.record.Record as ShRecord
import spray.json.*

import java.util.UUID
import scala.annotation.nowarn
import scala.concurrent.Future
import scala.util.Success

trait AbstractHttpServiceIntegrationTestFunsUserToken extends HttpServiceUserFixture.UserToken {
  self: AbstractHttpServiceIntegrationTestFuns =>

  protected def testId: String = this.getClass.getSimpleName

  protected def headersWithUserAuth(
      user: Option[String]
  ) =
    HttpServiceTestFixture.headersWithUserAuth(user)

  "get all parties using the legacy token format" in httpTestFixture { fixture =>
    import fixture.client
    val partyIds = Vector("P1", "P2", "P3", "P4").map(getUniqueParty(_).unwrap)
    val partyManagement = client.partyManagementClient
    partyIds
      .traverse { p =>
        partyManagement.allocateParty(Some(p))
      }
      .flatMap { allocatedParties =>
        fixture
          .getRequest(
            Uri.Path("/v1/parties"),
            headersWithUserAuth(None),
          )
          .parseResponse[List[HttpPartyDetails]]
          .map(inside(_) { case OkResponse(result, None, StatusCodes.OK) =>
            result.toSet should contain allElementsOf
              allocatedParties.toSet.map(HttpPartyDetails.fromLedgerApi)
          })
      }: Future[Assertion]
  }

}

/** Tests that may behave differently depending on
  *
  *   1. whether custom or user tokens are used, ''and''
  *   1. the query store configuration
  */
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
@nowarn("msg=match may not be exhaustive")
abstract class AbstractHttpServiceIntegrationTest extends AbstractHttpServiceIntegrationTestFuns {

  import AbstractHttpServiceIntegrationTestFuns.*
  import HttpServiceTestFixture.{accountCreateCommand, archiveCommand}

  val authorizationSecurity: SecurityTest =
    SecurityTest(property = Authorization, asset = "HTTP JSON API Service")

  val availabilitySecurity: SecurityTest =
    SecurityTest(property = Availability, asset = "HTTP JSON API Service")

  protected def genSearchDataSet(
      party: Party
  ): List[CreateCommand[v.Record, ContractTypeId.Template.RequiredPkg]] =
    List(
      iouCreateCommand(amount = "111.11", currency = "EUR", party = party),
      iouCreateCommand(amount = "222.22", currency = "EUR", party = party),
      iouCreateCommand(amount = "333.33", currency = "GBP", party = party),
      iouCreateCommand(amount = "444.44", currency = "BTC", party = party),
    )

  def packageIdOfDar(darFile: java.io.File): String = {
    import com.digitalasset.daml.lf.{archive, typesig}
    val dar = archive.UniversalArchiveReader.assertReadFile(darFile)
    typesig.PackageSignature.read(dar.main)._2.packageId
  }

  protected def testLargeQueries = true

  "query POST with empty query" should {
    "single party" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val searchDataSet = genSearchDataSet(alice)
        searchExpectOk(
          searchDataSet,
          jsObject(s"""{"templateIds": ["${TpId.Iou.Iou.fqn}"]}"""),
          fixture,
          headers,
        ).map { (acl: List[ActiveContract.ResolvedCtTyId[JsValue]]) =>
          acl.size shouldBe searchDataSet.size
        }
      }
    }

    "multi-party" in httpTestFixture { fixture =>
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
          jsObject(s"""{"templateIds": ["${TpId.Account.Account.fqn}"]}"""),
          fixture,
          aliceHeaders,
        )
          .map(acl => acl.size shouldBe 1)
        _ <- searchExpectOk(
          List(),
          jsObject(s"""{"templateIds": ["${TpId.Account.Account.fqn}"]}"""),
          fixture,
          bobHeaders,
        )
          .map(acl => acl.size shouldBe 1)
        _ <- fixture
          .headersWithPartyAuth(List(alice, bob))
          .flatMap(headers =>
            searchExpectOk(
              List(),
              jsObject(s"""{"templateIds": ["${TpId.Account.Account.fqn}"]}"""),
              fixture,
              headers,
            )
          )
          .map(acl => acl.size shouldBe 2)
      } yield {
        assert(true)
      }
    }

    "with an interface ID" in httpTestFixture { fixture =>
      import com.digitalasset.canton.http.json.JsonProtocol.*
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        aliceH <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, aliceHeaders) = aliceH
        _ <- postCreateCommand(
          iouCommand(alice, TpId.CIou.CIou),
          fixture,
          aliceHeaders,
        )

        searchResp <-
          suppressPackageIdWarning {
            search(
              List.empty,
              Map(
                "templateIds" -> Seq(TpId.IIou.IIou).toJson
              ).toJson.asJsObject,
              fixture,
              aliceHeaders,
            )
          }
      } yield inside(searchResp) {
        case OkResponse(Seq(ac), None, StatusCodes.OK) => {
          discard {
            ac.templateId shouldBe TpId.IIou.IIou.copy(packageId = ac.templateId.packageId)
          }
          ac.payload shouldBe JsObject("amount" -> JsString("42"))
        }
      }
    }

    "multi-view" should {
      val amountsCurrencies = Vector(("42.0", "USD"), ("84.0", "CHF"))
      val expectedAmountsCurrencies = amountsCurrencies.map { case (a, c) => (a.toDouble, c) }

      def testMultiView[ExParties](
          fixture: HttpServiceTestFixtureData,
          allocateParties: Future[ExParties],
      )(
          observers: ExParties => Vector[Party],
          queryHeaders: (Party, List[HttpHeader], ExParties) => Future[List[HttpHeader]],
      ) = for {
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
          (ctid: ContractTypeId.RequiredPkg, amountKey: String, currencyKey: String) =>
            searchExpectOk(
              List.empty,
              Map("templateIds" -> List(ctid)).toJson.asJsObject,
              fixture,
              queryAsBoth,
            ) map { resACs =>
              inside(resACs map (inside(_) {
                case ActiveContract(cid, _, _, payload, Seq(`alice`), `exObservers`) =>
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
        _ <- queryAtCtId(TpId.Iou.IIou, "iamount", "icurrency")
        // then try template ID again, in case interface ID mangled the results
        // for template ID by way of stakeholder join or something even odder
        _ <- queryAtCtId(TpId.Iou.Iou, "amount", "currency")
      } yield succeed

      "multi-party" in httpTestFixture { fixture =>
        testMultiView(
          fixture,
          fixture.getUniquePartyAndAuthHeaders("bob").map(_._1),
        )(
          bob => Vector(bob),
          (alice, _, bob) => fixture.headersWithPartyAuth(List(alice), List(bob)),
        )
      }
    }
  }

  "query with unknown Template IDs" should {
    "warns if some are known" in httpTestFixture { fixture =>
      val query =
        jsObject(
          s"""{"templateIds": ["${TpId.Iou.Iou.fqn}", "UnknownPkg:UnknownModule:UnknownEntity"]}"""
        )
      fixture
        .getUniquePartyAndAuthHeaders("UnknownParty")
        .flatMap { case (_, headers) =>
          suppressPackageIdWarning {
            search(List(), query, fixture, headers).map { response =>
              inside(response) { case OkResponse(acl, warnings, StatusCodes.OK) =>
                acl.size shouldBe 0
                warnings shouldBe Some(
                  UnknownTemplateIds(
                    List(
                      ContractTypeId(
                        Ref.PackageRef.assertFromString("UnknownPkg"),
                        "UnknownModule",
                        "UnknownEntity",
                      )
                    )
                  )
                )
              }
            }
          }
        }
    }

    "fails if all are unknown" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        search(
          genSearchDataSet(alice),
          jsObject("""{"templateIds": ["UnknownPkg:AAA:BBB", "UnknownPkg:ZZZ:YYY"]}"""),
          fixture,
          headers,
        ).map { response =>
          inside(response) { case ErrorResponse(errors, warnings, StatusCodes.BadRequest, _) =>
            errors shouldBe List(ErrorMessages.cannotResolveAnyTemplateId)
            inside(warnings) { case Some(UnknownTemplateIds(unknownTemplateIds)) =>
              unknownTemplateIds.toSet shouldBe Set(
                ContractTypeId(Ref.PackageRef.assertFromString("UnknownPkg"), "AAA", "BBB"),
                ContractTypeId(Ref.PackageRef.assertFromString("UnknownPkg"), "ZZZ", "YYY"),
              )
            }
          }
        }
      }
    }
  }

  "query multiple observers:" should {
    Seq(
      0 -> 1,
      1 -> 5,
      10 -> 75,
      50 -> 76, // Allows space to encode content into a JSON array of strings within 4k limit.
      50 -> 80, // The content is the exact 4k limit, no additional room for JSON array syntax.
      1000 -> 185,
    ).foreach { case (numSubs, partySize) =>
      (s"$numSubs observers of $partySize chars") in httpTestFixture { fixture =>
        val subscribers = (1 to numSubs).map(_ => Party(randomTextN(partySize))).toList
        for {
          (publisher, headers) <- fixture.getUniquePartyAndAuthHeaders("Alice")
          subscriberPartyDetails <- subscribers.traverse { p =>
            fixture.client.partyManagementClient.allocateParty(Some(p.unwrap))
          }
          subscriberParties = Party subst subscriberPartyDetails.map(p => p.party: String)
          found <- searchExpectOk(
            List(pubSubCreateCommand(publisher, subscriberParties)),
            jsObject(s"""{"templateIds": ["${TpId.Account.PubSub.fqn}"]}"""),
            fixture,
            headers,
          )
        } yield {
          found.size shouldBe 1
        }
      }
    }
  }

  protected implicit final class `AHS TI uri funs`(private val fixture: UriFixture) {

    def searchAllExpectOk(
        headers: List[HttpHeader]
    ): Future[List[ActiveContract.ResolvedCtTyId[JsValue]]] =
      searchAll(headers).map(expectOk(_))

    def searchAllExpectOk(
    ): Future[List[ActiveContract.ResolvedCtTyId[JsValue]]] =
      fixture.headersWithAuth.flatMap(searchAllExpectOk(_))

    def searchAll(
        headers: List[HttpHeader]
    ): Future[SyncResponse[List[ActiveContract.ResolvedCtTyId[JsValue]]]] =
      fixture
        .getRequest(Uri.Path("/v1/query"), headers)
        .parseResponse[List[ActiveContract.ResolvedCtTyId[JsValue]]]

  }

  "exercise" should {
    "succeeds normally" in httpTestFixture { fixture =>
      import fixture.encoder
      for {
        (alice, headers) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (bob, _) <- fixture.getUniquePartyAndAuthHeaders("Bob")
        create = iouCreateCommand(alice)
        res <- postCreateCommand(create, fixture, headers)
        _ <- inside(res) { case OkResponse(createResult, _, StatusCodes.OK) =>
          val exercise = iouExerciseTransferCommand(createResult.contractId, bob)
          val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

          fixture
            .postJsonRequest(Uri.Path("/v1/exercise"), exerciseJson, headers)
            .parseResponse[ExerciseResponse[JsValue]]
            .flatMap(inside(_) { case OkResponse(result, _, StatusCodes.OK) =>
              assertExerciseResponseNewActiveContract(
                result,
                create,
                exercise,
                fixture,
                headers,
              )
            })
        }
      } yield succeed
    }

    "with unknown contractId should return proper error" in httpTestFixture { fixture =>
      import fixture.encoder
      val contractIdString = ExampleContractFactory.buildContractId().coid
      val contractId = lar.ContractId(contractIdString)
      for {
        (bob, headers) <- fixture.getUniquePartyAndAuthHeaders("Bob")
        exerciseJson: JsValue =
          encodeExercise(encoder)(iouExerciseTransferCommand(contractId, bob))
        _ <- fixture
          .postJsonRequest(Uri.Path("/v1/exercise"), exerciseJson, headers)
          .parseResponse[ExerciseResponse[JsValue]]
          .map(inside(_) {
            case ErrorResponse(Seq(errorMsg), None, StatusCodes.NotFound, Some(ledgerApiError)) =>
              errorMsg should include(
                s"Contract could not be found with id $contractIdString"
              )
              ledgerApiError.message should include("CONTRACT_NOT_FOUND")
              ledgerApiError.message should include(
                s"Contract could not be found with id $contractIdString"
              )
              forExactly(1, ledgerApiError.details) {
                case ErrorInfoDetail(errorCodeId, _) =>
                  errorCodeId shouldBe "CONTRACT_NOT_FOUND"
                case _ => fail()
              }
              forExactly(1, ledgerApiError.details) {
                case RequestInfoDetail(_) => succeed
                case _ => fail()
              }
              forExactly(1, ledgerApiError.details) {
                case ResourceInfoDetail(name, typ) =>
                  name shouldBe contractIdString
                  typ shouldBe "CONTRACT_ID"
                case _ => fail()
              }
          })
      } yield succeed
    }

    "Archive" in httpTestFixture { fixture =>
      import fixture.encoder
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val create = iouCreateCommand(alice)
        postCreateCommand(create, fixture, headers)
          .flatMap(inside(_) { case OkResponse(createResult, _, StatusCodes.OK) =>
            val reference = EnrichedContractId(Some(TpId.Iou.Iou), createResult.contractId)
            val exercise = archiveCommand(reference)
            val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

            fixture
              .postJsonRequest(Uri.Path("/v1/exercise"), exerciseJson, headers)
              .parseResponse[ExerciseResponse[JsValue]]
              .map(inside(_) { case OkResponse(exercisedResponse, _, StatusCodes.OK) =>
                assertExerciseResponseArchivedContract(exercisedResponse, exercise)
              })
          })
      }
    }

    def postCreate(
        fixture: HttpServiceTestFixtureData,
        payload: JsValue,
        headers: List[HttpHeader],
    ): Future[ContractId] =
      fixture
        .postJsonRequest(Uri.Path("/v1/create"), payload, headers)
        .parseResponse[ActiveContract.ResolvedCtTyId[JsValue]]
        .map(resultContractId)

    // TODO(i24322): Port upgrading tests to JSON.v2
    "should handle multiple package ids with the same name" in httpTestFixture { fixture =>
      import AbstractHttpServiceIntegrationTestFuns.{fooV1Dar, fooV2Dar}
      for {
        _ <- uploadPackage(fixture)(fooV1Dar)
        _ <- uploadPackage(fixture)(fooV2Dar)

        pkgIdV1 = packageIdOfDar(fooV1Dar)
        pkgIdV2 = packageIdOfDar(fooV2Dar)

        (alice, hdrs) <- fixture.getUniquePartyAndAuthHeaders("Alice")

        // create v1 and v2 versions of contract, using the package name and package id.
        cidV1PkgId <- postCreate(
          fixture,
          jsObject(s"""{"templateId": "$pkgIdV1:Foo:Bar", "payload": {"owner": "$alice"}}"""),
          hdrs,
        )
        cidV1PkgNm <- postCreate(
          fixture,
          // Payload per V1 but interpreted as V2, as the current highest version with this name.
          jsObject(s"""{"templateId": "#foo:Foo:Bar", "payload": {"owner": "$alice"}}"""),
          hdrs,
        )
        cidV1PkgNmWithV1Pref <- postCreate(
          fixture,
          // Payload per V1 and interpreted as V1, due to the explicit package id preference.
          jsObject(
            s"""{"templateId": "#foo:Foo:Bar", "payload": {"owner": "$alice"}, "meta":{"packageIdSelectionPreference":["$pkgIdFooV1"]}}"""
          ),
          hdrs,
        )
        cidV2PkgId <- postCreate(
          fixture,
          jsObject(
            s"""{"templateId": "$pkgIdV2:Foo:Bar", "payload": {"owner": "$alice", "extra":42}}"""
          ),
          hdrs,
        )
        cidV2PkgNm <- postCreate(
          fixture,
          jsObject(
            s"""{"templateId": "#foo:Foo:Bar", "payload": {"owner": "$alice", "extra":42}}"""
          ),
          hdrs,
        )

        // query using both package ids and package name should lead to same results since package-id queries are
        // internally transformed to package-name queries

        _ <- searchExpectOk(
          Nil,
          jsObject(s"""{"templateIds": ["$pkgIdV1:Foo:Bar"]}"""),
          fixture,
          hdrs,
        ) map { results =>
          results.map(_.contractId) should contain theSameElementsAs List(
            cidV1PkgId,
            cidV1PkgNm,
            cidV1PkgNmWithV1Pref,
            cidV2PkgId,
            cidV2PkgNm,
          )
        }

        _ <- searchExpectOk(
          Nil,
          jsObject(s"""{"templateIds": ["$pkgIdV2:Foo:Bar"]}"""),
          fixture,
          hdrs,
        ) map { results =>
          results.map(_.contractId) should contain theSameElementsAs List(
            cidV1PkgId,
            cidV1PkgNm,
            cidV1PkgNmWithV1Pref,
            cidV2PkgId,
            cidV2PkgNm,
          )
        }

        _ <- searchExpectOk(
          Nil,
          jsObject(s"""{"templateIds": ["#foo:Foo:Bar"]}"""),
          fixture,
          hdrs,
          suppressWarnings = false,
        ) map { results =>
          results.map(_.contractId) should contain theSameElementsAs List(
            cidV1PkgId,
            cidV1PkgNm,
            cidV1PkgNmWithV1Pref,
            cidV2PkgId,
            cidV2PkgNm,
          )
        }
      } yield succeed
    }

    "should recognise an archive against a newer version of the same contract" in httpTestFixture {
      fixture =>
        import AbstractHttpServiceIntegrationTestFuns.{fooV1Dar, fooV2Dar}

        for {
          _ <- uploadPackage(fixture)(fooV1Dar)

          (alice, hdrs) <- fixture.getUniquePartyAndAuthHeaders("Alice")

          // Create using package package name. The created event will contain the package id from v1.
          createdCid <- postCreate(
            fixture,
            jsObject(s"""{"templateId": "#foo:Foo:Bar", "payload": {"owner": "$alice"}}"""),
            hdrs,
          )

          // Query using package name
          _ <- searchExpectOk(
            Nil,
            jsObject(s"""{"templateIds": ["#foo:Foo:Bar"]}"""),
            fixture,
            hdrs,
            suppressWarnings = false,
          ) map { results =>
            results.map(_.contractId) shouldBe List(createdCid)
          }

          // Upload v2 of the same package.
          _ <- uploadPackage(fixture)(fooV2Dar)

          // Archive using package name but the exercise event will contain the package id from v2.
          _ <- fixture
            .postJsonRequest(
              Uri.Path("/v1/exercise"),
              jsObject(s"""{
            "templateId": "#foo:Foo:Bar",
            "contractId": "$createdCid",
            "choice": "Archive",
            "argument": {}
          }"""),
              hdrs,
            )
            .parseResponse[ExerciseResponse[JsValue]]
            .flatMap(inside(_) {
              case OkResponse(
                    ExerciseResponse(_, List(Contract(-\/(archived))), _),
                    _,
                    StatusCodes.OK,
                  ) =>
                Future {
                  archived.contractId shouldBe createdCid
                }
            })

          // The query should no longer serve the contract, as it is no longer in the ACS.
          _ <- searchExpectOk(
            Nil,
            jsObject(s"""{"templateIds": ["#foo:Foo:Bar"]}"""),
            fixture,
            hdrs,
            suppressWarnings = false,
          ) map { results =>
            results.map(_.contractId) shouldBe List.empty
          }

        } yield succeed
    }

    "should support create and exerciseByKey with package names" in httpTestFixture { fixture =>
      val tmplId = "#foo:Foo:Quux"
      for {
        _ <- uploadPackage(fixture)(AbstractHttpServiceIntegrationTestFuns.fooV2Dar)

        (alice, hdrs) <- fixture.getUniquePartyAndAuthHeaders("Alice")

        // create using package name.
        cid <- postCreate(
          fixture,
          jsObject(s"""{"templateId": "$tmplId", "payload": {"owner": "$alice"}}"""),
          hdrs,
        )

        // exercise by key, to test resolution of the key type, the arg type and the result type.
        // TODO(#20994) Use the key rather than the contract id
//      locator = s""""key": {"value": "$alice"}"""
        locator = s""""contractId": "$cid""""
        _ <- fixture
          .postJsonRequest(
            Uri.Path("/v1/exercise"),
            jsObject(s"""{
              "templateId": "$tmplId",
              $locator,
              "choice": "Incr",
              "argument": {"a": {"value": 42}}
            }"""),
            hdrs,
          )
          .parseResponse[ExerciseResponse[JsValue]]
          .flatMap(inside(_) {
            case OkResponse(
                  ExerciseResponse(jsResult, List(Contract(-\/(archived))), _),
                  _,
                  StatusCodes.OK,
                ) =>
              Future {
                archived.contractId shouldBe cid
                val json = """{"value": "43"}"""
                jsResult shouldBe jsObject(json)
              }
          })
      } yield succeed
    }

    // TODO(https://github.com/DACH-NY/canton/issues/16065): re-enable or adapt once 3.x supports contract keys
    "Archive by key" ignore httpTestFixture { fixture =>
      import fixture.encoder
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val accountNumber = "abc123"
        val create: CreateCommand[v.Record, ContractTypeId.Template.RequiredPkg] =
          accountCreateCommand(alice, accountNumber)

        val keyRecord = v.Record(
          fields = Seq(
            v.RecordField(value = Some(v.Value(v.Value.Sum.Party(alice.unwrap)))),
            v.RecordField(value = Some(v.Value(v.Value.Sum.Text(accountNumber)))),
          )
        )
        val locator = EnrichedContractKey[v.Value](
          TpId.Account.Account,
          v.Value(v.Value.Sum.Record(keyRecord)),
        )
        val archive = archiveCommand(locator)
        val archiveJson: JsValue = encodeExercise(encoder)(archive)

        postCreateCommand(create, fixture, headers).flatMap(inside(_) {
          case OkResponse(_, _, StatusCodes.OK) =>
            fixture
              .postJsonRequest(Uri.Path("/v1/exercise"), archiveJson, headers)
              .parseResponse[JsValue]
              .map(inside(_) { case OkResponse(_, _, StatusCodes.OK) =>
                succeed
              })
        }): Future[Assertion]
      }
    }

    "passes along disclosed contracts in" should {
      import com.daml.ledger.api.v2 as lav2
      import com.digitalasset.canton.http.DisclosedContract as DC
      import lav2.commands.Command
      import util.IdentifierConverters.{lfIdentifier, refApiIdentifier}

      def unwrapPkgId(
          ctid: ContractTypeId.RequiredPkg
      ): ContractTypeId.RequiredPkgId =
        inside(ctid.packageId) { case Ref.PackageRef.Id(pid) => ctid.copy(packageId = pid) }

      lazy val (_, toDiscloseVA) =
        VA.record(
          lfIdentifier(unwrapPkgId(TpId.Disclosure.ToDisclose)),
          ShRecord(owner = VAx.partySynchronizer, junk = VA.text),
        )

      lazy val (_, anotherToDiscloseVA) =
        VA.record(
          lfIdentifier(unwrapPkgId(TpId.Disclosure.ToDisclose)),
          ShRecord(owner = VAx.partySynchronizer, garbage = VA.text),
        )

      val (_, viewportVA) =
        VA.record(
          lfIdentifier(unwrapPkgId(TpId.Disclosure.Viewport)),
          ShRecord(owner = VAx.partySynchronizer),
        )

      val (_, checkVisibilityVA) =
        VA.record(
          lfIdentifier(unwrapPkgId(TpId.Disclosure.CheckVisibility)),
          ShRecord(
            disclosed = VAx.contractIdSynchronizer,
            ifaceDisclosed = VAx.contractIdSynchronizer,
          ),
        )

      final case class ContractsToDisclose(
          alice: Party,
          toDiscloseCid: ContractId,
          toDiscloseCreatedEventBlob: ByteString,
          anotherToDiscloseCid: ContractId,
          anotherToDiscloseCreatedEventBlob: ByteString,
      )

      def formatWithPayloadsFor(party: Party) = EventFormat(
        filtersByParty = Map(
          party.unwrap -> Filters(
            Seq(
              CumulativeFilter(
                IdentifierFilter.InterfaceFilter(
                  InterfaceFilter(
                    interfaceId = Some(refApiIdentifier(TpId.Disclosure.HasGarbage).unwrap),
                    includeCreatedEventBlob = true,
                    includeInterfaceView = false,
                  )
                )
              )
            ) :+
              CumulativeFilter(
                IdentifierFilter.TemplateFilter(
                  TemplateFilter(
                    templateId = Some(refApiIdentifier(TpId.Disclosure.ToDisclose).unwrap),
                    includeCreatedEventBlob = true,
                  )
                )
              )
          )
        ),
        filtersForAnyParty = None,
        verbose = false,
      )

      def contractsToDisclose(
          fixture: HttpServiceTestFixtureData,
          junkMessage: String,
          garbageMessage: String,
      ): Future[ContractsToDisclose] = for {
        (alice, jwt, userId, _) <- fixture.getUniquePartyTokenUserIdAndAuthHeaders("Alice")
        // we're using the ledger API for the initial create because timestamp
        // is required in the metadata
        toDisclosePayload = argToApi(toDiscloseVA)(ShRecord(owner = alice, junk = junkMessage))
        anotherToDisclosePayload = argToApi(anotherToDiscloseVA)(
          ShRecord(owner = alice, garbage = garbageMessage)
        )
        createCommands = Seq(
          (TpId.Disclosure.ToDisclose, toDisclosePayload),
          (TpId.Disclosure.AnotherToDisclose, anotherToDisclosePayload),
        ) map { case (tpid, payload) =>
          Command(util.Commands.create(refApiIdentifier(tpid), payload))
        }
        initialCreate =
          SubmitAndWaitRequest(
            commands = Some(
              Commands.defaultInstance.copy(
                commandId = uniqueCommandId().unwrap,
                userId = userId.unwrap,
                actAs = Party unsubst Seq(alice),
                commands = createCommands,
              )
            )
          )
        createResp <- fixture.client.commandService
          .submitAndWaitForTransactionForJsonApi(initialCreate, token = Some(jwt.value))
        // fetch what we can from the command service transaction
        (ceAtOffset, (toDiscloseCid, atdCid)) = inside(
          createResp.transaction
        ) { case Some(tx) =>
          import lav2.event.Event
          import Event.Event.Created
          inside(tx.events) { case Seq(Event(Created(ce0)), Event(Created(ce1))) =>
            val EntityTD = TpId.Disclosure.ToDisclose.entityName
            val EntityATD = TpId.Disclosure.AnotherToDisclose.entityName
            val orderedCes = inside((ce0, ce1) umap (_.templateId.map(_.entityName))) {
              case (Some(EntityTD), Some(EntityATD)) => (ce0, ce1)
              case (Some(EntityATD), Some(EntityTD)) => (ce1, ce0)
            }
            (
              tx.offset,
              orderedCes umap { ce => ContractId(ce.contractId) },
            )
          }
        }
        // use the transaction service to get the blob, which submit-and-wait
        // doesn't include in the response
        payloadsToDisclose <- {
          import lav2.event.Event
          import Event.Event.Created
          suppressPackageIdWarning {
            fixture.client.updateService
              .getUpdatesSource(
                begin = 0L,
                eventFormat = formatWithPayloadsFor(alice),
                end = Some(ceAtOffset),
                token = Some(jwt.value),
              )
              .collect { response =>
                response.update match {
                  case Update.Transaction(t) => t
                }
              }
              .mapConcat(_.events)
              .collect {
                case Event(Created(ce))
                    if ce.contractId == toDiscloseCid.unwrap || ce.contractId == atdCid.unwrap =>
                  Base64(ce.createdEventBlob)
              }
              .runWith(Sink.seq)
          }
        }
        (firstPayload, anotherPayload) = inside(payloadsToDisclose) { case Seq(first, second) =>
          first -> second
        }
      } yield ContractsToDisclose(
        alice,
        toDiscloseCid,
        firstPayload.unwrap,
        atdCid,
        anotherPayload.unwrap,
      )

      def runDisclosureTestCase[Setup](
          fixture: HttpServiceTestFixtureData
      )(exerciseEndpoint: Uri.Path, setupBob: (Party, List[HttpHeader]) => Future[Setup])(
          exerciseVaryingOnlyMeta: (
              Setup,
              ContractsToDisclose,
              Option[CommandMeta[ContractTypeId.Template.RequiredPkg]],
          ) => JsValue
      ): Future[Assertion] = {
        val junkMessage = s"some test junk ${uniqueId()}"
        val garbageMessage = s"some test garbage ${uniqueId()}"
        for {
          // first, set up something for alice to disclose to bob
          toDisclose @ ContractsToDisclose(
            alice,
            toDiscloseCid,
            firstPayload,
            atdCid,
            anotherPayload,
          ) <-
            contractsToDisclose(fixture, junkMessage, garbageMessage)

          // next, onboard bob to try to interact with the disclosed contract
          (bob, bobHeaders) <- fixture.getUniquePartyAndAuthHeaders("Bob")
          setup <- setupBob(bob, bobHeaders)

          // exercise CheckVisibility with different disclosure options
          checkVisibility = { (disclosure: List[DC[ContractTypeId.Template.RequiredPkg]]) =>
            val meta = disclosure.nonEmpty option CommandMeta(
              None,
              None,
              None,
              None,
              None,
              None,
              disclosedContracts = Some(disclosure),
              None,
              None,
            )
            fixture
              .postJsonRequest(
                exerciseEndpoint,
                exerciseVaryingOnlyMeta(setup, toDisclose, meta),
                bobHeaders,
              )
              .parseResponse[ExerciseResponse[JsValue]]
          }

          // ensure that bob can't interact with alice's contract unless it's disclosed
          _ <- checkVisibility(List.empty)
            .map(inside(_) {
              case ErrorResponse(
                    _,
                    _,
                    StatusCodes.NotFound,
                    Some(LedgerApiError(lapiCode, errorMessage, _)),
                  ) =>
                lapiCode should ===(com.google.rpc.Code.NOT_FOUND_VALUE)
                errorMessage should include(toDiscloseCid.unwrap)
            })

          _ <- checkVisibility(
            List(
              DC(
                toDiscloseCid,
                TpId.Disclosure.ToDisclose,
                Base64(firstPayload),
              ),
              DC(
                atdCid,
                TpId.Disclosure.AnotherToDisclose,
                Base64(anotherPayload),
              ),
            )
          )
            .map(inside(_) { case OkResponse(ExerciseResponse(JsString(exResp), _, _), _, _) =>
              exResp should ===(s"'$bob' can see from '$alice': $junkMessage, $garbageMessage")
            })
        } yield succeed
      }

      val checkVisibilityChoice = Choice("CheckVisibility")

      "exercise" in httpTestFixture { fixture =>
        runDisclosureTestCase(fixture)(
          Uri.Path("/v1/exercise"),
          (bob, bobHeaders) =>
            postCreateCommand(
              CreateCommand(
                TpId.Disclosure.Viewport,
                argToApi(viewportVA)(ShRecord(owner = bob)),
                None,
              ),
              fixture,
              bobHeaders,
            ) map resultContractId,
        ) { (viewportCid, toDisclose, meta) =>
          encodeExercise(fixture.encoder)(
            ExerciseCommand(
              EnrichedContractId(Some(TpId.Disclosure.Viewport), viewportCid),
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

      "create-and-exercise" in httpTestFixture { fixture =>
        runDisclosureTestCase(fixture)(
          Uri.Path("/v1/create-and-exercise"),
          (bob, _) => Future successful bob,
        ) { (bob, toDisclose, meta) =>
          fixture.encoder
            .encodeCreateAndExerciseCommand(
              CreateAndExerciseCommand(
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
      exerciseResponse: ExerciseResponse[JsValue],
      createCmd: CreateCommand[v.Record, ContractTypeId.Template.RequiredPkg],
      exerciseCmd: ExerciseCommand[Any, v.Value, EnrichedContractId],
      fixture: HttpServiceTestFixtureData,
      headers: List[HttpHeader],
  ): Future[Assertion] = {
    import fixture.{decoder, uri}
    inside(exerciseResponse) {
      case ExerciseResponse(
            JsString(exerciseResult),
            List(contract1, contract2),
            completionOffset,
          ) =>
        completionOffset.unwrap should not be empty
        // checking contracts
        inside(contract1) { case Contract(-\/(archivedContract)) =>
          Future {
            (archivedContract.contractId: ContractId) shouldBe (exerciseCmd.reference.contractId: ContractId)
          }
        } *>
          inside(contract2) { case Contract(\/-(activeContract)) =>
            assertActiveContract(uri)(decoder, activeContract, createCmd, exerciseCmd, fixture)
          } *>
          // checking exerciseResult
          {
            exerciseResult.length should be > (0)
            val newContractLocator = EnrichedContractId(
              Some(TpId.Iou.IouTransfer),
              ContractId(exerciseResult),
            )
            postContractsLookup(newContractLocator, uri, headers).map(inside(_) {
              case OkResponse(Some(contract), _, StatusCodes.OK) =>
                contract.contractId shouldBe newContractLocator.contractId
            }): Future[Assertion]
          }
    }
  }

  "should support multi-party command submissions" in httpTestFixture { fixture =>
    import fixture.{client, encoder}
    val knownPartyNames = List("Alice", "Bob", "Charlie", "David").map(getUniqueParty)
    val partyManagement = client.partyManagementClient
    for {
      knownParties @ List(alice, bob, charlie, david) <-
        knownPartyNames.traverse { p =>
          Party subst partyManagement
            .allocateParty(Some(p.unwrap))
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
        .parseResponse[ExerciseResponse[JsValue]]
        .map(inside(_) { case OkResponse(ExerciseResponse(JsString(c), _, _), _, StatusCodes.OK) =>
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

  private def assertExerciseResponseArchivedContract(
      exerciseResponse: ExerciseResponse[JsValue],
      exercise: ExerciseCommand.RequiredPkg[v.Value, EnrichedContractId],
  ): Assertion =
    inside(exerciseResponse) { case ExerciseResponse(exerciseResult, List(contract1), _) =>
      exerciseResult shouldBe JsObject()
      inside(contract1) { case Contract(-\/(archivedContract)) =>
        (archivedContract.contractId.unwrap: String) shouldBe (exercise.reference.contractId.unwrap: String)
      }
    }

  "fetch by contractId" should {
    "succeeds normally" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val command = iouCreateCommand(alice)

        postCreateCommand(command, fixture, headers).flatMap(inside(_) {
          case OkResponse(result, _, StatusCodes.OK) =>
            val contractId: ContractId = result.contractId
            val locator = EnrichedContractId(None, contractId)
            lookupContractAndAssert(locator, contractId, command, fixture, headers)
        }): Future[Assertion]
      }
    }

    "succeeds normally with an interface ID" in httpTestFixture { fixture =>
      uploadPackage(fixture)(ciouDar).flatMap { case _ =>
        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
          val command = iouCommand(alice, TpId.CIou.CIou)
          postCreateCommand(command, fixture, headers).flatMap(inside(_) {
            case OkResponse(result, _, StatusCodes.OK) =>
              val contractId: ContractId = result.contractId
              val locator = EnrichedContractId(Some(TpId.IIou.IIou), contractId)
              postContractsLookup(locator, fixture.uri, headers).map(inside(_) {
                case OkResponse(Some(resultContract), _, StatusCodes.OK) =>
                  contractId shouldBe resultContract.contractId
                  assertJsPayload(resultContract)(result.payload)
              })
          }): Future[Assertion]
        }
      }
    }

//      TODO(#16065)
//    "returns {status:200, result:null} when contract is not found" in httpTestFixture { fixture =>
//      import fixture.uri
//      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
//        val accountNumber = "abc123"
//        val locator = synchronizer.EnrichedContractKey(
//          TpId.Account.Account,
//          JsArray(JsString(alice.unwrap), JsString(accountNumber)),
//        )
//        postContractsLookup(locator, uri.withPath(Uri.Path("/v1/fetch")), headers).map(inside(_) {
//          case synchronizer.OkResponse(None, _, StatusCodes.OK) =>
//            succeed
//        }): Future[Assertion]
//      }
//    }

    "fails when readAs not authed, even if prior fetch succeeded" taggedAs authorizationSecurity
      .setAttack(
        Attack(
          "Ledger client",
          "fetches by contractId but readAs is not authorized",
          "refuse request with UNAUTHORIZED",
        )
      ) in httpTestFixture { fixture =>
      import fixture.uri
      for {
        res <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, aliceHeaders) = res
        command = iouCreateCommand(alice)
        createStatusOutput <- postCreateCommand(command, fixture, aliceHeaders)
        contractId = inside(createStatusOutput) { case OkResponse(result, _, StatusCodes.OK) =>
          result.contractId
        }
        locator = EnrichedContractId(None, contractId)
        // will cache if DB configured
        _ <- lookupContractAndAssert(locator, contractId, command, fixture, aliceHeaders)
        charlie = getUniqueParty("Charlie")
        badLookup <- postContractsLookup(
          locator,
          uri.withPath(Uri.Path("/v1/fetch")),
          aliceHeaders,
          readAs = Some(List(charlie)),
        )
      } yield inside(badLookup) { case ErrorResponse(_, None, StatusCodes.Unauthorized, None) =>
        succeed
      }
    }
  }

//  "fetch by key" in {
//    "succeeds normally" in httpTestFixture { fixture =>
//      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
//        val accountNumber = "abc123"
//        val command = accountCreateCommand(alice, accountNumber)
//
//        postCreateCommand(command, fixture, headers).flatMap(inside(_) {
//          case synchronizer.OkResponse(result, _, StatusCodes.OK) =>
//            val contractId: ContractId = result.contractId
//            val locator = synchronizer.EnrichedContractKey(
//              TpId.Account.Account,
//              JsArray(JsString(alice.unwrap), JsString(accountNumber)),
//            )
//            lookupContractAndAssert(locator, contractId, command, fixture, headers)
//        }): Future[Assertion]
//      }
//    }
//
//    "containing variant and record" should {
//      "encoded as array with number num" in httpTestFixture { fixture =>
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
//      "encoded as record with string num" in httpTestFixture { fixture =>
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
//
//    "containing a decimal " should {
//      Seq(
////        "300000",
////        "300000.0",
//        // TODO(#13813): Due to big decimal normalization, you can only fetch a key if you
//        //               use the exactly normalized value
//        "300000.000001"
////        "300000.00000000000001", // Note this is more than the 6 decimal places allowed by the type
//      ).foreach { numStr =>
//        s"with value $numStr" in httpTestFixture { fixture =>
//          fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
//            testCreateAndFetchDecimalKey(fixture, numStr, alice, headers)
//          }
//        }
//      }
//    }
//  }

  "Should ignore conflicts on contract key hash constraint violation" in httpTestFixture {
    fixture =>
      import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
      import fixture.{client, encoder}
      import shapeless.record.Record as ShRecord
      val partyManagement = client.partyManagementClient

      val partyIds = Vector("Alice", "Bob").map(getUniqueParty)
      val packageId: Ref.PackageId = MetadataReader
        .templateByName(metadataUser)(Ref.QualifiedName.assertFromString("User:User"))
        .collectFirst { case (pkgid, _) => pkgid }
        .getOrElse(fail(s"Cannot retrieve packageId"))

      def userCreateCommand(
          username: Party,
          following: Seq[Party] = Seq.empty,
      ): CreateCommand[v.Record, ContractTypeId.Template.RequiredPkg] = {
        val followingList = lfToApi(
          VAx.seq(VAx.partySynchronizer).inj(following)
        ).sum
        val arg = recordFromFields(
          ShRecord(
            username = v.Value.Sum.Party(username.unwrap),
            following = followingList,
          )
        )

        CreateCommand(TpId.User.User, arg, None)
      }

      def userExerciseFollowCommand(
          contractId: lar.ContractId,
          toFollow: Party,
      ): ExerciseCommand[Nothing, v.Value, EnrichedContractId] = {
        val reference = EnrichedContractId(Some(TpId.User.User), contractId)
        val arg = recordFromFields(ShRecord(userToFollow = v.Value.Sum.Party(toFollow.unwrap)))
        val choice = lar.Choice("Follow")

        ExerciseCommand(reference, choice, boxedRecord(arg), None, None)
      }

      def followUser(contractId: lar.ContractId, actAs: Party, toFollow: Party) = {
        val exercise = userExerciseFollowCommand(contractId, toFollow)
        val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

        fixture
          .headersWithPartyAuth(actAs = List(actAs))
          .flatMap(headers =>
            fixture.postJsonRequest(Uri.Path("/v1/exercise"), exerciseJson, headers)
          )
          .parseResponse[JsValue]
          .map(inside(_) { case OkResponse(_, _, StatusCodes.OK) =>
          })

      }

      def queryUsers(fromPerspectiveOfParty: Party) = {
        val query = jsObject(s"""{"templateIds": ["$packageId:User:User"]}""")

        suppressPackageIdWarning {
          fixture
            .headersWithPartyAuth(actAs = List(fromPerspectiveOfParty))
            .flatMap(headers => fixture.postJsonRequest(Uri.Path("/v1/query"), query, headers))
            .parseResponse[JsValue]
            .map(inside(_) { case OkResponse(_, _, StatusCodes.OK) =>
            })
        }
      }

      for {
        partyDetails <- partyIds.traverse { p =>
          partyManagement.allocateParty(Some(p.unwrap))
        }
        parties = Party subst partyDetails.map(p => p.party: String)
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

  "query GET" should {
    "empty results" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        fixture.searchAllExpectOk(headers).map { vector =>
          vector should have size 0L
        }
      }
    }

    "single-party with results" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val searchDataSet = genSearchDataSet(alice)
        searchDataSet.traverse(c => postCreateCommand(c, fixture, headers)).flatMap { rs =>
          rs.map(_.status) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

          fixture
            .getRequest(Uri.Path("/v1/query"), headers)
            .parseResponse[Vector[JsValue]]
            .map(inside(_) { case OkResponse(vector, None, StatusCodes.OK) =>
              vector should have size searchDataSet.size.toLong
            }): Future[Assertion]
        }
      }
    }

    "single party with package id" in httpTestFixture { fixture =>
      val pkgId = packageIdOfDar(AbstractHttpServiceIntegrationTestFuns.dar1)
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val searchDataSet = genSearchDataSet(alice)
        searchExpectOk(
          searchDataSet,
          jsObject(s"""{"templateIds": ["$pkgId:Iou:Iou"]}"""),
          fixture,
          headers,
        ).map { (acl: List[ActiveContract.ResolvedCtTyId[JsValue]]) =>
          acl.size shouldBe searchDataSet.size
        }
      }
    }

    "multi-party" in httpTestFixture { fixture =>
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

  "create" should {
    "succeeds with single party, proper argument" in httpTestFixture { fixture =>
      import fixture.encoder
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val command = iouCreateCommand(alice)

        postCreateCommand(command, fixture, headers)
          .map(inside(_) { case OkResponse(activeContract, _, StatusCodes.OK) =>
            assertActiveContract(activeContract)(command, encoder)
          }): Future[Assertion]
      }
    }

    "propagate trace context" in httpTestFixture { fixture =>
      import fixture.encoder
      def generateHex(length: Int): String =
        RandomStringUtils.random(length, "0123456789abcdef").toLowerCase
      val randomTraceId = generateHex(32)

      val testContextHeaders =
        extractHeaders(W3CTraceContext(s"00-$randomTraceId-93bb0fa23a8fb53a-01"))

      fixture.getUniquePartyTokenUserIdAndAuthHeaders("Alice").flatMap {
        case (alice, jxt, userId, headers) =>
          val command = iouCreateCommand(alice)
          postCreateCommand(command, fixture, headers ++ testContextHeaders)
            .map(inside(_) { case OkResponse(activeContract, _, StatusCodes.OK) =>
              assertActiveContract(activeContract)(command, encoder)
            })
            .flatMap { _ =>
              fixture.client.updateService
                .getUpdatesSource(
                  begin = 0L,
                  eventFormat = EventFormat(
                    filtersByParty = Map(
                      alice.unwrap -> Filters(
                        Seq(
                          CumulativeFilter.defaultInstance
                            .withWildcardFilter(WildcardFilter(includeCreatedEventBlob = false))
                        )
                      )
                    ),
                    filtersForAnyParty = None,
                    verbose = false,
                  ),
                  end = None,
                  token = Some(jxt.value),
                )
                .collect { response =>
                  response.update match {
                    case Update.Transaction(tx) =>
                      SerializableTraceContextConverter
                        .fromDamlProtoSafeOpt(loggerWithoutTracing(logger))(tx.traceContext)
                        .traceContext
                        .traceId

                  }
                }
                .take(1)
                .runWith(Sink.seq)
                .map(tcs => tcs should be(Seq(Some(randomTraceId))))
            }
      }
    }

    "fails if authorization header is missing" taggedAs authorizationSecurity.setAttack(
      Attack(
        "Ledger client",
        "calls /create without authorization",
        "refuse request with UNAUTHORIZED",
      )
    ) in httpTestFixture { fixture =>
      import fixture.encoder
      val alice = getUniqueParty("Alice")
      val command = iouCreateCommand(alice)
      val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

      fixture
        .postJsonRequest(Uri.Path("/v1/create"), input, List())
        .parseResponse[JsValue]
        .map(inside(_) { case ErrorResponse(Seq(error), _, StatusCodes.Unauthorized, _) =>
          error should include(
            "missing Authorization header with OAuth 2.0 Bearer Token"
          )
        }): Future[Assertion]
    }

    "supports extra readAs parties" in httpTestFixture { fixture =>
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
          .parseResponse[ActiveContract.ResolvedCtTyId[JsValue]]
      } yield inside(activeContractResponse) { case OkResponse(activeContract, _, StatusCodes.OK) =>
        assertActiveContract(activeContract)(command, encoder)
      }
    }

    "with unsupported templateId should return proper error" in httpTestFixture { fixture =>
      import fixture.encoder
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val command: CreateCommand[v.Record, ContractTypeId.Template.RequiredPkg] =
          iouCreateCommand(alice)
            .copy(templateId = TpId.Iou.Dummy)
        val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

        fixture
          .postJsonRequest(Uri.Path("/v1/create"), input, headers)
          .parseResponse[JsValue]
          .map(inside(_) { case ErrorResponse(Seq(error), _, StatusCodes.BadRequest, _) =>
            val unknownTemplateId: ContractTypeId.Template.RequiredPkg = command.templateId
            error should include(
              s"Cannot resolve template ID, given: $unknownTemplateId"
            )
          }): Future[Assertion]
      }
    }

    "supports command deduplication" in httpTestFixture { fixture =>
      import fixture.encoder
      def genSubmissionId() = SubmissionId(UUID.randomUUID().toString)

      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val cmdId = CommandId apply UUID.randomUUID().toString

        def cmd(submissionId: SubmissionId) =
          iouCreateCommand(
            alice,
            amount = "19002.0",
            meta = Some(
              CommandMeta(
                commandId = Some(cmdId),
                actAs = None,
                readAs = None,
                submissionId = Some(submissionId),
                workflowId = None,
                deduplicationPeriod = Some(DeduplicationPeriod.Duration(10000L)),
                disclosedContracts = None,
                synchronizerId = None,
                packageIdSelectionPreference = None,
              )
            ),
          )

        val firstCreate: JsValue =
          encoder.encodeCreateCommand(cmd(genSubmissionId())).valueOr(e => fail(e.shows))

        fixture
          .postJsonRequest(Uri.Path("/v1/create"), firstCreate, headers)
          .parseResponse[CreateCommandResponse[JsValue]]
          .map(inside(_) { case OkResponse(result, _, _) =>
            result.completionOffset.unwrap should not be empty
          })
          .flatMap { _ =>
            val secondCreate: JsValue =
              encoder.encodeCreateCommand(cmd(genSubmissionId())).valueOr(e => fail(e.shows))
            fixture
              .postJsonRequest(Uri.Path("/v1/create"), secondCreate, headers)
              .map(inside(_) { case (StatusCodes.Conflict, _) => succeed }): Future[Assertion]
          }
      }
    }

    "supports workflow id" in httpTestFixture { fixture =>
      import fixture.encoder

      fixture.getUniquePartyTokenUserIdAndAuthHeaders("Alice").flatMap {
        case (alice, jwt, _, headers) =>
          val workflowId = WorkflowId("foobar")

          val cmd =
            iouCreateCommand(
              alice,
              amount = "19002.0",
              meta = Some(
                CommandMeta(
                  commandId = None,
                  actAs = None,
                  readAs = None,
                  submissionId = None,
                  workflowId = Some(workflowId),
                  deduplicationPeriod = None,
                  disclosedContracts = None,
                  synchronizerId = None,
                  packageIdSelectionPreference = None,
                )
              ),
            )

          val create: JsValue =
            encoder.encodeCreateCommand(cmd).valueOr(e => fail(e.shows))

          for {
            before <- fixture.client.stateService.getLedgerEndOffset()
            result <- fixture
              .postJsonRequest(Uri.Path("/v1/create"), create, headers)
              .parseResponse[CreateCommandResponse[JsValue]]
              .map(inside(_) { case OkResponse(result, _, _) => result })
            update <- fixture.client.updateService
              .getUpdatesSource(
                begin = before,
                eventFormat = EventFormat(
                  filtersByParty = Map(
                    alice.unwrap -> Filters(
                      Seq(
                        CumulativeFilter.defaultInstance
                          .withWildcardFilter(WildcardFilter(includeCreatedEventBlob = false))
                      )
                    )
                  ),
                  filtersForAnyParty = None,
                  verbose = false,
                ),
                end = Some(Offset.assertFromStringToLong(result.completionOffset.unwrap)),
                token = Some(jwt.value),
              )
              .runWith(Sink.last)
          } yield update.getTransaction.workflowId shouldBe workflowId
      }
    }
  }

  "create-and-exercise IOU_Transfer" in httpTestFixture { fixture =>
    import fixture.encoder
    for {
      (alice, headers) <- fixture.getUniquePartyAndAuthHeaders("Alice")
      (bob, _) <- fixture.getUniquePartyAndAuthHeaders("Bob")
      cmd = iouCreateAndExerciseTransferCommand(alice, bob)
      json: JsValue = encoder.encodeCreateAndExerciseCommand(cmd).valueOr(e => fail(e.shows))

      res <- fixture
        .postJsonRequest(Uri.Path("/v1/create-and-exercise"), json, headers)
        .parseResponse[ExerciseResponse[JsValue]]
      _ = inside(res) { case OkResponse(result, None, StatusCodes.OK) =>
        result.completionOffset.unwrap should not be empty
        inside(result.events) {
          case List(
                Contract(\/-(created0)),
                Contract(-\/(archived0)),
                Contract(\/-(created1)),
              ) =>
            ContractTypeId.withPkgRef(created0.templateId) shouldBe cmd.templateId
            ContractTypeId.withPkgRef(archived0.templateId) shouldBe cmd.templateId
            archived0.contractId shouldBe created0.contractId
            ContractTypeId.withPkgRef(created1.templateId) shouldBe TpId.Iou.IouTransfer
            asContractId(result.exerciseResult) shouldBe created1.contractId
        }
      }
    } yield succeed
  }

  "request non-existent endpoint should return 404 with errors" in httpTestFixture { fixture =>
    val badPath = Uri.Path("/contracts/does-not-exist")
    val badUri = fixture.uri withPath badPath
    fixture
      .getRequestWithMinimumAuth[JsValue](badPath)
      .map(inside(_) { case ErrorResponse(Seq(errorMsg), _, StatusCodes.NotFound, _) =>
        errorMsg shouldBe s"${HttpMethods.GET: HttpMethod}, uri: ${badUri: Uri}"
      }): Future[Assertion]
  }

  "parties endpoint should" should {
    "return all known parties" in httpTestFixture { fixture =>
      import fixture.client
      val partyIds = Vector("P1", "P2", "P3", "P4")
      val partyManagement = client.partyManagementClient

      partyIds
        .traverse { p =>
          partyManagement.allocateParty(Some(p))
        }
        .flatMap { allocatedParties =>
          fixture
            .getRequest(
              Uri.Path("/v1/parties"),
              headers = headersWithAdminAuth,
            )
            .parseResponse[List[HttpPartyDetails]]
            .map(inside(_) { case OkResponse(result, None, StatusCodes.OK) =>
              val actualIds: Set[Party] = result.view.map(_.identifier).toSet
              val allocatedIds: Set[Party] =
                Party.subst(allocatedParties.map(p => p.party: String)).toSet
              actualIds should contain allElementsOf allocatedIds
              result.toSet should contain allElementsOf
                allocatedParties.toSet.map(HttpPartyDetails.fromLedgerApi)
              result.size should be > maxPartiesPageSize.value
            })
        }: Future[Assertion]
    }

    "return only requested parties, unknown parties returned as warnings" in httpTestFixture {
      fixture =>
        import fixture.client
        val List(aliceName, bobName, charlieName, erinName) =
          List("Alice", "Bob", "Charlie", "Erin").map(getUniqueParty)
        // We do not allocate erin
        val namesToAllocate = List(aliceName, bobName, charlieName)
        val partyManagement = client.partyManagementClient

        namesToAllocate
          .traverse { p =>
            partyManagement.allocateParty(Some(p.unwrap))
          }
          .flatMap { allocatedParties =>
            val allocatedPartiesHttpApi: List[HttpPartyDetails] =
              allocatedParties.map(HttpPartyDetails.fromLedgerApi)
            // Get alice, bob and charlies real party names
            val List(alice, bob, charlie) = allocatedPartiesHttpApi.map(_.identifier)
            fixture
              .postJsonRequest(
                Uri.Path("/v1/parties"),
                // Request alice and bob as normal, erin by name (as unallocated, she has no hash)
                JsArray(Vector(alice, bob, erinName).map(x => JsString(x.unwrap))),
                headersWithAdminAuth,
              )
              .parseResponse[List[HttpPartyDetails]]
              .map(inside(_) { case OkResponse(result, Some(warnings), StatusCodes.OK) =>
                warnings shouldBe UnknownParties(List(erinName))
                val actualIds: Set[Party] = result.view.map(_.identifier).toSet
                actualIds shouldBe Set(alice, bob) // Erin is not known
                val expected: Set[HttpPartyDetails] = allocatedPartiesHttpApi.toSet
                  .filterNot(_.identifier == charlie)
                result.toSet shouldBe expected
              })
          }: Future[Assertion]
    }

    "error if empty array passed as input" in httpTestFixture { fixture =>
      fixture
        .postJsonRequestWithMinimumAuth[JsValue](
          Uri.Path("/v1/parties"),
          JsArray(Vector.empty),
        )
        .map(inside(_) { case ErrorResponse(Seq(errorMsg), None, StatusCodes.BadRequest, _) =>
          errorMsg should include("Cannot read JSON: <[]>")
          errorMsg should include("must be a JSON array with at least 1 element")
        }): Future[Assertion]
    }

    "error if empty party string passed" in httpTestFixture { fixture =>
      val requestedPartyIds: Vector[Party] = Party.subst(Vector(""))

      fixture
        .postJsonRequestWithMinimumAuth[List[HttpPartyDetails]](
          Uri.Path("/v1/parties"),
          JsArray(requestedPartyIds.map(x => JsString(x.unwrap))),
        )
        .map(inside(_) { case ErrorResponse(List(error), None, StatusCodes.BadRequest, _) =>
          error should include("Daml-LF Party is empty")
        }): Future[Assertion]
    }

    "return empty result with warnings and OK status if nothing found" in httpTestFixture {
      fixture =>
        val requestedPartyIds: Vector[Party] =
          Vector(getUniqueParty("Alice"), getUniqueParty("Bob"))

        fixture
          .postJsonRequest(
            Uri.Path("/v1/parties"),
            JsArray(requestedPartyIds.map(x => JsString(x.unwrap))),
            headers = headersWithAdminAuth,
          )
          .parseResponse[List[HttpPartyDetails]]
          .map(inside(_) {
            case OkResponse(
                  List(),
                  Some(UnknownParties(unknownParties)),
                  StatusCodes.OK,
                ) =>
              unknownParties.toSet shouldBe requestedPartyIds.toSet
          }): Future[Assertion]
    }
  }

  "parties/allocate should" should {
    "allocate a new party" in httpTestFixture { fixture =>
      val request = HttpAllocatePartyRequest(
        Some(Party(s"Carol${uniqueId()}")),
        None,
      )
      val json = SprayJson.encode(request).valueOr(e => fail(e.shows))
      fixture
        .postJsonRequest(
          Uri.Path("/v1/parties/allocate"),
          json = json,
          headers = headersWithAdminAuth,
        )
        .parseResponse[HttpPartyDetails]
        .flatMap(inside(_) { case OkResponse(newParty, _, StatusCodes.OK) =>
          newParty.identifier.toString should startWith(request.identifierHint.value.toString)
          newParty.isLocal shouldBe true
          fixture
            .getRequest(
              Uri.Path("/v1/parties"),
              headersWithAdminAuth,
            )
            .parseResponse[List[HttpPartyDetails]]
            .map(inside(_) { case OkResponse(result, _, StatusCodes.OK) =>
              result should contain(newParty)
            })
        }): Future[Assertion]
    }

    "allocate a new party without any hints" in httpTestFixture { fixture =>
      fixture
        .postJsonRequest(
          Uri.Path("/v1/parties/allocate"),
          json = JsObject(),
          headers = headersWithAdminAuth,
        )
        .parseResponse[HttpPartyDetails]
        .flatMap(inside(_) { case OkResponse(newParty, _, StatusCodes.OK) =>
          newParty.identifier.unwrap.length should be > 0
          newParty.isLocal shouldBe true

          fixture
            .getRequest(
              Uri.Path("/v1/parties"),
              headers = headersWithAdminAuth,
            )
            .parseResponse[List[HttpPartyDetails]]
            .map(inside(_) { case OkResponse(result, _, StatusCodes.OK) =>
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
      ) in httpTestFixture { fixture =>
      val request = HttpAllocatePartyRequest(
        Some(Party(s"Carol-!")),
        None,
      )
      val json = SprayJson.encode(request).valueOr(e => fail(e.shows))

      fixture
        .postJsonRequest(
          Uri.Path("/v1/parties/allocate"),
          json = json,
          headers = headersWithAdminAuth,
        )
        .parseResponse[JsValue]
        .map(inside(_) { case ErrorResponse(errors, None, StatusCodes.BadRequest, _) =>
          errors.length shouldBe 1
        })
    }
  }

  "packages endpoint should" should {
    "return all known package IDs" in httpTestFixture { fixture =>
      getAllPackageIds(fixture).map { x =>
        inside(x) {
          case OkResponse(ps, None, StatusCodes.OK) if ps.nonEmpty =>
            Inspectors.forAll(ps)(_.length should be > 0)
        }
      }: Future[Assertion]
    }
  }

  "packages/packageId should" should {
    "return a requested package" in httpTestFixture { fixture =>
      import AbstractHttpServiceIntegrationTestFuns.sha256
      import fixture.uri
      getAllPackageIds(fixture).flatMap { okResp =>
        inside(okResp.result.headOption) { case Some(packageId) =>
          singleRequest(
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

    "return NotFound if a non-existing package is requested" in httpTestFixture { fixture =>
      singleRequest(
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

  "packages upload endpoint" in httpTestFixture { fixture =>
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

  "package list is updated when a query request is made" in usingLedger() { (jsonApiPort, client) =>
    withHttpServiceOnly(jsonApiPort, client) { fixture =>
      for {
        alicePartyAndAuthHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, headers) = alicePartyAndAuthHeaders
        _ = withHttpServiceOnly(jsonApiPort, client) { innerFixture =>
          val searchDataSet = genSearchDataSet(alice)
          searchDataSet.traverse(c => postCreateCommand(c, innerFixture, headers)).map { rs =>
            rs.map(_.status) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)
          }
        }
        _ = withHttpServiceOnly(jsonApiPort, client) { innerFixture =>
          innerFixture
            .getRequest(Uri.Path("/v1/query"), headers)
            .parseResponse[Vector[JsValue]]
            .map(inside(_) { case OkResponse(result, _, StatusCodes.OK) =>
              result should have length 4
            }): Future[Assertion]
        }
      } yield succeed
    }
  }
}
