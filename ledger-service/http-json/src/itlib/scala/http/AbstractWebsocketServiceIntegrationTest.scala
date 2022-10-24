// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{
  Message,
  PeerClosedConnectionException,
  TextMessage,
  WebSocketRequest,
}
import akka.http.scaladsl.model.{HttpHeader, StatusCodes, Uri}
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.daml.http.HttpServiceTestFixture.{
  UseTls,
  accountCreateCommand,
  sharedAccountCreateCommand,
}
import AbstractHttpServiceIntegrationTestFuns.UriFixture
import com.daml.http.json.SprayJson
import com.daml.ledger.api.v1.admin.{participant_pruning_service => PruneGrpc}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.scalaFuture._
import scalaz.std.vector._
import scalaz.syntax.std.option._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.{-\/, \/-}
import spray.json.{
  DeserializationException,
  JsArray,
  JsNull,
  JsObject,
  JsString,
  JsValue,
  enrichAny => `sj enrichAny`,
}
import com.daml.fetchcontracts.domain.ResolvedQuery

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class AbstractWebsocketServiceIntegrationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with StrictLogging
    with AbstractHttpServiceIntegrationTestFuns
    with BeforeAndAfterAll {

  import WebsocketTestFixture._

  override def staticContentConfig: Option[StaticContentConfig] = None

  def useTls = UseTls.NoTls

  override def wsConfig: Option[WebsocketConfig] = Some(WebsocketConfig())

  private val baseQueryInput: Source[Message, NotUsed] =
    Source.single(TextMessage.Strict("""{"templateIds": ["Account:Account"]}"""))

  private val fetchRequest =
    """[{"templateId": "Account:Account", "key": ["Alice", "abc123"]}]"""

  private val baseFetchInput: Source[Message, NotUsed] =
    Source.single(TextMessage.Strict(fetchRequest))

  private def heartbeatOffset(event: JsValue) = event match {
    case ContractDelta(Vector(), Vector(), Some(offset)) => offset
    case _ => throw new IllegalArgumentException(s"Expected heartbeat but got $event")
  }

  private def immediateQuery(fixture: UriFixture, scenario: SimpleScenario): Future[Seq[String]] =
    for {
      jwt <- jwt(fixture.uri)
      webSocketFlow =
        Http().webSocketClientFlow(
          WebSocketRequest(
            uri = fixture.uri.copy(scheme = "ws").withPath(scenario.path),
            subprotocol = validSubprotocol(jwt),
          )
        )
      ran <- scenario.input via webSocketFlow runWith collectResultsAsTextMessageSkipOffsetTicks
    } yield ran

  List(
    SimpleScenario("query", Uri.Path("/v1/stream/query"), baseQueryInput),
    SimpleScenario("fetch", Uri.Path("/v1/stream/fetch"), baseFetchInput),
  ).foreach { scenario =>
    // TEST_EVIDENCE: Authorization: websocket request with valid protocol token should allow client subscribe to stream
    s"${scenario.id} request with valid protocol token should allow client subscribe to stream" in withHttpService {
      (uri, _, _, _) =>
        jwt(uri).flatMap(jwt =>
          wsConnectRequest(
            uri.copy(scheme = "ws").withPath(scenario.path),
            validSubprotocol(jwt),
            scenario.input,
          )._1 flatMap (x => x.response.status shouldBe StatusCodes.SwitchingProtocols)
        )
    }

    // TEST_EVIDENCE: Authorization: websocket request with invalid protocol token should be denied
    s"${scenario.id} request with invalid protocol token should be denied" in withHttpService {
      (uri, _, _, _) =>
        wsConnectRequest(
          uri.copy(scheme = "ws").withPath(scenario.path),
          Option("foo"),
          scenario.input,
        )._1 flatMap (x => x.response.status shouldBe StatusCodes.Unauthorized)
    }

    // TEST_EVIDENCE: Authorization: websocket request without protocol token should be denied
    s"${scenario.id} request without protocol token should be denied" in withHttpService {
      (uri, _, _, _) =>
        wsConnectRequest(
          uri.copy(scheme = "ws").withPath(scenario.path),
          None,
          scenario.input,
        )._1 flatMap (x => x.response.status shouldBe StatusCodes.Unauthorized)
    }

    // TEST_EVIDENCE: Authorization: multiple websocket requests over the same WebSocket connection are NOT allowed
    s"two ${scenario.id} requests over the same WebSocket connection are NOT allowed" in withHttpService {
      fixture =>
        immediateQuery(fixture, scenario.mapInput(_.mapConcat(x => List(x, x))))
          .flatMap { msgs =>
            inside(msgs) { case Seq(errorMsg) =>
              val error = decodeErrorResponse(errorMsg)
              error shouldBe domain.ErrorResponse(
                List("Multiple requests over the same WebSocket connection are not allowed."),
                None,
                StatusCodes.BadRequest,
              )
            }
          }
    }
  }

  List(
    SimpleScenario(
      "query",
      Uri.Path("/v1/stream/query"),
      Source.single(TextMessage.Strict("""{"templateIds": ["AA:BB"]}""")),
    ),
    SimpleScenario(
      "fetch",
      Uri.Path("/v1/stream/fetch"),
      Source.single(TextMessage.Strict("""[{"templateId": "AA:BB", "key": ["k", "v"]}]""")),
    ),
  ).foreach { scenario =>
    s"${scenario.id} report UnknownTemplateIds and error when cannot resolve any template ID" in withHttpService {
      fixture =>
        immediateQuery(fixture, scenario)
          .flatMap { msgs =>
            inside(msgs) { case Seq(warningMsg, errorMsg) =>
              val warning = decodeServiceWarning(warningMsg)
              inside(warning) { case domain.UnknownTemplateIds(ids) =>
                ids shouldBe List(domain.ContractTypeId(None, "AA", "BB"))
              }
              val error = decodeErrorResponse(errorMsg)
              error shouldBe domain.ErrorResponse(
                List(ErrorMessages.cannotResolveAnyTemplateId),
                None,
                StatusCodes.BadRequest,
              )
            }
          }
    }
  }

  "query error when queries with more than 1 interface id" in withHttpService { fixture =>
    import AbstractHttpServiceIntegrationTestFuns.ciouDar
    val queryInput = Source.single(
      TextMessage.Strict(
        """[{"templateIds": ["IAccount:IAccount"]}, {"templateIds": ["IIou:IIou"]}]"""
      )
    )
    val scenario = SimpleScenario("", Uri.Path("/v1/stream/query"), queryInput)
    for {
      _ <- uploadPackage(fixture)(ciouDar)
      msgs <- immediateQuery(fixture, scenario)
    } yield inside(msgs) { case Seq(errorMsg) =>
      val error = decodeErrorResponse(errorMsg)
      error shouldBe domain.ErrorResponse(
        List(ResolvedQuery.CannotQueryManyInterfaceIds.errorMsg),
        None,
        StatusCodes.BadRequest,
      )
    }
  }

  "query error when queries with both template and interface id" in withHttpService { fixture =>
    val queryInput = Source.single(
      TextMessage.Strict(
        """[{"templateIds": ["IAccount:IAccount"]}, {"templateIds": ["Account:Account"]}]"""
      )
    )
    val scenario = SimpleScenario("", Uri.Path("/v1/stream/query"), queryInput)
    for {
      msgs <- immediateQuery(fixture, scenario)
    } yield inside(msgs) { case Seq(errorMsg) =>
      val error = decodeErrorResponse(errorMsg)
      error shouldBe domain.ErrorResponse(
        List(ResolvedQuery.CannotQueryBothTemplateIdsAndInterfaceIds.errorMsg),
        None,
        StatusCodes.BadRequest,
      )
    }
  }

  "transactions when command create is completed from" - {
    "query endpoint" in withHttpService { fixture =>
      import fixture.uri
      for {
        aliceHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, headers) = aliceHeaders
        _ <- initialIouCreate(uri, alice, headers)
        jwt <- jwtForParties(uri)(List(alice.unwrap), List(), testId)
        clientMsg <- singleClientQueryStream(
          jwt,
          uri,
          """{"templateIds": ["Iou:Iou"]}""",
        ).take(2)
          .runWith(collectResultsAsTextMessage)
      } yield inside(clientMsg) { case result +: heartbeats =>
        result should include(s""""issuer":"$alice"""")
        result should include(""""amount":"999.99"""")
        Inspectors.forAll(heartbeats)(assertHeartbeat)
      }
    }

    "fetch endpoint" in withHttpService { fixture =>
      import fixture.uri
      for {
        aliceHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, headers) = aliceHeaders
        _ <- initialAccountCreate(fixture, alice, headers)
        jwt <- jwtForParties(uri)(List(alice.unwrap), Nil, testId)
        fetchRequest = s"""[{"templateId": "Account:Account", "key": ["$alice", "abc123"]}]"""
        clientMsg <- singleClientFetchStream(jwt, uri, fetchRequest)
          .take(2)
          .runWith(collectResultsAsTextMessage)
      } yield inside(clientMsg) { case result +: heartbeats =>
        result should include(s""""owner":"$alice"""")
        result should include(""""number":"abc123"""")
        result should not include (""""offset":"""")
        Inspectors.forAll(heartbeats)(assertHeartbeat)
      }
    }
  }

  "interface sub" - {
    "query endpoint" in withHttpService { fixture =>
      import fixture.uri
      val query =
        """[
        {"templateIds": ["IAccount:IAccount"], "query": {"isAbcPrefix": true}},
        {"templateIds": ["IAccount:IAccount"], "query": {"is123Suffix": true}}
      ]"""

      @nowarn("msg=pattern var evtsWrapper .* is never used")
      def resp(
          owner: domain.Party,
          headers: List[HttpHeader],
          kill: UniqueKillSwitch,
      ): Sink[JsValue, Future[ShouldHaveEnded]] = {
        def createAccount(
            owner: domain.Party,
            amount: String,
            headers: List[HttpHeader],
        ) = postCreateCommand(
          accountCreateCommand(owner, amount),
          fixture,
          headers = headers,
        )

        def exerciseTransferPayload(cid: domain.ContractId) = {
          import json.JsonProtocol._
          val ecid: domain.ContractLocator[JsValue] =
            domain.EnrichedContractId(Some(TpId.IAccount.IAccount), cid)
          domain
            .ExerciseCommand(
              ecid,
              choice = domain.Choice("ChangeAmount"),
              argument = Map("newAmount" -> "abcxx").toJson,
              None,
              None,
            )
            .toJson
        }

        val dslSyntax = Consume.syntax[JsValue]
        import dslSyntax._

        def readAndExtract(
            record: AccountRecord,
            mq: Vector[Int],
        ): Consume.FCC[JsValue, CreatedAccountEvent] = for {
          _ <- liftF(createAccount(owner, record.amount, headers))
          AccountQuery(event) <- readOne
        } yield {
          event.created.record should ===(record)
          event.created.templateId.copy(packageId = None) should ===(TpId.IAccount.IAccount)
          event.matchedQueries should ===(mq)
          event
        }

        Consume.interpret(
          for {
            ContractDelta(Vector(), _, Some(offset)) <- readOne
            Seq(createdAccountEvent1, _, _) <-
              List(
                (AccountRecord("abc123", true, true), Vector(0, 1)),
                (AccountRecord("abc456", true, false), Vector(0)),
                (AccountRecord("def123", false, true), Vector(1)),
              ).traverse((readAndExtract _).tupled)

            _ <- liftF(createAccount(owner, "def456", headers))

            _ <- liftF(
              fixture.postJsonRequest(
                Uri.Path("/v1/exercise"),
                exerciseTransferPayload(createdAccountEvent1.created.contractId),
                headers,
              ) map { case (statusCode, _) =>
                statusCode.isSuccess shouldBe true
              }
            )

            evtsWrapper @ ContractDelta(
              Vector(_),
              Vector(observeConsumed),
              Some(lastSeenOffset),
            ) <- readOne
            liveStartOffset = {
              observeConsumed.contractId should ===(createdAccountEvent1.created.contractId)
              inside(evtsWrapper) { case JsObject(obj) =>
                inside(obj get "events") {
                  case Some(
                        JsArray(
                          Vector(
                            Archived(
                              ContractIdField(
                                JsString(archivedContractId),
                                TemplateIdField(ContractTypeId(archivedTemplateId), _),
                              ),
                              _,
                            ),
                            Created(
                              CreatedAccount(
                                CreatedAccountContract(_, createdTemplateId, createdRecord)
                              ),
                              MatchedQueries(NumList(matchedQueries), _),
                            ),
                          )
                        )
                      ) =>
                    archivedContractId should ===(createdAccountEvent1.created.contractId)
                    archivedTemplateId.copy(packageId = None) should ===(TpId.IAccount.IAccount)

                    createdTemplateId.copy(packageId = None) should ===(TpId.IAccount.IAccount)

                    createdRecord should ===(AccountRecord("abcxx", true, false))
                    matchedQueries shouldBe Vector(0)
                }
              }
              offset
            }
            _ = kill.shutdown()
            heartbeats <- drain
            hbCount = (heartbeats.iterator.map(heartbeatOffset).toSet + lastSeenOffset).size - 1
          } yield ShouldHaveEnded(
            liveStartOffset = liveStartOffset,
            msgCount = 2 + hbCount,
            lastSeenOffset = lastSeenOffset,
          )
        )
      }

      for {
        (alice, aliceAuthHeaders) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        jwt <- jwtForParties(uri)(List(alice.unwrap), List(), testId)
        (kill, source) = singleClientQueryStream(
          jwt,
          uri,
          query,
        ).viaMat(KillSwitches.single)(Keep.right)
          .preMaterialize()
        ShouldHaveEnded(_, msgCount, _) <- source via parseResp runWith resp(
          alice,
          aliceAuthHeaders,
          kill,
        )
      } yield {
        msgCount should ===(2)
      }
    }
  }

  "warn on unknown template IDs from" - {
    "query endpoint" in withHttpService { fixture =>
      import fixture.uri
      for {
        aliceHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, headers) = aliceHeaders
        _ <- initialIouCreate(uri, alice, headers)

        clientMsg <- jwtForParties(uri)(List(alice.unwrap), List(), testId)
          .flatMap(
            singleClientQueryStream(
              _,
              uri,
              """{"templateIds": ["Iou:Iou", "Unknown:Template"]}""",
            )
              .take(3)
              .runWith(collectResultsAsTextMessage)
          )
      } yield inside(clientMsg) { case warning +: result +: heartbeats =>
        warning should include("\"warnings\":{\"unknownTemplateIds\":[\"Unk")
        result should include(s""""issuer":"$alice"""")
        Inspectors.forAll(heartbeats)(assertHeartbeat)
      }
    }

    "fetch endpoint" in withHttpService { fixture =>
      import fixture.uri
      for {
        aliceHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, headers) = aliceHeaders
        _ <- initialAccountCreate(fixture, alice, headers)

        clientMsg <- jwtForParties(uri)(List(alice.unwrap), List(), testId).flatMap(
          singleClientFetchStream(
            _,
            uri,
            s"""[{"templateId": "Account:Account", "key": ["$alice", "abc123"]}, {"templateId": "Unknown:Template", "key": ["$alice", "abc123"]}]""",
          ).take(3)
            .runWith(collectResultsAsTextMessage)
        )
      } yield inside(clientMsg) { case warning +: result +: heartbeats =>
        warning should include("""{"warnings":{"unknownTemplateIds":["Unk""")
        result should include(s""""owner":"$alice"""")
        result should include(""""number":"abc123"""")
        Inspectors.forAll(heartbeats)(assertHeartbeat)
      }
    }
  }

  "error msg when receiving malformed message," - {
    "query endpoint" in withHttpService { (uri, _, _, _) =>
      val clientMsg = jwt(uri).flatMap(
        singleClientQueryStream(_, uri, "{}")
          .runWith(collectResultsAsTextMessageSkipOffsetTicks)
      )

      val result = Await.result(clientMsg, 10.seconds)

      result should have size 1
      val errorResponse = decodeErrorResponse(result.head)
      errorResponse.status shouldBe StatusCodes.BadRequest
      errorResponse.errors should have size 1
    }

    "fetch endpoint" in withHttpService { fixture =>
      import fixture.uri
      val clientMsg = jwt(uri)
        .flatMap(
          singleClientFetchStream(_, uri, """[abcdefg!]""")
            .runWith(collectResultsAsTextMessageSkipOffsetTicks)
        )

      val result = Await.result(clientMsg, 10.seconds)

      result should have size 1
      val errorResponse = decodeErrorResponse(result.head)
      errorResponse.status shouldBe StatusCodes.BadRequest
      errorResponse.errors should have size 1
    }
  }

  private def exercisePayload(cid: domain.ContractId, amount: BigDecimal = BigDecimal("42.42")) = {
    import json.JsonProtocol._
    domain
      .ExerciseCommand(
        domain.EnrichedContractId(Some(TpId.Iou.Iou), cid): domain.ContractLocator[JsValue],
        domain.Choice("Iou_Split"),
        Map("splitAmount" -> amount).toJson,
        None,
        None,
      )
      .toJson
  }

  "matchedQueries should be correct for multiqueries with per-query offsets" in withHttpService {
    fixture =>
      import fixture.uri
      @nowarn("msg=pattern var evtsWrapper .* is never used")
      def resp(
          iouCid: domain.ContractId,
          kill: UniqueKillSwitch,
      ): Sink[JsValue, Future[domain.Offset]] = {
        val dslSyntax = Consume.syntax[JsValue]
        import dslSyntax._
        Consume
          .interpret(
            for {
              evtsWrapper @ ContractDelta(Vector((ctid, _)), Vector(), None) <- readOne
              _ = {
                (ctid: domain.ContractId) shouldBe iouCid
                inside(evtsWrapper) { case JsObject(obj) =>
                  inside(obj get "events") {
                    case Some(
                          JsArray(
                            Vector(
                              Created(IouAmount(amt), MatchedQueries(NumList(ix), _))
                            )
                          )
                        ) =>
                      // matchedQuery should be 0 for the initial query supplied
                      Set((amt, ix)) should ===(Set((BigDecimal("999.99"), Vector(BigDecimal(0)))))
                  }
                }
              }
              ContractDelta(Vector(), _, Some(offset)) <- readOne

              _ = kill.shutdown()
              _ <- drain

            } yield offset
          )
      }

      // initial query without offset
      val query =
        """[
          {"templateIds": ["Iou:Iou"], "query": {"currency": "USD"}}
        ]"""

      for {
        aliceHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (party, headers) = aliceHeaders
        creation <- initialIouCreate(uri, party, headers)
        iouCid = resultContractId(creation)
        jwt <- jwtForParties(uri)(List(party.unwrap), List(), testId)
        (kill, source) = singleClientQueryStream(jwt, uri, query)
          .viaMat(KillSwitches.single)(Keep.right)
          .preMaterialize()
        lastSeen <- source via parseResp runWith resp(iouCid, kill)

        // construct a new multiquery with one of them having an offset while the other doesn't
        multiquery = s"""[
          {"templateIds": ["Iou:Iou"], "query": {"currency": "USD"}, "offset": "${lastSeen.unwrap}"},
          {"templateIds": ["Iou:Iou"]}
        ]"""

        clientMsg <- singleClientQueryStream(jwt, uri, multiquery)
          .take(1)
          .runWith(collectResultsAsTextMessageSkipOffsetTicks)
      } yield inside(clientMsg) { case Vector(result) =>
        // we should expect to have matchedQueries [1] to indicate a match for the new template query only.
        result should include(s"""$iouCid""")
        result should include(""""matchedQueries":[1]""")
      }
  }

  "deltas as contracts are archived/created from" - {
    "single-party query" in withHttpService { fixture =>
      import fixture.uri
      val getAliceHeaders = fixture.getUniquePartyAndAuthHeaders("Alice")

      @nowarn("msg=pattern var evtsWrapper .* is never used")
      def resp(
          iouCid: domain.ContractId,
          kill: UniqueKillSwitch,
      ): Sink[JsValue, Future[ShouldHaveEnded]] = {
        val dslSyntax = Consume.syntax[JsValue]
        import dslSyntax._
        Consume
          .interpret(
            for {
              ContractDelta(Vector((ctid, _)), Vector(), None) <- readOne
              _ = (ctid: domain.ContractId) shouldBe iouCid
              _ <- liftF(
                getAliceHeaders.flatMap { case (_, headers) =>
                  fixture.postJsonRequest(
                    Uri.Path("/v1/exercise"),
                    exercisePayload(ctid),
                    headers,
                  ) map { case (statusCode, _) =>
                    statusCode.isSuccess shouldBe true
                  }
                }
              )

              ContractDelta(Vector(), _, Some(offset)) <- readOne

              (preOffset, consumedCtid) = (offset, ctid)
              evtsWrapper @ ContractDelta(
                Vector((fstId, fst), (sndId, snd)),
                Vector(observeConsumed),
                Some(lastSeenOffset),
              ) <- readOne
              (liveStartOffset, msgCount) = {
                observeConsumed.contractId should ===(consumedCtid)
                Set(fstId, sndId, consumedCtid) should have size 3
                inside(evtsWrapper) { case JsObject(obj) =>
                  inside(obj get "events") {
                    case Some(
                          JsArray(
                            Vector(
                              Archived(_, _),
                              Created(IouAmount(amt1), MatchedQueries(NumList(ixes1), _)),
                              Created(IouAmount(amt2), MatchedQueries(NumList(ixes2), _)),
                            )
                          )
                        ) =>
                      Set((amt1, ixes1), (amt2, ixes2)) should ===(
                        Set(
                          (BigDecimal("42.42"), Vector(BigDecimal(0), BigDecimal(2))),
                          (BigDecimal("957.57"), Vector(BigDecimal(1), BigDecimal(2))),
                        )
                      )
                  }
                }
                (preOffset, 2)
              }

              _ = kill.shutdown()
              heartbeats <- drain
              hbCount = (heartbeats.iterator.map(heartbeatOffset).toSet + lastSeenOffset).size - 1
            } yield
            // don't count empty events block if lastSeenOffset does not change
            ShouldHaveEnded(
              liveStartOffset = liveStartOffset,
              msgCount = msgCount + hbCount,
              lastSeenOffset = lastSeenOffset,
            )
          )
      }

      val query =
        """[
          {"templateIds": ["Iou:Iou"], "query": {"amount": {"%lte": 50}}},
          {"templateIds": ["Iou:Iou"], "query": {"amount": {"%gt": 50}}},
          {"templateIds": ["Iou:Iou"]}
        ]"""

      for {
        aliceHeaders <- getAliceHeaders
        (party, headers) = aliceHeaders
        creation <- initialIouCreate(uri, party, headers)
        iouCid = resultContractId(creation)
        jwt <- jwtForParties(uri)(List(party.unwrap), List(), testId)
        (kill, source) = singleClientQueryStream(jwt, uri, query)
          .viaMat(KillSwitches.single)(Keep.right)
          .preMaterialize()
        lastState <- source via parseResp runWith resp(iouCid, kill)
        liveOffset = inside(lastState) { case ShouldHaveEnded(liveStart, 2, lastSeen) =>
          lastSeen.unwrap should be > liveStart.unwrap
          liveStart
        }
        rescan <- (singleClientQueryStream(jwt, uri, query, Some(liveOffset))
          via parseResp).take(1) runWith remainingDeltas
      } yield inside(rescan) {
        case (Vector((fstId, fst @ _), (sndId, snd @ _)), Vector(observeConsumed), Some(_)) =>
          Set(fstId, sndId, observeConsumed.contractId) should have size 3
      }
    }

    "multi-party query" in withHttpService { fixture =>
      import fixture.uri
      for {
        aliceHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, aliceAuthHeaders) = aliceHeaders
        bobHeaders <- fixture.getUniquePartyAndAuthHeaders("Bob")
        (bob, bobAuthHeaders) = bobHeaders
        f1 =
          postCreateCommand(
            accountCreateCommand(alice, "abc123"),
            fixture,
            headers = aliceAuthHeaders,
          )
        f2 =
          postCreateCommand(
            accountCreateCommand(bob, "def456"),
            fixture,
            headers = bobAuthHeaders,
          )

        query =
          """[
          {"templateIds": ["Account:Account"]}
        ]"""
        resp = (
            cid1: domain.ContractId,
            cid2: domain.ContractId,
            kill: UniqueKillSwitch,
        ) => {
          val dslSyntax = Consume.syntax[JsValue]
          import dslSyntax._
          Consume.interpret(
            for {
              Vector((account1, _), (account2, _)) <- readAcsN(2)
              _ = Seq(account1, account2) should contain theSameElementsAs Seq(cid1, cid2)
              ContractDelta(Vector(), _, Some(liveStartOffset)) <- readOne
              _ <- liftF(
                postCreateCommand(
                  accountCreateCommand(alice, "abc234"),
                  fixture,
                  headers = aliceAuthHeaders,
                )
              )
              ContractDelta(Vector((_, aliceAccount)), _, Some(_)) <- readOne
              _ = inside(aliceAccount) { case JsObject(obj) =>
                inside((obj get "owner", obj get "number")) {
                  case (Some(JsString(owner)), Some(JsString(number))) =>
                    owner shouldBe alice.unwrap
                    number shouldBe "abc234"
                }
              }
              _ <- liftF(
                postCreateCommand(
                  accountCreateCommand(bob, "def567"),
                  fixture,
                  headers = bobAuthHeaders,
                )
              )
              ContractDelta(Vector((_, bobAccount)), _, Some(lastSeenOffset)) <- readOne
              _ = inside(bobAccount) { case JsObject(obj) =>
                inside((obj get "owner", obj get "number")) {
                  case (Some(JsString(owner)), Some(JsString(number))) =>
                    owner shouldBe bob.unwrap
                    number shouldBe "def567"
                }
              }
              _ = kill.shutdown()
              heartbeats <- drain
              hbCount = (heartbeats.iterator.map(heartbeatOffset).toSet + lastSeenOffset).size - 1
            } yield (
              // don't count empty events block if lastSeenOffset does not change
              ShouldHaveEnded(
                liveStartOffset = liveStartOffset,
                msgCount = 5 + hbCount,
                lastSeenOffset = lastSeenOffset,
              ),
            )
          )
        }

        r1 <- f1
        cid1 = resultContractId(r1)

        r2 <- f2
        cid2 = resultContractId(r2)

        jwt <- jwtForParties(uri)(List(alice.unwrap, bob.unwrap), List(), testId)
        (kill, source) = singleClientQueryStream(
          jwt,
          uri,
          query,
        ).viaMat(KillSwitches.single)(Keep.right).preMaterialize()
        lastState <- source via parseResp runWith resp(cid1, cid2, kill)
        liveOffset = inside(lastState) { case ShouldHaveEnded(liveStart, 5, lastSeen) =>
          lastSeen.unwrap should be > liveStart.unwrap
          liveStart
        }
        rescan <- jwtForParties(uri)(List(alice.unwrap), List(), testId).flatMap(jwt =>
          (singleClientQueryStream(
            jwt,
            uri,
            query,
            Some(liveOffset),
          )
            via parseResp).take(1) runWith remainingDeltas
        )
      } yield inside(rescan) { case (Vector(_), _, Some(_)) =>
        succeed
      }
    }

    "fetch, filtering out phantom archives" in withHttpService { fixture =>
      import fixture.uri
      for {
        aliceHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, headers) = aliceHeaders
        templateId = TpId.Account.Account
        fetchRequest = (contractIdAtOffset: Option[Option[domain.ContractId]]) => {
          import json.JsonProtocol._
          List(
            Map(
              "templateId" -> "Account:Account".toJson,
              "key" -> List(alice.unwrap, "abc123").toJson,
            )
              ++ contractIdAtOffset
                .map(ocid => contractIdAtOffsetKey -> ocid.toJson)
                .toList
          ).toJson.compactPrint
        }
        f1 =
          postCreateCommand(
            accountCreateCommand(alice, "abc123"),
            fixture,
            headers,
          )
        f2 =
          postCreateCommand(
            accountCreateCommand(alice, "def456"),
            fixture,
            headers,
          )

        resp = (
            cid1: domain.ContractId,
            cid2: domain.ContractId,
            kill: UniqueKillSwitch,
        ) => {
          val dslSyntax = Consume.syntax[JsValue]
          import dslSyntax._
          Consume.interpret(
            for {
              ContractDelta(Vector((cid, c)), Vector(), None) <- readOne
              _ = (cid: domain.ContractId) shouldBe cid1
              ctid <- liftF(postArchiveCommand(templateId, cid2, fixture, headers).flatMap {
                case (statusCode, _) =>
                  statusCode.isSuccess shouldBe true
                  postArchiveCommand(templateId, cid1, fixture, headers).map {
                    case (statusCode, _) =>
                      statusCode.isSuccess shouldBe true
                      cid
                  }
              })

              ContractDelta(Vector(), _, Some(offset)) <- readOne
              (off, archivedCid) = (offset, ctid)

              ContractDelta(Vector(), Vector(observeArchivedCid), Some(lastSeenOffset)) <- readOne
              (liveStartOffset, msgCount) = {
                observeArchivedCid.contractId shouldBe archivedCid
                observeArchivedCid.contractId shouldBe cid1
                (off, 0)
              }

              _ = kill.shutdown()
              heartbeats <- drain
              hbCount = (heartbeats.iterator.map(heartbeatOffset).toSet + lastSeenOffset).size - 1

            } yield
            // don't count empty events block if lastSeenOffset does not change
            ShouldHaveEnded(
              liveStartOffset = liveStartOffset,
              msgCount = msgCount + hbCount,
              lastSeenOffset = lastSeenOffset,
            )
          )
        }
        r1 <- f1
        cid1 = resultContractId(r1)

        r2 <- f2
        cid2 = resultContractId(r2)
        jwt <- jwtForParties(uri)(List(alice.unwrap), List(), testId)
        (kill, source) = singleClientFetchStream(jwt, uri, fetchRequest(None))
          .viaMat(KillSwitches.single)(Keep.right)
          .preMaterialize()

        lastState <- source
          .via(parseResp) runWith resp(cid1, cid2, kill)

        liveOffset = inside(lastState) { case ShouldHaveEnded(liveStart, 0, lastSeen) =>
          lastSeen.unwrap should be > liveStart.unwrap
          liveStart
        }

        // check contractIdAtOffsets' effects on phantom filtering
        resumes <- Future.traverse(Seq((None, 2L), (Some(None), 0L), (Some(Some(cid1)), 1L))) {
          case (abcHint, expectArchives) =>
            (singleClientFetchStream(
              jwt,
              uri,
              fetchRequest(abcHint),
              Some(liveOffset),
            )
              via parseResp)
              .take(2)
              .runWith(remainingDeltas)
              .map { case (creates, archives, _) =>
                creates shouldBe empty
                archives should have size expectArchives
              }
        }

      } yield resumes.foldLeft(1 shouldBe 1)((_, a) => a)
    }

    "multi-party fetch-by-key" in withHttpService { fixture =>
      import fixture.uri
      for {
        aliceHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, aliceAuthHeaders) = aliceHeaders
        bobHeaders <- fixture.getUniquePartyAndAuthHeaders("Bob")
        (bob, bobAuthHeaders) = bobHeaders
        templateId = TpId.Account.Account

        f1 =
          postCreateCommand(
            accountCreateCommand(alice, "abc123"),
            fixture,
            headers = aliceAuthHeaders,
          )
        f2 =
          postCreateCommand(
            accountCreateCommand(bob, "def456"),
            fixture,
            headers = bobAuthHeaders,
          )

        query =
          s"""[
            {"templateId": "Account:Account", "key": ["$alice", "abc123"]},
            {"templateId": "Account:Account", "key": ["$bob", "def456"]}
          ]"""

        resp = (
            cid1: domain.ContractId,
            cid2: domain.ContractId,
            kill: UniqueKillSwitch,
        ) => {
          val dslSyntax = Consume.syntax[JsValue]
          import dslSyntax._
          Consume.interpret(
            for {
              Vector((account1, _), (account2, _)) <- readAcsN(2)
              _ = Seq(account1, account2) should contain theSameElementsAs Seq(cid1, cid2)
              ContractDelta(Vector(), _, Some(liveStartOffset)) <- readOne
              _ <- liftF(postArchiveCommand(templateId, cid1, fixture, aliceAuthHeaders))
              ContractDelta(Vector(), Vector(archivedCid1), Some(_)) <- readOne
              _ = archivedCid1.contractId shouldBe cid1
              _ <- liftF(
                postArchiveCommand(
                  templateId,
                  cid2,
                  fixture,
                  headers = bobAuthHeaders,
                )
              )
              ContractDelta(Vector(), Vector(archivedCid2), Some(lastSeenOffset)) <- readOne
              _ = archivedCid2.contractId shouldBe cid2
              _ = kill.shutdown()
              heartbeats <- drain
              hbCount = (heartbeats.iterator.map(heartbeatOffset).toSet + lastSeenOffset).size - 1
            } yield (
              // don't count empty events block if lastSeenOffset does not change
              ShouldHaveEnded(
                liveStartOffset = liveStartOffset,
                msgCount = 5 + hbCount,
                lastSeenOffset = lastSeenOffset,
              ),
            )
          )
        }
        r1 <- f1
        cid1 = resultContractId(r1)

        r2 <- f2
        cid2 = resultContractId(r2)

        jwt <- jwtForParties(uri)(List(alice.unwrap, bob.unwrap), List(), testId)
        (kill, source) = singleClientFetchStream(
          jwt,
          uri,
          query,
        ).viaMat(KillSwitches.single)(Keep.right).preMaterialize()
        lastState <- source via parseResp runWith resp(cid1, cid2, kill)
        liveOffset = inside(lastState) { case ShouldHaveEnded(liveStart, 5, lastSeen) =>
          lastSeen.unwrap should be > liveStart.unwrap
          liveStart
        }
        rescan <- (singleClientFetchStream(
          jwt,
          uri,
          query,
          Some(liveOffset),
        )
          via parseResp).take(2) runWith remainingDeltas
      } yield inside(rescan) { case (Vector(), Vector(_, _), Some(_)) =>
        succeed
      }
    }
  }

  "fetch multiple keys should work" in withHttpService { fixture =>
    import fixture.uri
    for {
      aliceHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
      (alice, headers) = aliceHeaders
      jwt <- jwtForParties(uri)(List(alice.unwrap), List(), testId)
      create = (account: String) =>
        for {
          r <- postCreateCommand(
            accountCreateCommand(alice, account),
            fixture,
            headers,
          )
        } yield resultContractId(r)
      archive = (id: domain.ContractId) =>
        for {
          r <- postArchiveCommand(
            TpId.Account.Account,
            id,
            fixture,
            headers,
          )
        } yield {
          assert(r._1.isSuccess)
        }
      resp = (kill: UniqueKillSwitch) => {
        val dslSyntax = Consume.syntax[JsValue]
        import dslSyntax._
        Consume.interpret(
          for {
            ContractDelta(Vector(), Vector(), Some(liveStartOffset)) <- readOne
            cid1 <- liftF(create("abc123"))
            ContractDelta(Vector((cid, _)), Vector(), Some(_)) <- readOne
            _ = cid shouldBe cid1
            _ <- liftF(create("abc124"))
            _ <- liftF(create("abc125"))
            cid2 <- liftF(create("def456"))
            ContractDelta(Vector((cid, _)), Vector(), Some(_)) <- readOne
            _ = cid shouldBe cid2
            _ <- liftF(archive(cid2))
            ContractDelta(Vector(), Vector(cid), Some(_)) <- readOne
            _ = cid.contractId shouldBe cid2
            _ <- liftF(archive(cid1))
            ContractDelta(Vector(), Vector(cid), Some(_)) <- readOne
            _ = cid.contractId shouldBe cid1
            _ = kill.shutdown()
            heartbeats <- drain
            _ = heartbeats.foreach { d =>
              inside(d) { case ContractDelta(Vector(), Vector(), Some(_)) =>
                succeed
              }
            }
          } yield succeed
        )
      }
      req =
        s"""
               |[{"templateId": "Account:Account", "key": ["$alice", "abc123"]},
               | {"templateId": "Account:Account", "key": ["$alice", "def456"]}]
               |""".stripMargin
      (kill, source) = singleClientFetchStream(jwt, uri, req)
        .viaMat(KillSwitches.single)(Keep.right)
        .preMaterialize()

      res <- source.via(parseResp).runWith(resp(kill))
    } yield res
  }

  /** Consume ACS blocks expecting `createCount` contracts.  Fail if there
    * are too many contracts.
    */
  private[this] def readAcsN(
      createCount: Int
  ): Consume.FCC[JsValue, Vector[(domain.ContractId, JsValue)]] = {
    val dslSyntax = Consume.syntax[JsValue]
    import dslSyntax._
    def go(createCount: Int): Consume.FCC[JsValue, Vector[(domain.ContractId, JsValue)]] =
      if (createCount <= 0) point(Vector.empty)
      else
        for {
          ContractDelta(creates, Vector(), None) <- readOne
          found = creates.size
          if found <= createCount
          tail <- if (found < createCount) go(createCount - found) else point(Vector.empty)
        } yield creates ++ tail
    go(createCount)
  }

  /** Updates the ACS retrieved with [[readAcsN]] with the given number of events
    * The caller is in charge of reading the live marker if that is expected
    */
  private[this] def updateAcs(
      acs: Map[domain.ContractId, JsValue],
      events: Int,
  ): Consume.FCC[JsValue, Map[domain.ContractId, JsValue]] = {
    val dslSyntax = Consume.syntax[JsValue]
    import dslSyntax._
    def go(
        acs: Map[domain.ContractId, JsValue],
        missingEvents: Int,
    ): Consume.FCC[JsValue, Map[domain.ContractId, JsValue]] =
      if (missingEvents <= 0) {
        point(acs)
      } else {
        for {
          ContractDelta(creates, archives, _) <- readOne
          newAcs = acs ++ creates -- archives.map(_.contractId)
          events = creates.size + archives.size
          next <- go(newAcs, missingEvents - events)
        } yield next
      }
    go(acs, events)
  }

  "fetch should should return an error if empty list of (templateId, key) pairs is passed" in withHttpService {
    (uri, _, _, _) =>
      jwt(uri)
        .flatMap(
          singleClientFetchStream(_, uri, "[]")
            .runWith(collectResultsAsTextMessageSkipOffsetTicks)
        )
        .map { clientMsgs =>
          inside(clientMsgs) { case Seq(errorMsg) =>
            val errorResponse = decodeErrorResponse(errorMsg)
            errorResponse.status shouldBe StatusCodes.BadRequest
            inside(errorResponse.errors) { case List(error) =>
              error should include("must be a JSON array with at least 1 element")
            }
          }
        }: Future[Assertion]
  }

  "fail reading from a pruned offset" in withHttpService { fixture =>
    import fixture.{uri, client}
    for {
      aliceH <- fixture.getUniquePartyAndAuthHeaders("Alice")
      (alice, aliceHeaders) = aliceH
      offsets <- offsetBeforeAfterArchival(alice, fixture, aliceHeaders)
      (offsetBeforeArchive, offsetAfterArchive) = offsets

      pruned <- PruneGrpc.ParticipantPruningServiceGrpc
        .stub(client.channel)
        .prune(
          PruneGrpc.PruneRequest(
            pruneUpTo = domain.Offset unwrap offsetAfterArchive,
            pruneAllDivulgedContracts = true,
          )
        )
      _ = pruned should ===(PruneGrpc.PruneResponse())

      // now query again with a pruned offset
      jwt <- jwtForParties(uri)(List(alice.unwrap), List(), testId)
      query = s"""[{"templateIds": ["Iou:Iou"]}]"""
      streamError <- singleClientQueryStream(jwt, uri, query, Some(offsetBeforeArchive))
        .runWith(Sink.seq)
        .failed
    } yield inside(streamError) { case t: PeerClosedConnectionException =>
      // TODO #13506 descriptive/structured error.  The logs when running this
      // test include
      //     Websocket handler failed with FAILED_PRECONDITION: PARTICIPANT_PRUNED_DATA_ACCESSED(9,0):
      //     Transactions request from 0000000000000006 to 0000000000000008
      //     precedes pruned offset 0000000000000007
      // but this doesn't propagate to the client
      t.closeCode should ===(1011) // see RFC 6455
      t.closeReason should ===("internal error")
    }
  }

  import AbstractHttpServiceIntegrationTestFuns.{UriFixture, EncoderFixture}

  private[this] def offsetBeforeAfterArchival(
      party: domain.Party,
      fixture: UriFixture with EncoderFixture,
      headers: List[HttpHeader],
  ): Future[(domain.Offset, domain.Offset)] = {
    import fixture.uri
    type In = JsValue // JsValue might not be the most convenient choice
    val syntax = Consume.syntax[In]
    import syntax._

    def offsetAfterCreate(): Consume.FCC[In, (domain.ContractId, domain.Offset)] = for {
      // make a contract
      create <- liftF(
        postCreateCommand(
          iouCreateCommand(party),
          fixture,
          headers,
        )
      )
      cid = resultContractId(create)
      // wait for the creation's offset
      offsetAfter <- readUntil[In] {
        case ContractDelta(creates, _, off @ Some(_)) =>
          if (creates.exists(_._1 == cid)) off else None
        case _ => None
      }
    } yield (cid, offsetAfter)

    def readMidwayOffset(kill: UniqueKillSwitch) = for {
      // wait for the ACS
      _ <- readUntil[In] {
        case ContractDelta(_, _, offset) => offset
        case _ => None
      }
      // make a contract and fetch the offset after it
      (cid, betweenOffset) <- offsetAfterCreate()
      // archive it
      archive <- liftF(postArchiveCommand(TpId.Iou.Iou, cid, fixture, headers))
      _ = archive._1 should ===(StatusCodes.OK)
      // wait for the archival offset
      afterOffset <- readUntil[In] {
        case ContractDelta(_, archived, offset) =>
          if (archived.exists(_.contractId == cid)) offset else None
        case _ => None
      }
      // if you try to prune afterOffset, pruning fails with
      // OFFSET_OUT_OF_RANGE(9,db14ee96): prune_up_to needs to be before ledger end 0000000000000007
      // create another dummy contract and ignore it
      _ <- offsetAfterCreate()
      _ = kill.shutdown()
    } yield (betweenOffset, afterOffset)

    val query = """[{"templateIds": ["Iou:Iou"]}]"""
    for {
      jwt <- jwtForParties(uri)(List(party.unwrap), List(), testId)
      (kill, source) =
        singleClientQueryStream(jwt, uri, query)
          .viaMat(KillSwitches.single)(Keep.right)
          .preMaterialize()
      offsets <- source.via(parseResp).runWith(Consume.interpret(readMidwayOffset(kill)))
    } yield offsets
  }

  "query on a bunch of random splits should yield consistent results" in withHttpService {
    fixture =>
      import fixture.uri
      for {
        aliceHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, headers) = aliceHeaders
        splitSample = SplitSeq.gen.map(_ map (BigDecimal(_))).sample.get
        query =
          """[
            {"templateIds": ["Iou:Iou"]}
          ]"""
        jwt <- jwtForParties(uri)(List(alice.unwrap), List(), testId)
        (kill, source) =
          singleClientQueryStream(jwt, uri, query)
            .viaMat(KillSwitches.single)(Keep.right)
            .preMaterialize()
        res <- source
          .via(parseResp)
          .map(iouSplitResult)
          .filterNot(_ == \/-((Vector(), Vector()))) // liveness marker/heartbeat
          .runWith(
            Consume.interpret(trialSplitSeq(fixture, splitSample, kill, alice, headers))
          )
      } yield res
  }

  private def trialSplitSeq(
      fixture: UriFixture,
      ss: SplitSeq[BigDecimal],
      kill: UniqueKillSwitch,
      partyName: domain.Party,
      headers: List[HttpHeader],
  ): Consume.FCC[IouSplitResult, Assertion] = {
    val dslSyntax = Consume.syntax[IouSplitResult]
    import SplitSeq._
    import dslSyntax._
    def go(
        createdCid: domain.ContractId,
        ss: SplitSeq[BigDecimal],
    ): Consume.FCC[IouSplitResult, Assertion] = ss match {
      case Leaf(_) =>
        point(1 shouldBe 1)
      case Node(_, l, r) =>
        for {
          (StatusCodes.OK, _) <- liftF(
            fixture.postJsonRequest(
              Uri.Path("/v1/exercise"),
              exercisePayload(createdCid, l.x),
              headers,
            )
          )

          \/-((Vector((cid1, amt1), (cid2, amt2)), Vector(archival))) <- readOne
          (lCid, rCid) = {
            archival should ===(createdCid)
            Set(amt1, amt2) should ===(Set(l.x, r.x))
            if (amt1 == l.x) (cid1, cid2) else (cid2, cid1)
          }

          _ <- go(lCid, l)
          last <- go(rCid, r)
        } yield last
    }

    val initialPayload = {
      import json.JsonProtocol._
      domain
        .CreateCommand(
          TpId.Iou.Iou,
          Map(
            "observers" -> List[String]().toJson,
            "issuer" -> partyName.toJson,
            "amount" -> ss.x.toJson,
            "currency" -> "USD".toJson,
            "owner" -> partyName.toJson,
          ).toJson,
          meta = None,
        )
        .toJson
    }
    for {
      (StatusCodes.OK, _) <- liftF(
        fixture.postJsonRequest(
          Uri.Path("/v1/create"),
          initialPayload,
          headers,
        )
      )
      \/-((Vector((genesisCid, amt)), Vector())) <- readOne
      _ = amt should ===(ss.x)
      last <- go(genesisCid, ss)
      _ = kill.shutdown()
    } yield last
  }

  private def iouSplitResult(jsv: JsValue): IouSplitResult = jsv match {
    case ContractDelta(creates, archives, _) =>
      creates traverse {
        case (cid, JsObject(fields)) =>
          fields get "amount" collect { case JsString(amt) =>
            (cid, BigDecimal(amt))
          }
        case _ => None
      } map ((_, archives map (_.contractId))) toRightDisjunction jsv
    case _ => -\/(jsv)
  }

  "no duplicates should be returned when retrieving contracts for multiple parties" in withHttpService {
    fixture =>
      import fixture.uri

      def test(
          expectedContractId: String,
          expectedParties: Vector[JsString],
          killSwitch: UniqueKillSwitch,
      ): Sink[JsValue, Future[ShouldHaveEnded]] = {
        val dslSyntax = Consume.syntax[JsValue]
        import dslSyntax._
        Consume.interpret(
          for {
            Vector((sharedAccountId, sharedAccount)) <- readAcsN(1)
            _ = sharedAccountId shouldBe expectedContractId
            ContractDelta(Vector(), _, Some(offset)) <- readOne
            _ = inside(sharedAccount) { case JsObject(obj) =>
              inside((obj get "owners", obj get "number")) {
                case (Some(JsArray(owners)), Some(JsString(number))) =>
                  owners should contain theSameElementsAs expectedParties
                  number shouldBe "4444"
              }
            }
            ContractDelta(Vector(), _, Some(_)) <- readOne
            _ = killSwitch.shutdown()
            heartbeats <- drain
            hbCount = (heartbeats.iterator.map(heartbeatOffset).toSet + offset).size - 1
          } yield
          // don't count empty events block if lastSeenOffset does not change
          ShouldHaveEnded(
            liveStartOffset = offset,
            msgCount = 2 + hbCount,
            lastSeenOffset = offset,
          )
        )
      }

      for {
        aliceAndBob @ List(alice, bob) <- List("Alice", "Bob").traverse { p =>
          fixture.getUniquePartyAndAuthHeaders(p).map(_._1.unwrap)
        }
        jwtForAliceAndBob <-
          jwtForParties(uri)(actAs = aliceAndBob, readAs = Nil, ledgerId = testId)
        createResponse <-
          fixture
            .headersWithPartyAuth(aliceAndBob)
            .flatMap(headers =>
              postCreateCommand(
                cmd = sharedAccountCreateCommand(owners = aliceAndBob, "4444"),
                fixture = fixture,
                headers = headers,
              )
            )
        expectedContractId = resultContractId(createResponse)
        (killSwitch, source) = singleClientQueryStream(
          jwt = jwtForAliceAndBob,
          serviceUri = uri,
          query = """{"templateIds": ["Account:SharedAccount"]}""",
        )
          .viaMat(KillSwitches.single)(Keep.right)
          .preMaterialize()
        result <- source via parseResp runWith test(
          expectedContractId.unwrap,
          Vector(JsString(alice), JsString(bob)),
          killSwitch,
        )
      } yield inside(result) { case ShouldHaveEnded(_, 2, _) =>
        succeed
      }
  }

  "Per-query offsets should work as expected" in withHttpService { fixture =>
    import fixture.uri
    val dslSyntax = Consume.syntax[JsValue]
    import dslSyntax._
    for {
      aliceHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
      (alice, headers) = aliceHeaders
      jwt <- jwtForParties(uri)(List(alice.unwrap), List(), testId)
      createIouCommand = (currency: String) => s"""{
           |  "templateId": "Iou:Iou",
           |  "payload": {
           |    "observers": [],
           |    "issuer": "$alice",
           |    "amount": "999.99",
           |    "currency": "$currency",
           |    "owner": "$alice"
           |  }
           |}""".stripMargin
      createIou = (currency: String) =>
        HttpServiceTestFixture
          .postJsonStringRequest(
            uri.withPath(Uri.Path("/v1/create")),
            createIouCommand(currency),
            headers,
          )
          .map(_._1 shouldBe a[StatusCodes.Success])
      contractsQuery = (currency: String) =>
        s"""{"templateIds":["Iou:Iou"], "query":{"currency":"$currency"}}"""
      contractsQueryWithOffset = (offset: domain.Offset, currency: String) =>
        s"""{"templateIds":["Iou:Iou"], "query":{"currency":"$currency"}, "offset":"${offset.unwrap}"}"""
      contracts = (currency: String, offset: Option[domain.Offset]) =>
        offset.fold(contractsQuery(currency))(contractsQueryWithOffset(_, currency))
      acsEnd = (expectedContracts: Int) => {
        def go(killSwitch: UniqueKillSwitch): Sink[JsValue, Future[domain.Offset]] =
          Consume.interpret(
            for {
              _ <- readAcsN(expectedContracts)
              ContractDelta(Vector(), Vector(), Some(offset)) <- readOne
              _ = killSwitch.shutdown()
              _ <- drain
            } yield offset
          )
        val (killSwitch, source) =
          singleClientQueryStream(jwt, uri, """{"templateIds":["Iou:Iou"]}""")
            .viaMat(KillSwitches.single)(Keep.right)
            .preMaterialize()
        source.via(parseResp).runWith(go(killSwitch))
      }
      test = (
          clue: String,
          expectedAcsSize: Int,
          expectedEvents: Int,
          queryFrom: Option[domain.Offset],
          eurFrom: Option[domain.Offset],
          usdFrom: Option[domain.Offset],
          expected: Map[String, Int],
      ) => {
        def go(killSwitch: UniqueKillSwitch): Sink[JsValue, Future[Assertion]] = {
          Consume.interpret(
            for {
              acs <- readAcsN(expectedAcsSize)
              _ <- if (acs.nonEmpty) readOne else point(())
              contracts <- updateAcs(Map.from(acs), expectedEvents)
              result = contracts
                .map(_._2.asJsObject.fields("currency").asInstanceOf[JsString].value)
                .groupBy(identity)
                .map { case (k, vs) => k -> vs.size }
              _ = killSwitch.shutdown()
            } yield withClue(clue) { result shouldEqual expected }
          )
        }
        val (killSwitch, source) =
          singleClientQueryStream(
            jwt,
            uri,
            Seq(contracts("EUR", eurFrom), contracts("USD", usdFrom)).mkString("[", ",", "]"),
            queryFrom,
          )
            .viaMat(KillSwitches.single)(Keep.right)
            .preMaterialize()
        source.via(parseResp).runWith(go(killSwitch))
      }
      _ <- createIou("EUR")
      _ <- createIou("USD")
      offset1 <- acsEnd(2)
      _ <- createIou("EUR")
      _ <- createIou("USD")
      offset2 <- acsEnd(4)
      _ <- createIou("EUR")
      _ <- createIou("USD")
      _ <- test(
        "No offsets",
        6,
        0,
        None,
        None,
        None,
        Map(
          "EUR" -> 3,
          "USD" -> 3,
        ),
      )
      _ <- test(
        "Offset message only",
        0,
        2,
        Some(offset2),
        None,
        None,
        Map(
          "EUR" -> 1,
          "USD" -> 1,
        ),
      )
      _ <- test(
        "Per-query offsets only",
        0,
        3,
        None,
        Some(offset1),
        Some(offset2),
        Map(
          "EUR" -> 2,
          "USD" -> 1,
        ),
      )
      _ <- test(
        "Absent per-query offset is overridden by offset message",
        0,
        3,
        Some(offset2),
        None,
        Some(offset1),
        Map(
          "EUR" -> 1,
          "USD" -> 2,
        ),
      )
      _ <- test(
        "Offset message does not override per-query offsets",
        0,
        4,
        Some(offset2),
        Some(offset1),
        Some(offset1),
        Map(
          "EUR" -> 2,
          "USD" -> 2,
        ),
      )
      _ <- test(
        "Per-query offset with ACS query",
        3,
        1,
        None,
        None,
        Some(offset2),
        Map(
          "EUR" -> 3,
          "USD" -> 1,
        ),
      )
    } yield succeed
  }

  "ContractKeyStreamRequest" - {
    import json.JsonProtocol._
    val baseVal =
      domain.EnrichedContractKey(
        domain.ContractTypeId.Template(Some("ab"), "cd", "ef"),
        JsString("42"): JsValue,
      )
    val baseMap = baseVal.toJson.asJsObject.fields
    val withSome = JsObject(baseMap + (contractIdAtOffsetKey -> JsString("hi")))
    val withNone = JsObject(baseMap + (contractIdAtOffsetKey -> JsNull))

    "initial JSON reader" - {
      type T = domain.ContractKeyStreamRequest[Unit, JsValue]

      "shares EnrichedContractKey format" in {
        JsObject(baseMap).convertTo[T] should ===(domain.ContractKeyStreamRequest((), baseVal))
      }

      "errors on contractIdAtOffset presence" in {
        a[DeserializationException] shouldBe thrownBy {
          withSome.convertTo[T]
        }
        a[DeserializationException] shouldBe thrownBy {
          withNone.convertTo[T]
        }
      }
    }

    "resuming JSON reader" - {
      type T = domain.ContractKeyStreamRequest[Option[Option[domain.ContractId]], JsValue]

      "shares EnrichedContractKey format" in {
        JsObject(baseMap).convertTo[T] should ===(domain.ContractKeyStreamRequest(None, baseVal))
      }

      "distinguishes null and string" in {
        withSome.convertTo[T] should ===(domain.ContractKeyStreamRequest(Some(Some("hi")), baseVal))
        withNone.convertTo[T] should ===(domain.ContractKeyStreamRequest(Some(None), baseVal))
      }
    }
  }

  private def wsConnectRequest[M](
      uri: Uri,
      subprotocol: Option[String],
      input: Source[Message, NotUsed],
  ) =
    Http().singleWebSocketRequest(
      request = WebSocketRequest(uri = uri, subprotocol = subprotocol),
      clientFlow = dummyFlow(input),
    )

  private def assertHeartbeat(str: String): Assertion =
    inside(
      SprayJson
        .decode[EventsBlock](str)
    ) { case \/-(eventsBlock) =>
      eventsBlock.events shouldBe Vector.empty[JsValue]
      inside(eventsBlock.offset) {
        case Some(JsString(offset)) =>
          offset.length should be > 0
        case Some(JsNull) =>
          Succeeded
      }
    }

  private def decodeErrorResponse(str: String): domain.ErrorResponse = {
    import json.JsonProtocol._
    inside(SprayJson.decode[domain.ErrorResponse](str)) { case \/-(e) =>
      e
    }
  }

  private def decodeServiceWarning(str: String): domain.ServiceWarning = {
    import json.JsonProtocol._
    inside(SprayJson.decode[domain.AsyncWarningsWrapper](str)) { case \/-(w) =>
      w.warnings
    }
  }
}
