// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.model.{HttpHeader, StatusCodes, Uri}
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.daml.http.json.SprayJson
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.std.option._
import scalaz.std.vector._
import scalaz.syntax.std.option._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.{-\/, \/-}
import spray.json.{JsNull, JsObject, JsString, JsValue}

import scala.annotation.nowarn
import scala.collection.compat._
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

  import HttpServiceTestFixture._
  import WebsocketTestFixture._

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def useTls = UseTls.NoTls

  override def wsConfig: Option[WebsocketConfig] = Some(Config.DefaultWsConfig)

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

  List(
    SimpleScenario("query", Uri.Path("/v1/stream/query"), baseQueryInput),
    SimpleScenario("fetch", Uri.Path("/v1/stream/fetch"), baseFetchInput),
  ).foreach { scenario =>
    s"${scenario.id} request with valid protocol token should allow client subscribe to stream" in withHttpService {
      (uri, _, _, _) =>
        wsConnectRequest(
          uri.copy(scheme = "ws").withPath(scenario.path),
          validSubprotocol(jwt),
          scenario.input,
        )._1 flatMap (x => x.response.status shouldBe StatusCodes.SwitchingProtocols)
    }

    s"${scenario.id} request with invalid protocol token should be denied" in withHttpService {
      (uri, _, _, _) =>
        wsConnectRequest(
          uri.copy(scheme = "ws").withPath(scenario.path),
          Option("foo"),
          scenario.input,
        )._1 flatMap (x => x.response.status shouldBe StatusCodes.Unauthorized)
    }

    s"${scenario.id} request without protocol token should be denied" in withHttpService {
      (uri, _, _, _) =>
        wsConnectRequest(
          uri.copy(scheme = "ws").withPath(scenario.path),
          None,
          scenario.input,
        )._1 flatMap (x => x.response.status shouldBe StatusCodes.Unauthorized)
    }

    s"two ${scenario.id} requests over the same WebSocket connection are NOT allowed" in withHttpService {
      (uri, _, _, _) =>
        val input = scenario.input.mapConcat(x => List(x, x))
        val webSocketFlow =
          Http().webSocketClientFlow(
            WebSocketRequest(
              uri = uri.copy(scheme = "ws").withPath(scenario.path),
              subprotocol = validSubprotocol(jwt),
            )
          )
        input
          .via(webSocketFlow)
          .runWith(collectResultsAsTextMessageSkipOffsetTicks)
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
      (uri, _, _, _) =>
        val webSocketFlow =
          Http().webSocketClientFlow(
            WebSocketRequest(
              uri = uri.copy(scheme = "ws").withPath(scenario.path),
              subprotocol = validSubprotocol(jwt),
            )
          )
        scenario.input
          .via(webSocketFlow)
          .runWith(collectResultsAsTextMessageSkipOffsetTicks)
          .flatMap { msgs =>
            inside(msgs) { case Seq(warningMsg, errorMsg) =>
              val warning = decodeServiceWarning(warningMsg)
              inside(warning) { case domain.UnknownTemplateIds(ids) =>
                ids shouldBe List(domain.TemplateId(None, "AA", "BB"))
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

  "query endpoint should publish transactions when command create is completed" in withHttpService {
    (uri, _, _, _) =>
      val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
      for {
        _ <- initialIouCreate(uri, alice, headers)
        jwt = jwtForParties(List(alice.unwrap), List(), testId)
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

  "fetch endpoint should publish transactions when command create is completed" in withHttpService {
    (uri, encoder, _, _) =>
      val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
      for {
        _ <- initialAccountCreate(uri, encoder, alice, headers)
        jwt = jwtForParties(List(alice.unwrap), Nil, testId)
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

  "query endpoint should warn on unknown template IDs" in withHttpService { (uri, _, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    for {
      _ <- initialIouCreate(uri, alice, headers)

      clientMsg <- singleClientQueryStream(
        jwtForParties(List(alice.unwrap), List(), testId),
        uri,
        """{"templateIds": ["Iou:Iou", "Unknown:Template"]}""",
      ).take(3)
        .runWith(collectResultsAsTextMessage)
    } yield inside(clientMsg) { case warning +: result +: heartbeats =>
      warning should include("\"warnings\":{\"unknownTemplateIds\":[\"Unk")
      result should include(s""""issuer":"$alice"""")
      Inspectors.forAll(heartbeats)(assertHeartbeat)
    }
  }

  "fetch endpoint should warn on unknown template IDs" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    for {
      _ <- initialAccountCreate(uri, encoder, alice, headers)

      clientMsg <- singleClientFetchStream(
        jwtForParties(List(alice.unwrap), List(), testId),
        uri,
        s"""[{"templateId": "Account:Account", "key": ["$alice", "abc123"]}, {"templateId": "Unknown:Template", "key": ["$alice", "abc123"]}]""",
      ).take(3)
        .runWith(collectResultsAsTextMessage)
    } yield inside(clientMsg) { case warning +: result +: heartbeats =>
      warning should include("""{"warnings":{"unknownTemplateIds":["Unk""")
      result should include(s""""owner":"$alice"""")
      result should include(""""number":"abc123"""")
      Inspectors.forAll(heartbeats)(assertHeartbeat)
    }
  }

  "query endpoint should send error msg when receiving malformed message" in withHttpService {
    (uri, _, _, _) =>
      val clientMsg = singleClientQueryStream(jwt, uri, "{}")
        .runWith(collectResultsAsTextMessageSkipOffsetTicks)

      val result = Await.result(clientMsg, 10.seconds)

      result should have size 1
      val errorResponse = decodeErrorResponse(result.head)
      errorResponse.status shouldBe StatusCodes.BadRequest
      errorResponse.errors should have size 1
  }

  "fetch endpoint should send error msg when receiving malformed message" in withHttpService {
    (uri, _, _, _) =>
      val clientMsg = singleClientFetchStream(jwt, uri, """[abcdefg!]""")
        .runWith(collectResultsAsTextMessageSkipOffsetTicks)

      val result = Await.result(clientMsg, 10.seconds)

      result should have size 1
      val errorResponse = decodeErrorResponse(result.head)
      errorResponse.status shouldBe StatusCodes.BadRequest
      errorResponse.errors should have size 1
  }

  private def exercisePayload(cid: domain.ContractId, amount: BigDecimal = BigDecimal("42.42")) = {
    import json.JsonProtocol._
    import spray.json._
    Map(
      "templateId" -> "Iou:Iou".toJson,
      "contractId" -> cid.toJson,
      "choice" -> "Iou_Split".toJson,
      "argument" -> Map("splitAmount" -> amount).toJson,
    ).toJson
  }

  "matchedQueries should be correct for multiqueries with per-query offsets" in withHttpService {
    (uri, _, _, _) =>
      import spray.json._

      val (party, headers) = getUniquePartyAndAuthHeaders("Alice")
      val initialCreate = initialIouCreate(uri, party, headers)

      //initial query without offset
      val query =
        """[
          {"templateIds": ["Iou:Iou"], "query": {"currency": "USD"}}
        ]"""

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
              _ = (ctid: String) shouldBe (iouCid.unwrap: String)

              _ = {
                inside(evtsWrapper) { case JsObject(obj) =>
                  inside(obj get "events") {
                    case Some(
                          JsArray(
                            Vector(
                              Created(IouAmount(amt), MatchedQueries(NumList(ix), _))
                            )
                          )
                        ) =>
                      //matchedQuery should be 0 for the initial query supplied
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

      for {
        creation <- initialCreate
        _ = creation._1 shouldBe a[StatusCodes.Success]
        iouCid = getContractId(getResult(creation._2))
        jwt = jwtForParties(List(party.unwrap), List(), testId)
        (kill, source) = singleClientQueryStream(jwt, uri, query)
          .viaMat(KillSwitches.single)(Keep.right)
          .preMaterialize()
        lastSeen <- source via parseResp runWith resp(iouCid, kill)

        //construct a new multiquery with one of them having an offset while the other doesn't
        multiquery = s"""[
          {"templateIds": ["Iou:Iou"], "query": {"currency": "USD"}, "offset": "${lastSeen.unwrap}"},
          {"templateIds": ["Iou:Iou"]}
        ]"""

        clientMsg <- singleClientQueryStream(jwt, uri, multiquery)
          .take(1)
          .runWith(collectResultsAsTextMessageSkipOffsetTicks)
      } yield inside(clientMsg) { case Vector(result) =>
        //we should expect to have matchedQueries [1] to indicate a match for the new template query only.
        result should include(s"""$iouCid""")
        result should include(""""matchedQueries":[1]""")
      }
  }

  "query should receive deltas as contracts are archived/created" in withHttpService {
    (uri, _, _, _) =>
      import spray.json._

      val (party, headers) = getUniquePartyAndAuthHeaders("Alice")
      val initialCreate = initialIouCreate(uri, party, headers)

      val query =
        """[
          {"templateIds": ["Iou:Iou"], "query": {"amount": {"%lte": 50}}},
          {"templateIds": ["Iou:Iou"], "query": {"amount": {"%gt": 50}}},
          {"templateIds": ["Iou:Iou"]}
        ]"""

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
              _ = (ctid: String) shouldBe (iouCid.unwrap: String)
              _ <- liftF(
                postJsonRequest(
                  uri.withPath(Uri.Path("/v1/exercise")),
                  exercisePayload(domain.ContractId(ctid)),
                  headers,
                ) map { case (statusCode, _) =>
                  statusCode.isSuccess shouldBe true
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

      for {
        creation <- initialCreate
        _ = creation._1 shouldBe a[StatusCodes.Success]
        iouCid = getContractId(getResult(creation._2))
        jwt = jwtForParties(List(party.unwrap), List(), testId)
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

  "multi-party query should receive deltas as contracts are archived/created" in withHttpService {
    (uri, encoder, _, _) =>
      import spray.json._

      val (alice, aliceAuthHeaders) = getUniquePartyAndAuthHeaders("Alice")
      val (bob, bobAuthHeaders) = getUniquePartyAndAuthHeaders("Bob")
      val f1 =
        postCreateCommand(
          accountCreateCommand(alice, "abc123"),
          encoder,
          uri,
          headers = aliceAuthHeaders,
        )
      val f2 =
        postCreateCommand(
          accountCreateCommand(bob, "def456"),
          encoder,
          uri,
          headers = bobAuthHeaders,
        )

      val query =
        """[
          {"templateIds": ["Account:Account"]}
        ]"""

      def resp(
          cid1: domain.ContractId,
          cid2: domain.ContractId,
          kill: UniqueKillSwitch,
      ): Sink[JsValue, Future[ShouldHaveEnded]] = {
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
                encoder,
                uri,
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
                encoder,
                uri,
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

      for {
        r1 <- f1
        _ = r1._1 shouldBe a[StatusCodes.Success]
        cid1 = getContractId(getResult(r1._2))

        r2 <- f2
        _ = r2._1 shouldBe a[StatusCodes.Success]
        cid2 = getContractId(getResult(r2._2))

        (kill, source) = singleClientQueryStream(
          jwtForParties(List(alice.unwrap, bob.unwrap), List(), testId),
          uri,
          query,
        ).viaMat(KillSwitches.single)(Keep.right).preMaterialize()
        lastState <- source via parseResp runWith resp(cid1, cid2, kill)
        liveOffset = inside(lastState) { case ShouldHaveEnded(liveStart, 5, lastSeen) =>
          lastSeen.unwrap should be > liveStart.unwrap
          liveStart
        }
        rescan <- (singleClientQueryStream(
          jwtForParties(List(alice.unwrap), List(), testId),
          uri,
          query,
          Some(liveOffset),
        )
          via parseResp).take(1) runWith remainingDeltas
      } yield inside(rescan) { case (Vector(_), _, Some(_)) =>
        succeed
      }
  }

  "fetch should receive deltas as contracts are archived/created, filtering out phantom archives" in withHttpService {
    (uri, encoder, _, _) =>
      val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
      val templateId = domain.TemplateId(None, "Account", "Account")
      def fetchRequest(contractIdAtOffset: Option[Option[domain.ContractId]] = None) = {
        import spray.json._, json.JsonProtocol._
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
      val f1 =
        postCreateCommand(
          accountCreateCommand(alice, "abc123"),
          encoder,
          uri,
          headers,
        )
      val f2 =
        postCreateCommand(
          accountCreateCommand(alice, "def456"),
          encoder,
          uri,
          headers,
        )

      def resp(
          cid1: domain.ContractId,
          cid2: domain.ContractId,
          kill: UniqueKillSwitch,
      ): Sink[JsValue, Future[ShouldHaveEnded]] = {
        val dslSyntax = Consume.syntax[JsValue]
        import dslSyntax._
        Consume.interpret(
          for {
            ContractDelta(Vector((cid, c)), Vector(), None) <- readOne
            _ = (cid: String) shouldBe (cid1.unwrap: String)
            ctid <- liftF(postArchiveCommand(templateId, cid2, encoder, uri, headers).flatMap {
              case (statusCode, _) =>
                statusCode.isSuccess shouldBe true
                postArchiveCommand(templateId, cid1, encoder, uri, headers).map {
                  case (statusCode, _) =>
                    statusCode.isSuccess shouldBe true
                    cid
                }
            })

            ContractDelta(Vector(), _, Some(offset)) <- readOne
            (off, archivedCid) = (offset, ctid)

            ContractDelta(Vector(), Vector(observeArchivedCid), Some(lastSeenOffset)) <- readOne
            (liveStartOffset, msgCount) = {
              (observeArchivedCid.contractId.unwrap: String) shouldBe (archivedCid: String)
              (observeArchivedCid.contractId: domain.ContractId) shouldBe (cid1: domain.ContractId)
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

      for {
        r1 <- f1
        _ = r1._1 shouldBe a[StatusCodes.Success]
        cid1 = getContractId(getResult(r1._2))

        r2 <- f2
        _ = r2._1 shouldBe a[StatusCodes.Success]
        cid2 = getContractId(getResult(r2._2))
        jwt = jwtForParties(List(alice.unwrap), List(), testId)
        (kill, source) = singleClientFetchStream(jwt, uri, fetchRequest())
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

  "fetch multiple keys should work" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val jwt = jwtForParties(List(alice.unwrap), List(), testId)
    def create(account: String): Future[domain.ContractId] =
      for {
        r <- postCreateCommand(
          accountCreateCommand(alice, account),
          encoder,
          uri,
          headers,
        )
      } yield {
        assert(r._1.isSuccess)
        getContractId(getResult(r._2))
      }
    def archive(id: domain.ContractId): Future[Assertion] =
      for {
        r <- postArchiveCommand(
          domain.TemplateId(None, "Account", "Account"),
          id,
          encoder,
          uri,
          headers,
        )
      } yield {
        assert(r._1.isSuccess)
      }
    def resp(kill: UniqueKillSwitch): Sink[JsValue, Future[Assertion]] = {
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
    val req =
      s"""
          |[{"templateId": "Account:Account", "key": ["$alice", "abc123"]},
          | {"templateId": "Account:Account", "key": ["$alice", "def456"]}]
          |""".stripMargin

    val (kill, source) = singleClientFetchStream(jwt, uri, req)
      .viaMat(KillSwitches.single)(Keep.right)
      .preMaterialize()

    source.via(parseResp).runWith(resp(kill))
  }

  "multi-party fetch-by-key query should receive deltas as contracts are archived/created" in withHttpService {
    (uri, encoder, _, _) =>
      import spray.json._

      val templateId = domain.TemplateId(None, "Account", "Account")
      val (alice, aliceAuthHeaders) = getUniquePartyAndAuthHeaders("Alice")
      val (bob, bobAuthHeaders) = getUniquePartyAndAuthHeaders("Bob")

      val f1 =
        postCreateCommand(
          accountCreateCommand(alice, "abc123"),
          encoder,
          uri,
          headers = aliceAuthHeaders,
        )
      val f2 =
        postCreateCommand(
          accountCreateCommand(bob, "def456"),
          encoder,
          uri,
          headers = bobAuthHeaders,
        )

      val query =
        s"""[
          {"templateId": "Account:Account", "key": ["$alice", "abc123"]},
          {"templateId": "Account:Account", "key": ["$bob", "def456"]}
        ]"""

      def resp(
          cid1: domain.ContractId,
          cid2: domain.ContractId,
          kill: UniqueKillSwitch,
      ): Sink[JsValue, Future[ShouldHaveEnded]] = {
        val dslSyntax = Consume.syntax[JsValue]
        import dslSyntax._
        Consume.interpret(
          for {
            Vector((account1, _), (account2, _)) <- readAcsN(2)
            _ = Seq(account1, account2) should contain theSameElementsAs Seq(cid1, cid2)
            ContractDelta(Vector(), _, Some(liveStartOffset)) <- readOne
            _ <- liftF(postArchiveCommand(templateId, cid1, encoder, uri, aliceAuthHeaders))
            ContractDelta(Vector(), Vector(archivedCid1), Some(_)) <- readOne
            _ = archivedCid1.contractId shouldBe cid1
            _ <- liftF(
              postArchiveCommand(
                templateId,
                cid2,
                encoder,
                uri,
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

      for {
        r1 <- f1
        _ = r1._1 shouldBe a[StatusCodes.Success]
        cid1 = getContractId(getResult(r1._2))

        r2 <- f2
        _ = r2._1 shouldBe a[StatusCodes.Success]
        cid2 = getContractId(getResult(r2._2))

        jwt = jwtForParties(List(alice.unwrap, bob.unwrap), List(), testId)
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

  /** Consume ACS blocks expecting `createCount` contracts.  Fail if there
    * are too many contracts.
    */
  private[this] def readAcsN(createCount: Int): Consume.FCC[JsValue, Vector[(String, JsValue)]] = {
    val dslSyntax = Consume.syntax[JsValue]
    import dslSyntax._
    def go(createCount: Int): Consume.FCC[JsValue, Vector[(String, JsValue)]] =
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
      acs: Map[String, JsValue],
      events: Int,
  ): Consume.FCC[JsValue, Map[String, JsValue]] = {
    val dslSyntax = Consume.syntax[JsValue]
    import dslSyntax._
    def go(
        acs: Map[String, JsValue],
        missingEvents: Int,
    ): Consume.FCC[JsValue, Map[String, JsValue]] =
      if (missingEvents <= 0) {
        point(acs)
      } else {
        for {
          ContractDelta(creates, archives, _) <- readOne
          newAcs = acs ++ creates -- archives.map(_.contractId.unwrap)
          events = creates.size + archives.size
          next <- go(newAcs, missingEvents - events)
        } yield next
      }
    go(acs, events)
  }

  "fetch should should return an error if empty list of (templateId, key) pairs is passed" in withHttpService {
    (uri, _, _, _) =>
      singleClientFetchStream(jwt, uri, "[]")
        .runWith(collectResultsAsTextMessageSkipOffsetTicks)
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

  "query on a bunch of random splits should yield consistent results" in withHttpService {
    (uri, _, _, _) =>
      val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
      val splitSample = SplitSeq.gen.map(_ map (BigDecimal(_))).sample.get
      val query =
        """[
          {"templateIds": ["Iou:Iou"]}
        ]"""
      val (kill, source) =
        singleClientQueryStream(jwtForParties(List(alice.unwrap), List(), testId), uri, query)
          .viaMat(KillSwitches.single)(Keep.right)
          .preMaterialize()
      source
        .via(parseResp)
        .map(iouSplitResult)
        .filterNot(_ == \/-((Vector(), Vector()))) // liveness marker/heartbeat
        .runWith(Consume.interpret(trialSplitSeq(uri, splitSample, kill, alice.unwrap, headers)))
  }

  private def trialSplitSeq(
      serviceUri: Uri,
      ss: SplitSeq[BigDecimal],
      kill: UniqueKillSwitch,
      partyName: String,
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
            postJsonRequest(
              serviceUri.withPath(Uri.Path("/v1/exercise")),
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
      import spray.json._, json.JsonProtocol._
      Map(
        "templateId" -> "Iou:Iou".toJson,
        "payload" -> Map(
          "observers" -> List[String]().toJson,
          "issuer" -> partyName.toJson,
          "amount" -> ss.x.toJson,
          "currency" -> "USD".toJson,
          "owner" -> partyName.toJson,
        ).toJson,
      ).toJson
    }
    for {
      (StatusCodes.OK, _) <- liftF(
        postJsonRequest(
          serviceUri.withPath(Uri.Path("/v1/create")),
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
            (domain.ContractId(cid), BigDecimal(amt))
          }
        case _ => None
      } map ((_, archives map (_.contractId))) toRightDisjunction jsv
    case _ => -\/(jsv)
  }

  "no duplicates should be returned when retrieving contracts for multiple parties" in withHttpService {
    (uri, encoder, _, _) =>
      val aliceAndBob = List("Alice", "Bob")
      val jwtForAliceAndBob = jwtForParties(actAs = aliceAndBob, readAs = Nil, ledgerId = testId)
      import spray.json._

      def test(
          expectedContractId: String,
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
                  owners should contain theSameElementsAs Vector(JsString("Alice"), JsString("Bob"))
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
        (status, value) <-
          postCreateCommand(
            cmd = sharedAccountCreateCommand(owners = aliceAndBob, "4444"),
            encoder = encoder,
            uri = uri,
            headers = headersWithPartyAuth(aliceAndBob),
          )
        _ = status shouldBe a[StatusCodes.Success]
        expectedContractId = getContractId(getResult(value))
        (killSwitch, source) = singleClientQueryStream(
          jwt = jwtForAliceAndBob,
          serviceUri = uri,
          query = """{"templateIds": ["Account:SharedAccount"]}""",
        )
          .viaMat(KillSwitches.single)(Keep.right)
          .preMaterialize()
        result <- source via parseResp runWith test(expectedContractId.unwrap, killSwitch)
      } yield inside(result) { case ShouldHaveEnded(_, 2, _) =>
        succeed
      }
  }

  "Per-query offsets should work as expected" in withHttpService { (uri, _, _, _) =>
    val dslSyntax = Consume.syntax[JsValue]
    import dslSyntax._
    import spray.json._
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val jwt = jwtForParties(List(alice.unwrap), List(), testId)
    def createIouCommand(currency: String): String =
      s"""{
           |  "templateId": "Iou:Iou",
           |  "payload": {
           |    "observers": [],
           |    "issuer": "$alice",
           |    "amount": "999.99",
           |    "currency": "$currency",
           |    "owner": "$alice"
           |  }
           |}""".stripMargin
    def createIou(currency: String): Future[Assertion] =
      HttpServiceTestFixture
        .postJsonStringRequest(
          uri.withPath(Uri.Path("/v1/create")),
          createIouCommand(currency),
          headers,
        )
        .map(_._1 shouldBe a[StatusCodes.Success])
    def contractsQuery(currency: String): String =
      s"""{"templateIds":["Iou:Iou"], "query":{"currency":"$currency"}}"""
    def contractsQueryWithOffset(offset: domain.Offset, currency: String): String =
      s"""{"templateIds":["Iou:Iou"], "query":{"currency":"$currency"}, "offset":"${offset.unwrap}"}"""
    def contracts(currency: String, offset: Option[domain.Offset]): String =
      offset.fold(contractsQuery(currency))(contractsQueryWithOffset(_, currency))
    def acsEnd(expectedContracts: Int): Future[domain.Offset] = {
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
    def test(
        clue: String,
        expectedAcsSize: Int,
        expectedEvents: Int,
        queryFrom: Option[domain.Offset],
        eurFrom: Option[domain.Offset],
        usdFrom: Option[domain.Offset],
        expected: Map[String, Int],
    ): Future[Assertion] = {
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

    for {
      _ <- createIou("EUR")
      _ <- createIou("USD")
      offset1 <- acsEnd(2)
      _ <- createIou("EUR")
      _ <- createIou("USD")
      offset2 <- acsEnd(4)
      _ <- createIou("EUR")
      _ <- createIou("USD")
      _ <- test(
        clue = "No offsets",
        expectedAcsSize = 6,
        expectedEvents = 0,
        queryFrom = None,
        eurFrom = None,
        usdFrom = None,
        expected = Map(
          "EUR" -> 3,
          "USD" -> 3,
        ),
      )
      _ <- test(
        clue = "Offset message only",
        expectedAcsSize = 0,
        expectedEvents = 2,
        queryFrom = Some(offset2),
        eurFrom = None,
        usdFrom = None,
        expected = Map(
          "EUR" -> 1,
          "USD" -> 1,
        ),
      )
      _ <- test(
        clue = "Per-query offsets only",
        expectedAcsSize = 0,
        expectedEvents = 3,
        queryFrom = None,
        eurFrom = Some(offset1),
        usdFrom = Some(offset2),
        expected = Map(
          "EUR" -> 2,
          "USD" -> 1,
        ),
      )
      _ <- test(
        clue = "Absent per-query offset is overridden by offset message",
        expectedAcsSize = 0,
        expectedEvents = 3,
        queryFrom = Some(offset2),
        eurFrom = None,
        usdFrom = Some(offset1),
        expected = Map(
          "EUR" -> 1,
          "USD" -> 2,
        ),
      )
      _ <- test(
        clue = "Offset message does not override per-query offsets",
        expectedAcsSize = 0,
        expectedEvents = 4,
        queryFrom = Some(offset2),
        eurFrom = Some(offset1),
        usdFrom = Some(offset1),
        expected = Map(
          "EUR" -> 2,
          "USD" -> 2,
        ),
      )
      _ <- test(
        clue = "Per-query offset with ACS query",
        expectedAcsSize = 3,
        expectedEvents = 1,
        queryFrom = None,
        eurFrom = None,
        usdFrom = Some(offset2),
        expected = Map(
          "EUR" -> 3,
          "USD" -> 1,
        ),
      )
    } yield succeed
  }

  "ContractKeyStreamRequest" - {
    import spray.json._, json.JsonProtocol._
    val baseVal =
      domain.EnrichedContractKey(domain.TemplateId(Some("ab"), "cd", "ef"), JsString("42"): JsValue)
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
