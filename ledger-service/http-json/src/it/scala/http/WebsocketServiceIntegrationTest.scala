// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.model.{StatusCodes, Uri}
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

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class WebsocketServiceIntegrationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with StrictLogging
    with AbstractHttpServiceIntegrationTestFuns
    with BeforeAndAfterAll {

  import HttpServiceTestFixture._
  import WebsocketTestFixture._

  override def jdbcConfig: Option[JdbcConfig] = None

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def useTls = UseTls.NoTls

  override def wsConfig: Option[WebsocketConfig] = Some(Config.DefaultWsConfig)

  private val baseQueryInput: Source[Message, NotUsed] =
    Source.single(TextMessage.Strict("""{"templateIds": ["Account:Account"]}"""))

  private val fetchRequest =
    """[{"templateId": "Account:Account", "key": ["Alice", "abc123"]}]"""

  private val baseFetchInput: Source[Message, NotUsed] =
    Source.single(TextMessage.Strict(fetchRequest))

  List(
    SimpleScenario("query", Uri.Path("/v1/stream/query"), baseQueryInput),
    SimpleScenario("fetch", Uri.Path("/v1/stream/fetch"), baseFetchInput)
  ).foreach { scenario =>
    s"${scenario.id} request with valid protocol token should allow client subscribe to stream" in withHttpService {
      (uri, _, _) =>
        wsConnectRequest(
          uri.copy(scheme = "ws").withPath(scenario.path),
          validSubprotocol(jwt),
          scenario.input)._1 flatMap (x =>
          x.response.status shouldBe StatusCodes.SwitchingProtocols)
    }

    s"${scenario.id} request with invalid protocol token should be denied" in withHttpService {
      (uri, _, _) =>
        wsConnectRequest(
          uri.copy(scheme = "ws").withPath(scenario.path),
          Option("foo"),
          scenario.input)._1 flatMap (x => x.response.status shouldBe StatusCodes.Unauthorized)
    }

    s"${scenario.id} request without protocol token should be denied" in withHttpService {
      (uri, _, _) =>
        wsConnectRequest(
          uri.copy(scheme = "ws").withPath(scenario.path),
          None,
          scenario.input
        )._1 flatMap (x => x.response.status shouldBe StatusCodes.Unauthorized)
    }

    s"two ${scenario.id} requests over the same WebSocket connection are NOT allowed" in withHttpService {
      (uri, _, _) =>
        val input = scenario.input.mapConcat(x => List(x, x))
        val webSocketFlow =
          Http().webSocketClientFlow(
            WebSocketRequest(
              uri = uri.copy(scheme = "ws").withPath(scenario.path),
              subprotocol = validSubprotocol(jwt)))
        input
          .via(webSocketFlow)
          .runWith(collectResultsAsTextMessageSkipOffsetTicks)
          .flatMap { msgs =>
            inside(msgs) {
              case Seq(errorMsg) =>
                val error = decodeErrorResponse(errorMsg)
                error shouldBe domain.ErrorResponse(
                  List("Multiple requests over the same WebSocket connection are not allowed."),
                  None,
                  StatusCodes.BadRequest
                )
            }
          }
    }
  }

  List(
    SimpleScenario(
      "query",
      Uri.Path("/v1/stream/query"),
      Source.single(TextMessage.Strict("""{"templateIds": ["AA:BB"]}"""))),
    SimpleScenario(
      "fetch",
      Uri.Path("/v1/stream/fetch"),
      Source.single(TextMessage.Strict("""[{"templateId": "AA:BB", "key": ["k", "v"]}]""")))
  ).foreach { scenario =>
    s"${scenario.id} report UnknownTemplateIds and error when cannot resolve any template ID" in withHttpService {
      (uri, _, _) =>
        val webSocketFlow =
          Http().webSocketClientFlow(
            WebSocketRequest(
              uri = uri.copy(scheme = "ws").withPath(scenario.path),
              subprotocol = validSubprotocol(jwt)))
        scenario.input
          .via(webSocketFlow)
          .runWith(collectResultsAsTextMessageSkipOffsetTicks)
          .flatMap { msgs =>
            inside(msgs) {
              case Seq(warningMsg, errorMsg) =>
                val warning = decodeServiceWarning(warningMsg)
                inside(warning) {
                  case domain.UnknownTemplateIds(ids) =>
                    ids shouldBe List(domain.TemplateId(None, "AA", "BB"))
                }
                val error = decodeErrorResponse(errorMsg)
                error shouldBe domain.ErrorResponse(
                  List(ErrorMessages.cannotResolveAnyTemplateId),
                  None,
                  StatusCodes.BadRequest
                )
            }
          }
    }
  }

  "query endpoint should publish transactions when command create is completed" in withHttpService {
    (uri, _, _) =>
      for {
        _ <- initialIouCreate(uri)

        clientMsg <- singleClientQueryStream(
          jwt,
          uri,
          """{"templateIds": ["Iou:Iou"]}"""
        ).take(2)
          .runWith(collectResultsAsTextMessage)
      } yield
        inside(clientMsg) {
          case result +: heartbeats =>
            result should include(""""issuer":"Alice"""")
            result should include(""""amount":"999.99"""")
            Inspectors.forAll(heartbeats)(assertHeartbeat)
        }
  }

  "fetch endpoint should publish transactions when command create is completed" in withHttpService {
    (uri, encoder, _) =>
      for {
        _ <- initialAccountCreate(uri, encoder)

        clientMsg <- singleClientFetchStream(jwt, uri, fetchRequest)
          .take(2)
          .runWith(collectResultsAsTextMessage)
      } yield
        inside(clientMsg) {
          case result +: heartbeats =>
            result should include(""""owner":"Alice"""")
            result should include(""""number":"abc123"""")
            result should not include (""""offset":"""")
            Inspectors.forAll(heartbeats)(assertHeartbeat)
        }
  }

  "query endpoint should warn on unknown template IDs" in withHttpService { (uri, _, _) =>
    for {
      _ <- initialIouCreate(uri)

      clientMsg <- singleClientQueryStream(
        jwt,
        uri,
        """{"templateIds": ["Iou:Iou", "Unknown:Template"]}"""
      ).take(3)
        .runWith(collectResultsAsTextMessage)
    } yield
      inside(clientMsg) {
        case warning +: result +: heartbeats =>
          warning should include("\"warnings\":{\"unknownTemplateIds\":[\"Unk")
          result should include("\"issuer\":\"Alice\"")
          Inspectors.forAll(heartbeats)(assertHeartbeat)
      }
  }

  "fetch endpoint should warn on unknown template IDs" in withHttpService { (uri, encoder, _) =>
    for {
      _ <- initialAccountCreate(uri, encoder)

      clientMsg <- singleClientFetchStream(
        jwt,
        uri,
        """[{"templateId": "Account:Account", "key": ["Alice", "abc123"]}, {"templateId": "Unknown:Template", "key": ["Alice", "abc123"]}]"""
      ).take(3)
        .runWith(collectResultsAsTextMessage)
    } yield
      inside(clientMsg) {
        case warning +: result +: heartbeats =>
          warning should include("""{"warnings":{"unknownTemplateIds":["Unk""")
          result should include(""""owner":"Alice"""")
          result should include(""""number":"abc123"""")
          Inspectors.forAll(heartbeats)(assertHeartbeat)
      }
  }

  "query endpoint should send error msg when receiving malformed message" in withHttpService {
    (uri, _, _) =>
      val clientMsg = singleClientQueryStream(jwt, uri, "{}")
        .runWith(collectResultsAsTextMessageSkipOffsetTicks)

      val result = Await.result(clientMsg, 10.seconds)

      result should have size 1
      val errorResponse = decodeErrorResponse(result.head)
      errorResponse.status shouldBe StatusCodes.BadRequest
      errorResponse.errors should have size 1
  }

  "fetch endpoint should send error msg when receiving malformed message" in withHttpService {
    (uri, _, _) =>
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
      "argument" -> Map("splitAmount" -> amount).toJson).toJson
  }

  "query should receive deltas as contracts are archived/created" in withHttpService {
    (uri, _, _) =>
      import spray.json._

      val initialCreate = initialIouCreate(uri)

      val query =
        """[
          {"templateIds": ["Iou:Iou"], "query": {"amount": {"%lte": 50}}},
          {"templateIds": ["Iou:Iou"], "query": {"amount": {"%gt": 50}}},
          {"templateIds": ["Iou:Iou"]}
        ]"""

      @com.github.ghik.silencer.silent("evtsWrapper.*never used")
      def resp(
          iouCid: domain.ContractId,
          kill: UniqueKillSwitch): Sink[JsValue, Future[ShouldHaveEnded]] = {
        val dslSyntax = Consume.syntax[JsValue]
        import dslSyntax._
        Consume.interpret(
          for {
            ContractDelta(Vector((ctid, _)), Vector(), None) <- readOne
            _ = (ctid: String) shouldBe (iouCid.unwrap: String)
            _ <- liftF(
              postJsonRequest(
                uri.withPath(Uri.Path("/v1/exercise")),
                exercisePayload(domain.ContractId(ctid)),
                headersWithAuth) map {
                case (statusCode, _) =>
                  statusCode.isSuccess shouldBe true
              })

            ContractDelta(Vector(), _, Some(offset)) <- readOne

            (preOffset, consumedCtid) = (offset, ctid)
            evtsWrapper @ ContractDelta(
              Vector((fstId, fst), (sndId, snd)),
              Vector(observeConsumed),
              Some(lastSeenOffset)
            ) <- readOne
            (liveStartOffset, msgCount) = {
              observeConsumed.contractId should ===(consumedCtid)
              Set(fstId, sndId, consumedCtid) should have size 3
              inside(evtsWrapper) {
                case JsObject(obj) =>
                  inside(obj get "events") {
                    case Some(
                        JsArray(
                          Vector(
                            Archived(_, _),
                            Created(IouAmount(amt1), MatchedQueries(NumList(ixes1), _)),
                            Created(IouAmount(amt2), MatchedQueries(NumList(ixes2), _))))) =>
                      Set((amt1, ixes1), (amt2, ixes2)) should ===(
                        Set(
                          (BigDecimal("42.42"), Vector(BigDecimal(0), BigDecimal(2))),
                          (BigDecimal("957.57"), Vector(BigDecimal(1), BigDecimal(2))),
                        ))
                  }
              }
              (preOffset, 2)
            }

            _ = kill.shutdown
            heartbeats <- drain
            hbCount = (heartbeats.iterator.map {
              case ContractDelta(Vector(), Vector(), Some(currentOffset)) => currentOffset
            }.toSet + lastSeenOffset).size - 1
          } yield
          // don't count empty events block if lastSeenOffset does not change
          ShouldHaveEnded(
            liveStartOffset = liveStartOffset,
            msgCount = msgCount + hbCount,
            lastSeenOffset = lastSeenOffset
          ))
      }

      for {
        creation <- initialCreate
        _ = creation._1 shouldBe 'success
        iouCid = getContractId(getResult(creation._2))
        (kill, source) = singleClientQueryStream(jwt, uri, query)
          .viaMat(KillSwitches.single)(Keep.right)
          .preMaterialize
        lastState <- source via parseResp runWith resp(iouCid, kill)
        liveOffset = inside(lastState) {
          case ShouldHaveEnded(liveStart, 2, lastSeen) =>
            lastSeen.unwrap should be > liveStart.unwrap
            liveStart
        }
        rescan <- (singleClientQueryStream(jwt, uri, query, Some(liveOffset))
          via parseResp).take(1) runWith remainingDeltas
      } yield
        inside(rescan) {
          case (Vector((fstId, fst @ _), (sndId, snd @ _)), Vector(observeConsumed), Some(_)) =>
            Set(fstId, sndId, observeConsumed.contractId) should have size 3
        }
  }

  "multi-party query should receive deltas as contracts are archived/created" in withHttpService {
    (uri, encoder, _) =>
      import spray.json._

      val f1 =
        postCreateCommand(
          accountCreateCommand(domain.Party("Alice"), "abc123"),
          encoder,
          uri,
          headers = headersWithPartyAuth(List("Alice")))
      val f2 =
        postCreateCommand(
          accountCreateCommand(domain.Party("Bob"), "def456"),
          encoder,
          uri,
          headers = headersWithPartyAuth(List("Bob")))

      val query =
        """[
          {"templateIds": ["Account:Account"]}
        ]"""

      def resp(
          cid1: domain.ContractId,
          cid2: domain.ContractId,
          kill: UniqueKillSwitch): Sink[JsValue, Future[ShouldHaveEnded]] = {
        val dslSyntax = Consume.syntax[JsValue]
        import dslSyntax._
        Consume.interpret(
          for {
            ContractDelta(Vector((account1, _)), Vector(), None) <- readOne
            _ = Seq(cid1, cid2) should contain(account1)
            ContractDelta(Vector((account2, _)), Vector(), None) <- readOne
            _ = Seq(cid1, cid2) should contain(account2)
            ContractDelta(Vector(), _, Some(liveStartOffset)) <- readOne
            _ <- liftF(
              postCreateCommand(
                accountCreateCommand(domain.Party("Alice"), "abc234"),
                encoder,
                uri,
                headers = headersWithPartyAuth(List("Alice"))))
            ContractDelta(Vector((_, aliceAccount)), _, Some(_)) <- readOne
            _ = inside(aliceAccount) {
              case JsObject(obj) =>
                inside((obj get "owner", obj get "number")) {
                  case (Some(JsString(owner)), Some(JsString(number))) =>
                    owner shouldBe "Alice"
                    number shouldBe "abc234"
                }
            }
            _ <- liftF(
              postCreateCommand(
                accountCreateCommand(domain.Party("Bob"), "def567"),
                encoder,
                uri,
                headers = headersWithPartyAuth(List("Bob"))))
            ContractDelta(Vector((_, bobAccount)), _, Some(lastSeenOffset)) <- readOne
            _ = inside(bobAccount) {
              case JsObject(obj) =>
                inside((obj get "owner", obj get "number")) {
                  case (Some(JsString(owner)), Some(JsString(number))) =>
                    owner shouldBe "Bob"
                    number shouldBe "def567"
                }
            }
            _ = kill.shutdown
            heartbeats <- drain
            hbCount = (heartbeats.iterator.map {
              case ContractDelta(Vector(), Vector(), Some(currentOffset)) => currentOffset
            }.toSet + lastSeenOffset).size - 1
          } yield
            (
              // don't count empty events block if lastSeenOffset does not change
              ShouldHaveEnded(
                liveStartOffset = liveStartOffset,
                msgCount = 5 + hbCount,
                lastSeenOffset = lastSeenOffset
              )
            ))
      }

      for {
        r1 <- f1
        _ = r1._1 shouldBe 'success
        cid1 = getContractId(getResult(r1._2))

        r2 <- f2
        _ = r2._1 shouldBe 'success
        cid2 = getContractId(getResult(r2._2))

        (kill, source) = singleClientQueryStream(
          jwtForParties(List("Alice", "Bob"), List(), testId),
          uri,
          query
        ).viaMat(KillSwitches.single)(Keep.right).preMaterialize()
        lastState <- source via parseResp runWith resp(cid1, cid2, kill)
        liveOffset = inside(lastState) {
          case ShouldHaveEnded(liveStart, 5, lastSeen) =>
            lastSeen.unwrap should be > liveStart.unwrap
            liveStart
        }
        rescan <- (singleClientQueryStream(jwt, uri, query, Some(liveOffset))
          via parseResp).take(1) runWith remainingDeltas
      } yield
        inside(rescan) {
          case (Vector(_), _, Some(_)) => succeed
        }
  }

  "fetch should receive deltas as contracts are archived/created, filtering out phantom archives" in withHttpService {
    (uri, encoder, _) =>
      val templateId = domain.TemplateId(None, "Account", "Account")
      def fetchRequest(contractIdAtOffset: Option[Option[domain.ContractId]] = None) = {
        import spray.json._, json.JsonProtocol._
        List(
          Map("templateId" -> "Account:Account".toJson, "key" -> List("Alice", "abc123").toJson)
            ++ contractIdAtOffset
              .map(ocid => contractIdAtOffsetKey -> ocid.toJson)
              .toList).toJson.compactPrint
      }
      val f1 =
        postCreateCommand(accountCreateCommand(domain.Party("Alice"), "abc123"), encoder, uri)
      val f2 =
        postCreateCommand(accountCreateCommand(domain.Party("Alice"), "def456"), encoder, uri)

      def resp(
          cid1: domain.ContractId,
          cid2: domain.ContractId,
          kill: UniqueKillSwitch): Sink[JsValue, Future[ShouldHaveEnded]] = {
        val dslSyntax = Consume.syntax[JsValue]
        import dslSyntax._
        Consume.interpret(
          for {
            ContractDelta(Vector((cid, c)), Vector(), None) <- readOne
            _ = (cid: String) shouldBe (cid1.unwrap: String)
            ctid <- liftF(postArchiveCommand(templateId, cid2, encoder, uri).flatMap {
              case (statusCode, _) =>
                statusCode.isSuccess shouldBe true
                postArchiveCommand(templateId, cid1, encoder, uri).map {
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

            _ = kill.shutdown
            heartbeats <- drain
            hbCount = (heartbeats.iterator.map {
              case ContractDelta(Vector(), Vector(), Some(currentOffset)) => currentOffset
            }.toSet + lastSeenOffset).size - 1

          } yield
          // don't count empty events block if lastSeenOffset does not change
          ShouldHaveEnded(
            liveStartOffset = liveStartOffset,
            msgCount = msgCount + hbCount,
            lastSeenOffset = lastSeenOffset
          ))
      }

      for {
        r1 <- f1
        _ = r1._1 shouldBe 'success
        cid1 = getContractId(getResult(r1._2))

        r2 <- f2
        _ = r2._1 shouldBe 'success
        cid2 = getContractId(getResult(r2._2))

        (kill, source) = singleClientFetchStream(jwt, uri, fetchRequest())
          .viaMat(KillSwitches.single)(Keep.right)
          .preMaterialize

        lastState <- source
          .via(parseResp) runWith resp(cid1, cid2, kill)

        liveOffset = inside(lastState) {
          case ShouldHaveEnded(liveStart, 0, lastSeen) =>
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
              Some(liveOffset)
            )
              via parseResp)
              .take(2)
              .runWith(remainingDeltas)
              .map {
                case (creates, archives, _) =>
                  creates shouldBe empty
                  archives should have size expectArchives
              }
        }

      } yield resumes.foldLeft(1 shouldBe 1)((_, a) => a)
  }

  "fetch multiple keys should work" in withHttpService { (uri, encoder, _) =>
    def create(account: String): Future[domain.ContractId] =
      for {
        r <- postCreateCommand(accountCreateCommand(domain.Party("Alice"), account), encoder, uri)
      } yield {
        assert(r._1.isSuccess)
        getContractId(getResult(r._2))
      }
    def archive(id: domain.ContractId): Future[Assertion] =
      for {
        r <- postArchiveCommand(domain.TemplateId(None, "Account", "Account"), id, encoder, uri)
      } yield {
        assert(r._1.isSuccess)
      }
    val req =
      """
          |[{"templateId": "Account:Account", "key": ["Alice", "abc123"]},
          | {"templateId": "Account:Account", "key": ["Alice", "def456"]}]
          |""".stripMargin
    val futureResults =
      singleClientFetchStream(jwt, uri, req)
        .via(parseResp)
        .filterNot(isOffsetTick)
        .take(4)
        .runWith(Sink.seq[JsValue])

    for {
      cid1 <- create("abc123")
      _ <- create("abc124")
      _ <- create("abc125")
      cid2 <- create("def456")
      _ <- archive(cid2)
      _ <- archive(cid1)
      results <- futureResults
    } yield {
      val expected: Seq[JsValue] = {
        import spray.json._
        Seq(
          """
            |{"events":[{"created":{"payload":{"number":"abc123"}}}]}
            |""".stripMargin.parseJson,
          """
            |{"events":[{"created":{"payload":{"number":"def456"}}}]}
            |""".stripMargin.parseJson,
          """
            |{"events":[{"archived":{}}]}
            |""".stripMargin.parseJson,
          """
            |{"events":[{"archived":{}}]}
            |""".stripMargin.parseJson
        )
      }
      results should matchJsValues(expected)

    }
  }

  "multi-party fetch-by-key query should receive deltas as contracts are archived/created" in withHttpService {
    (uri, encoder, _) =>
      import spray.json._

      val templateId = domain.TemplateId(None, "Account", "Account")

      val f1 =
        postCreateCommand(
          accountCreateCommand(domain.Party("Alice"), "abc123"),
          encoder,
          uri,
          headers = headersWithPartyAuth(List("Alice")))
      val f2 =
        postCreateCommand(
          accountCreateCommand(domain.Party("Bob"), "def456"),
          encoder,
          uri,
          headers = headersWithPartyAuth(List("Bob")))

      val query =
        """[
          {"templateId": "Account:Account", "key": ["Alice", "abc123"]},
          {"templateId": "Account:Account", "key": ["Bob", "def456"]}
        ]"""

      def resp(
          cid1: domain.ContractId,
          cid2: domain.ContractId,
          kill: UniqueKillSwitch): Sink[JsValue, Future[ShouldHaveEnded]] = {
        val dslSyntax = Consume.syntax[JsValue]
        import dslSyntax._
        Consume.interpret(
          for {
            ContractDelta(Vector((account1, _)), Vector(), None) <- readOne
            _ = Seq(cid1, cid2) should contain(account1)
            ContractDelta(Vector((account2, _)), Vector(), None) <- readOne
            _ = Seq(cid1, cid2) should contain(account2)
            ContractDelta(Vector(), _, Some(liveStartOffset)) <- readOne
            _ <- liftF(postArchiveCommand(templateId, cid1, encoder, uri))
            ContractDelta(Vector(), Vector(archivedCid1), Some(_)) <- readOne
            _ = archivedCid1.contractId shouldBe cid1
            _ <- liftF(
              postArchiveCommand(
                templateId,
                cid2,
                encoder,
                uri,
                headers = headersWithPartyAuth(List("Bob"))))
            ContractDelta(Vector(), Vector(archivedCid2), Some(lastSeenOffset)) <- readOne
            _ = archivedCid2.contractId shouldBe cid2
            _ = kill.shutdown
            heartbeats <- drain
            hbCount = (heartbeats.iterator.map {
              case ContractDelta(Vector(), Vector(), Some(currentOffset)) => currentOffset
            }.toSet + lastSeenOffset).size - 1
          } yield
            (
              // don't count empty events block if lastSeenOffset does not change
              ShouldHaveEnded(
                liveStartOffset = liveStartOffset,
                msgCount = 5 + hbCount,
                lastSeenOffset = lastSeenOffset
              )
            ))
      }

      for {
        r1 <- f1
        _ = r1._1 shouldBe 'success
        cid1 = getContractId(getResult(r1._2))

        r2 <- f2
        _ = r2._1 shouldBe 'success
        cid2 = getContractId(getResult(r2._2))

        (kill, source) = singleClientFetchStream(
          jwtForParties(List("Alice", "Bob"), List(), testId),
          uri,
          query
        ).viaMat(KillSwitches.single)(Keep.right).preMaterialize
        lastState <- source via parseResp runWith resp(cid1, cid2, kill)
        liveOffset = inside(lastState) {
          case ShouldHaveEnded(liveStart, 5, lastSeen) =>
            lastSeen.unwrap should be > liveStart.unwrap
            liveStart
        }
        rescan <- (singleClientFetchStream(
          jwtForParties(List("Alice", "Bob"), List(), testId),
          uri,
          query,
          Some(liveOffset)
        )
          via parseResp).take(2) runWith remainingDeltas
      } yield
        inside(rescan) {
          case (Vector(), Vector(_, _), Some(_)) => succeed
        }
  }

  "fetch should should return an error if empty list of (templateId, key) pairs is passed" in withHttpService {
    (uri, _, _) =>
      singleClientFetchStream(jwt, uri, "[]")
        .runWith(collectResultsAsTextMessageSkipOffsetTicks)
        .map { clientMsgs =>
          inside(clientMsgs) {
            case Seq(errorMsg) =>
              val errorResponse = decodeErrorResponse(errorMsg)
              errorResponse.status shouldBe StatusCodes.BadRequest
              inside(errorResponse.errors) {
                case List(error) =>
                  error should include("must be a JSON array with at least 1 element")
              }
          }
        }: Future[Assertion]
  }

  "query on a bunch of random splits should yield consistent results" in withHttpService {
    (uri, _, _) =>
      val splitSample = SplitSeq.gen.map(_ map (BigDecimal(_))).sample.get
      val query =
        """[
          {"templateIds": ["Iou:Iou"]}
        ]"""
      val (kill, source) = singleClientQueryStream(jwt, uri, query)
        .viaMat(KillSwitches.single)(Keep.right)
        .preMaterialize
      source
        .via(parseResp)
        .map(iouSplitResult)
        .filterNot(_ == \/-((Vector(), Vector()))) // liveness marker/heartbeat
        .runWith(Consume.interpret(trialSplitSeq(uri, splitSample, kill)))
  }

  private def trialSplitSeq(
      serviceUri: Uri,
      ss: SplitSeq[BigDecimal],
      kill: UniqueKillSwitch): Consume.FCC[IouSplitResult, Assertion] = {
    val dslSyntax = Consume.syntax[IouSplitResult]
    import SplitSeq._
    import dslSyntax._
    def go(
        createdCid: domain.ContractId,
        ss: SplitSeq[BigDecimal]): Consume.FCC[IouSplitResult, Assertion] = ss match {
      case Leaf(_) =>
        point(1 shouldBe 1)
      case Node(_, l, r) =>
        for {
          (StatusCodes.OK, _) <- liftF(
            postJsonRequest(
              serviceUri.withPath(Uri.Path("/v1/exercise")),
              exercisePayload(createdCid, l.x),
              headersWithAuth))

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
          "issuer" -> "Alice".toJson,
          "amount" -> ss.x.toJson,
          "currency" -> "USD".toJson,
          "owner" -> "Alice".toJson).toJson
      ).toJson
    }
    for {
      (StatusCodes.OK, _) <- liftF(
        postJsonRequest(
          serviceUri.withPath(Uri.Path("/v1/create")),
          initialPayload,
          headersWithAuth))
      \/-((Vector((genesisCid, amt)), Vector())) <- readOne
      _ = amt should ===(ss.x)
      last <- go(genesisCid, ss)
      _ = kill.shutdown
    } yield last
  }

  private def iouSplitResult(jsv: JsValue): IouSplitResult = jsv match {
    case ContractDelta(creates, archives, _) =>
      creates traverse {
        case (cid, JsObject(fields)) =>
          fields get "amount" collect {
            case JsString(amt) => (domain.ContractId(cid), BigDecimal(amt))
          }
        case _ => None
      } map ((_, archives map (_.contractId))) toRightDisjunction jsv
    case _ => -\/(jsv)
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
      input: Source[Message, NotUsed]) =
    Http().singleWebSocketRequest(
      request = WebSocketRequest(uri = uri, subprotocol = subprotocol),
      clientFlow = dummyFlow(input)
    )

  private def assertHeartbeat(str: String): Assertion =
    inside(
      SprayJson
        .decode[EventsBlock](str)) {
      case \/-(eventsBlock) =>
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
    inside(SprayJson.decode[domain.ErrorResponse](str)) {
      case \/-(e) => e
    }
  }

  private def decodeServiceWarning(str: String): domain.ServiceWarning = {
    import json.JsonProtocol._
    inside(SprayJson.decode[domain.AsyncWarningsWrapper](str)) {
      case \/-(w) => w.warnings
    }
  }
}
