// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.model.{StatusCode, StatusCodes, Uri}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.daml.http.json.{DomainJsonEncoder, SprayJson}
import com.daml.http.util.TestUtil
import HttpServiceTestFixture.UseTls
import com.typesafe.scalalogging.StrictLogging
import org.scalacheck.Gen
import org.scalatest._
import scalaz.{-\/, \/, \/-}
import scalaz.std.option._
import scalaz.std.vector._
import scalaz.syntax.apply._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._
import spray.json.{JsNull, JsObject, JsString, JsValue}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.NonUnitStatements"))
class WebsocketServiceIntegrationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with StrictLogging
    with AbstractHttpServiceIntegrationTestFuns
    with BeforeAndAfterAll {

  import WebsocketServiceIntegrationTest._
  import WebsocketEndpoints._

  override def jdbcConfig: Option[JdbcConfig] = None

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def useTls = UseTls.NoTls

  private val baseQueryInput: Source[Message, NotUsed] =
    Source.single(TextMessage.Strict("""{"templateIds": ["Account:Account"]}"""))

  private val fetchRequest =
    """[{"templateId": "Account:Account", "key": ["Alice", "abc123"]}]"""

  private val baseFetchInput: Source[Message, NotUsed] =
    Source.single(TextMessage.Strict(fetchRequest))

  private val validSubprotocol = Option(s"""$tokenPrefix${jwt.value},$wsProtocol""")

  List(
    SimpleScenario("query", Uri.Path("/v1/stream/query"), baseQueryInput),
    SimpleScenario("fetch", Uri.Path("/v1/stream/fetch"), baseFetchInput)
  ).foreach { scenario =>
    s"${scenario.id} request with valid protocol token should allow client subscribe to stream" in withHttpService {
      (uri, _, _) =>
        wsConnectRequest(
          uri.copy(scheme = "ws").withPath(scenario.path),
          validSubprotocol,
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
              subprotocol = validSubprotocol))
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
              subprotocol = validSubprotocol))
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

  private val collectResultsAsTextMessageSkipOffsetTicks: Sink[Message, Future[Seq[String]]] =
    Flow[Message]
      .collect { case m: TextMessage => m.getStrictText }
      .filterNot(isOffsetTick)
      .toMat(Sink.seq)(Keep.right)

  private val collectResultsAsTextMessage: Sink[Message, Future[Seq[String]]] =
    Flow[Message]
      .collect { case m: TextMessage => m.getStrictText }
      .toMat(Sink.seq)(Keep.right)

  private def singleClientWSStream(
      path: String,
      serviceUri: Uri,
      query: String,
      offset: Option[domain.Offset]): Source[Message, NotUsed] = {
    import spray.json._, json.JsonProtocol._
    val uri = serviceUri.copy(scheme = "ws").withPath(Uri.Path(s"/v1/stream/$path"))
    logger.info(
      s"---- singleClientWSStream uri: ${uri.toString}, query: $query, offset: ${offset.toString}")
    val webSocketFlow =
      Http().webSocketClientFlow(WebSocketRequest(uri = uri, subprotocol = validSubprotocol))
    offset
      .cata(
        off =>
          Source.fromIterator(() =>
            Seq(Map("offset" -> off.unwrap).toJson.compactPrint, query).iterator),
        Source single query)
      .map(TextMessage(_))
      .via(webSocketFlow)
  }

  private def singleClientQueryStream(
      serviceUri: Uri,
      query: String,
      offset: Option[domain.Offset] = None): Source[Message, NotUsed] =
    singleClientWSStream("query", serviceUri, query, offset)

  private def singleClientFetchStream(
      serviceUri: Uri,
      request: String,
      offset: Option[domain.Offset] = None): Source[Message, NotUsed] =
    singleClientWSStream("fetch", serviceUri, request, offset)

  private def initialIouCreate(serviceUri: Uri) = {
    val payload = TestUtil.readFile("it/iouCreateCommand.json")
    TestUtil.postJsonStringRequest(
      serviceUri.withPath(Uri.Path("/v1/create")),
      payload,
      headersWithAuth)
  }

  private def initialAccountCreate(
      serviceUri: Uri,
      encoder: DomainJsonEncoder): Future[(StatusCode, JsValue)] = {
    val command = accountCreateCommand(domain.Party("Alice"), "abc123")
    postCreateCommand(command, encoder, serviceUri)
  }

  "query endpoint should publish transactions when command create is completed" in withHttpService {
    (uri, _, _) =>
      for {
        _ <- initialIouCreate(uri)

        clientMsg <- singleClientQueryStream(uri, """{"templateIds": ["Iou:Iou"]}""")
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

        clientMsg <- singleClientFetchStream(uri, fetchRequest)
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
        uri,
        """{"templateIds": ["Iou:Iou", "Unknown:Template"]}""")
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
        uri,
        """[{"templateId": "Account:Account", "key": ["Alice", "abc123"]}, {"templateId": "Unknown:Template", "key": ["Alice", "abc123"]}]""")
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
      val clientMsg = singleClientQueryStream(uri, "{}")
        .runWith(collectResultsAsTextMessageSkipOffsetTicks)

      val result = Await.result(clientMsg, 10.seconds)

      result should have size 1
      val errorResponse = decodeErrorResponse(result.head)
      errorResponse.status shouldBe StatusCodes.BadRequest
      errorResponse.errors should have size 1
  }

  "fetch endpoint should send error msg when receiving malformed message" in withHttpService {
    (uri, _, _) =>
      val clientMsg = singleClientFetchStream(uri, """[abcdefg!]""")
        .runWith(collectResultsAsTextMessageSkipOffsetTicks)

      val result = Await.result(clientMsg, 10.seconds)

      result should have size 1
      val errorResponse = decodeErrorResponse(result.head)
      errorResponse.status shouldBe StatusCodes.BadRequest
      errorResponse.errors should have size 1
  }

  // NB SC #3936: the WS connection below terminates at an appropriate time for
  // unknown reasons.  By all appearances, it should not disconnect, and
  // fail for timeout; instead, it receives the correct # of contracts before
  // disconnecting.  The read-write-read test further down demonstrates that this
  // is not a matter of too-strictness; the `case (1` cannot possibly be from the
  // ACS at the time of WS connect, if the test passes.  You can also see, structurally
  // and observing by logging, that withHttpService is not responsible by way of
  // disconnecting. We can see that the stream is truly continuous by testing outside
  // this test code, e.g. in a browser with a JavaScript client, so will leave this
  // mystery to be explored another time.

  private def exercisePayload(cid: domain.ContractId, amount: BigDecimal = BigDecimal("42.42")) = {
    import spray.json._, json.JsonProtocol._
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

      def resp(iouCid: domain.ContractId): Sink[JsValue, Future[ShouldHaveEnded]] = {
        val dslSyntax = Consume.syntax[JsValue]
        import dslSyntax._
        Consume.interpret(
          for {
            ContractDelta(Vector((ctid, _)), Vector(), None) <- readOne
            _ = (ctid: String) shouldBe (iouCid.unwrap: String)
            _ <- liftF(
              TestUtil.postJsonRequest(
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
        lastState <- singleClientQueryStream(uri, query) via parseResp runWith resp(iouCid)
        liveOffset = inside(lastState) {
          case ShouldHaveEnded(liveStart, 2, lastSeen) =>
            lastSeen.unwrap should be > liveStart.unwrap
            liveStart
        }
        rescan <- (singleClientQueryStream(uri, query, Some(liveOffset))
          via parseResp runWith remainingDeltas)
      } yield
        inside(rescan) {
          case (Vector((fstId, fst), (sndId, snd)), Vector(observeConsumed), Some(_)) =>
            Set(fstId, sndId, observeConsumed.contractId) should have size 3
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
          cid2: domain.ContractId): Sink[JsValue, Future[ShouldHaveEnded]] = {
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

        lastState <- singleClientFetchStream(uri, fetchRequest())
          .via(parseResp) runWith resp(cid1, cid2)

        liveOffset = inside(lastState) {
          case ShouldHaveEnded(liveStart, 0, lastSeen) =>
            lastSeen.unwrap should be > liveStart.unwrap
            liveStart
        }

        // check contractIdAtOffsets' effects on phantom filtering
        resumes <- Future.traverse(Seq((None, 2L), (Some(None), 0L), (Some(Some(cid1)), 1L))) {
          case (abcHint, expectArchives) =>
            (singleClientFetchStream(uri, fetchRequest(abcHint), Some(liveOffset))
              via parseResp runWith remainingDeltas)
              .map {
                case (creates, archives, _) =>
                  creates shouldBe empty
                  archives should have size expectArchives
              }
        }

      } yield resumes.foldLeft(1 shouldBe 1)((_, a) => a)
  }

  "fetch should should return an error if empty list of (templateId, key) pairs is passed" in withHttpService {
    (uri, _, _) =>
      singleClientFetchStream(uri, "[]")
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
      singleClientQueryStream(uri, query)
        .via(parseResp)
        .map(iouSplitResult)
        .filterNot(_ == \/-((Vector(), Vector()))) // liveness marker/heartbeat
        .runWith(Consume.interpret(trialSplitSeq(uri, splitSample)))
  }

  private def trialSplitSeq(
      serviceUri: Uri,
      ss: SplitSeq[BigDecimal]): Consume.FCC[IouSplitResult, Assertion] = {
    val dslSyntax = Consume.syntax[IouSplitResult]
    import dslSyntax._, SplitSeq._
    def go(
        createdCid: domain.ContractId,
        ss: SplitSeq[BigDecimal]): Consume.FCC[IouSplitResult, Assertion] = ss match {
      case Leaf(x) =>
        point(1 shouldBe 1)
      case Node(x, l, r) =>
        for {
          (StatusCodes.OK, _) <- liftF(
            TestUtil.postJsonRequest(
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
        TestUtil.postJsonRequest(
          serviceUri.withPath(Uri.Path("/v1/create")),
          initialPayload,
          headersWithAuth))
      \/-((Vector((genesisCid, amt)), Vector())) <- readOne
      _ = amt should ===(ss.x)
      last <- go(genesisCid, ss)
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

  private val parseResp: Flow[Message, JsValue, NotUsed] = {
    import spray.json._
    Flow[Message]
      .mapAsync(1) {
        case _: BinaryMessage => fail("shouldn't get BinaryMessage")
        case tm: TextMessage => tm.toStrict(1.second).map(_.text.parseJson)
      }
      .filter {
        case JsObject(fields) => !(fields contains "heartbeat")
        case _ => true
      }
  }

  private val remainingDeltas: Sink[JsValue, Future[ContractDelta.T]] =
    Sink.fold[ContractDelta.T, JsValue]((Vector.empty, Vector.empty, Option.empty[domain.Offset])) {
      (acc, jsv) =>
        import scalaz.std.tuple._, scalaz.std.vector._, scalaz.syntax.semigroup._
        import domain.Offset.semigroup
        jsv match {
          case ContractDelta(c, a, o) => acc |+| ((c, a, o))
          case _ => acc
        }
    }

  private def assertHeartbeat(str: String): Assertion =
    inside(
      SprayJson
        .decode[EventsBlock](str)) {
      case \/-(eventsBlock) =>
        eventsBlock.events shouldBe Vector.empty[JsValue]
        inside(eventsBlock.offset) {
          case JsString(offset) =>
            offset.length should be > 0
          case JsNull =>
            Succeeded
        }
    }

  private def isOffsetTick(str: String): Boolean =
    SprayJson
      .decode[EventsBlock](str)
      .map { b =>
        val isEmpty: Boolean = (b.events: Vector[JsValue]) == Vector.empty[JsValue]
        val hasOffset: Boolean = b.offset match {
          case JsString(offset) => offset.length > 0
          case JsNull => true
          case _ => false
        }
        isEmpty && hasOffset
      }
      .valueOr(_ => false)

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

object WebsocketServiceIntegrationTest {
  import spray.json._

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def dummyFlow[A](source: Source[A, NotUsed]): Flow[A, A, NotUsed] =
    Flow.fromSinkAndSource(Sink.foreach(println), source)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def foldWhile[S, A, T](zero: S)(f: (S, A) => (S \/ T)): Sink[A, Future[Option[T]]] =
    Flow[A]
      .scan(-\/(zero): S \/ T)((st, a) =>
        st match {
          case -\/(s) => f(s, a)
          case \/-(_) => st
      })
      .collect { case \/-(t) => t }
      .toMat(Sink.headOption)(Keep.right)

  private val contractIdAtOffsetKey = "contractIdAtOffset"

  private case class SimpleScenario(id: String, path: Uri.Path, input: Source[Message, NotUsed])

  private final case class ShouldHaveEnded(
      liveStartOffset: domain.Offset,
      msgCount: Int,
      lastSeenOffset: domain.Offset)

  private object ContractDelta {
    private val tagKeys = Set("created", "archived", "error")
    type T = (Vector[(String, JsValue)], Vector[domain.ArchivedContract], Option[domain.Offset])

    def unapply(
        jsv: JsValue
    ): Option[T] =
      for {
        JsObject(eventsWrapper) <- Some(jsv)
        JsArray(sums) <- eventsWrapper.get("events")
        pairs = sums collect { case JsObject(fields) => fields.filterKeys(tagKeys).head }
        if pairs.length == sums.length
        sets = pairs groupBy (_._1)
        creates = sets.getOrElse("created", Vector()) collect {
          case (_, JsObject(fields)) => fields
        }

        createPairs = creates collect (Function unlift { add =>
          (add get "contractId" collect { case JsString(v) => v }) tuple (add get "payload")
        }): Vector[(String, JsValue)]

        archives = sets.getOrElse("archived", Vector()) collect {
          case (_, adata) =>
            import json.JsonProtocol.ArchivedContractFormat
            adata.convertTo[domain.ArchivedContract]
        }: Vector[domain.ArchivedContract]

        offset = eventsWrapper
          .get("offset")
          .collect { case JsString(str) => domain.Offset(str) }: Option[domain.Offset]

      } yield (createPairs, archives, offset)
  }

  private object IouAmount {
    def unapply(jsv: JsObject): Option[BigDecimal] =
      for {
        JsObject(payload) <- jsv.fields get "payload"
        JsString(amount) <- payload get "amount"
      } yield BigDecimal(amount)
  }

  private object NumList {
    def unapply(jsv: JsValue): Option[Vector[BigDecimal]] =
      for {
        JsArray(numvs) <- Some(jsv)
        nums = numvs collect { case JsNumber(n) => n }
        if numvs.length == nums.length
      } yield nums
  }

  private abstract class JsoField(label: String) {
    def unapply(jsv: JsObject): Option[(JsValue, JsObject)] =
      jsv.fields get label map ((_, JsObject(jsv.fields - label)))
  }

  private object Created extends JsoField("created")
  private object Archived extends JsoField("archived")
  private object MatchedQueries extends JsoField("matchedQueries")

  private final case class EventsBlock(events: Vector[JsValue], offset: JsValue)
  private object EventsBlock {
    import DefaultJsonProtocol._
    implicit val EventsBlockFormat: RootJsonFormat[EventsBlock] = jsonFormat2(EventsBlock.apply)
  }

  type IouSplitResult =
    JsValue \/ (Vector[(domain.ContractId, BigDecimal)], Vector[domain.ContractId])

  sealed abstract class SplitSeq[+X] extends Product with Serializable {
    import SplitSeq._
    def x: X

    def fold[Z](leaf: X => Z, node: (X, Z, Z) => Z): Z = {
      def go(self: SplitSeq[X]): Z = self match {
        case Leaf(x) => leaf(x)
        case Node(x, l, r) => node(x, go(l), go(r))
      }
      go(this)
    }

    def map[B](f: X => B): SplitSeq[B] =
      fold[SplitSeq[B]](x => Leaf(f(x)), (x, l, r) => Node(f(x), l, r))
  }

  object SplitSeq {
    final case class Leaf[+X](x: X) extends SplitSeq[X]
    final case class Node[+X](x: X, l: SplitSeq[X], r: SplitSeq[X]) extends SplitSeq[X]

    type Amount = Long

    val gen: Gen[SplitSeq[Amount]] =
      Gen.posNum[Amount] flatMap (x => Gen.sized(genSplit(x, _)))

    private def genSplit(x: Amount, size: Int): Gen[SplitSeq[Amount]] =
      if (size > 1 && x > 1)
        Gen.frequency(
          (1, Gen const Leaf(x)),
          (8 min size, Gen.chooseNum(1: Amount, x - 1) flatMap { split =>
            Gen zip (genSplit(split, size / 2), genSplit(x - split, size / 2)) map {
              case (l, r) => Node(x, l, r)
            }
          })
        )
      else Gen const Leaf(x)
  }
}
