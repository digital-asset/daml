// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.model.{StatusCode, StatusCodes, Uri}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.digitalasset.http.json.{DomainJsonEncoder, SprayJson}
import com.digitalasset.http.util.TestUtil
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import scalaz.{-\/, \/, \/-}
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.tag._
import scalaz.syntax.std.option._
import spray.json.{JsNull, JsString, JsValue}

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

  private val headersWithAuth = List(Authorization(OAuth2BearerToken(jwt.value)))

  private val baseQueryFlow: Flow[Message, Message, NotUsed] =
    Flow.fromSinkAndSource(Sink.foreach(println), Source.single(TextMessage.Strict("{}")))

  private val fetchRequest =
    """[{"templateId": "Account:Account", "key": ["Alice", "abc123"]}]"""

  private val baseFetchFlow: Flow[Message, Message, NotUsed] =
    Flow.fromSinkAndSource(Sink.foreach(println), Source.single(TextMessage.Strict(fetchRequest)))

  private val validSubprotocol = Option(s"""$tokenPrefix${jwt.value},$wsProtocol""")

  List(
    SimpleScenario("query", Uri.Path("/v1/stream/query"), baseQueryFlow),
    SimpleScenario("fetch", Uri.Path("/v1/stream/fetch"), baseFetchFlow)
  ).foreach { scenario =>
    s"${scenario.id} request with valid protocol token should allow client subscribe to stream" in withHttpService {
      (uri, _, _) =>
        wsConnectRequest(
          uri.copy(scheme = "ws").withPath(scenario.path),
          validSubprotocol,
          scenario.flow)._1 flatMap (x => x.response.status shouldBe StatusCodes.SwitchingProtocols)
    }

    s"${scenario.id} request with invalid protocol token should be denied" in withHttpService {
      (uri, _, _) =>
        wsConnectRequest(
          uri.copy(scheme = "ws").withPath(scenario.path),
          Option("foo"),
          scenario.flow
        )._1 flatMap (x => x.response.status shouldBe StatusCodes.Unauthorized)
    }

    s"${scenario.id} request without protocol token should be denied" in withHttpService {
      (uri, _, _) =>
        wsConnectRequest(
          uri.copy(scheme = "ws").withPath(scenario.path),
          None,
          scenario.flow
        )._1 flatMap (x => x.response.status shouldBe StatusCodes.Unauthorized)
    }
  }

  private val collectResultsAsTextMessageSkipHeartbeats: Sink[Message, Future[Seq[String]]] =
    Flow[Message]
      .collect { case m: TextMessage => m.getStrictText }
      .filterNot(isHeartbeat)
      .toMat(Sink.seq)(Keep.right)

  private val collectResultsAsTextMessage: Sink[Message, Future[Seq[String]]] =
    Flow[Message]
      .collect { case m: TextMessage => m.getStrictText }
      .toMat(Sink.seq)(Keep.right)

  private def singleClientQueryStream(
      serviceUri: Uri,
      query: String,
      offset: Option[domain.Offset] = None): Source[Message, NotUsed] = {
    import spray.json._, json.JsonProtocol._
    val uri = serviceUri.copy(scheme = "ws").withPath(Uri.Path("/v1/stream/query"))
    logger.info(
      s"---- singleClientQueryStream uri: ${uri.toString}, query: $query, offset: ${offset.toString}")
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

  private def singleClientFetchStream(
      serviceUri: Uri,
      request: String): Source[Message, NotUsed] = {
    val uri = serviceUri.copy(scheme = "ws").withPath(Uri.Path("/v1/stream/fetch"))
    logger.info(s"---- singleClientFetchStream uri: ${uri.toString}, request: $request")
    val webSocketFlow =
      Http().webSocketClientFlow(WebSocketRequest(uri = uri, subprotocol = validSubprotocol))
    Source
      .single(TextMessage(request))
      .via(webSocketFlow)
  }

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
        .runWith(collectResultsAsTextMessageSkipHeartbeats)

      val result = Await.result(clientMsg, 10.seconds)

      result should have size 1
      result.head should include("error")
  }

  "fetch endpoint should send error msg when receiving malformed message" in withHttpService {
    (uri, _, _) =>
      val clientMsg = singleClientFetchStream(uri, """[abcdefg!]""")
        .runWith(collectResultsAsTextMessageSkipHeartbeats)

      val result = Await.result(clientMsg, 10.seconds)

      result should have size 1
      result.head should include("""{"error":""")
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

  private val baseExercisePayload = {
    import spray.json._
    """{"templateId": "Iou:Iou",
        "choice": "Iou_Split",
        "argument": {"splitAmount": 42.42}}""".parseJson.asJsObject
  }

  "query should receive deltas as contracts are archived/created" in withHttpService {
    (uri, _, _) =>
      import spray.json._

      val initialCreate = initialIouCreate(uri)
      def exercisePayload(cid: String) =
        baseExercisePayload.copy(
          fields = baseExercisePayload.fields updated ("contractId", JsString(cid)))

      val query =
        """[
          {"templateIds": ["Iou:Iou"], "query": {"amount": {"%lte": 50}}},
          {"templateIds": ["Iou:Iou"], "query": {"amount": {"%gt": 50}}},
          {"templateIds": ["Iou:Iou"]}
        ]"""

      def resp(iouCid: domain.ContractId): Sink[JsValue, Future[StreamState]] =
        Sink
          .foldAsync(NothingYet: StreamState) {
            case (NothingYet, ContractDelta(Vector((ctid, _)), Vector(), None)) =>
              (ctid: String) shouldBe (iouCid.unwrap: String)
              TestUtil.postJsonRequest(
                uri.withPath(Uri.Path("/v1/exercise")),
                exercisePayload(ctid),
                headersWithAuth) map {
                case (statusCode, _) =>
                  statusCode.isSuccess shouldBe true
                  GotAcs(ctid)
              }

            case (GotAcs(ctid), ContractDelta(Vector(), _, Some(offset))) =>
              Future.successful(GotLive(offset, ctid))

            case (
                GotLive(preOffset, consumedCtid),
                evtsWrapper @ ContractDelta(
                  Vector((fstId, fst), (sndId, snd)),
                  Vector(observeConsumed),
                  Some(lastSeenOffset)
                )) =>
              Future {
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
                ShouldHaveEnded(preOffset, 2, lastSeenOffset)
              }

            case (
                ShouldHaveEnded(liveStartOffset, msgCount, lastSeenOffset),
                ContractDelta(Vector(), Vector(), Some(currentOffset))
                ) =>
              Future {
                // don't count empty events block if lastSeenOffset does not change
                ShouldHaveEnded(
                  liveStartOffset = liveStartOffset,
                  msgCount = if (lastSeenOffset == currentOffset) msgCount else msgCount + 1,
                  lastSeenOffset = currentOffset
                )
              }
          }

      for {
        creation <- initialCreate
        _ = creation._1 shouldBe 'success
        iouCid = getContractId(getResult(creation._2))
        lastState <- singleClientQueryStream(uri, query) via parseResp runWith resp(iouCid)
        liveOffset = inside(lastState) {
          case ShouldHaveEnded(liveStart, 2, lastSeen) =>
            import domain.Offset.ordering
            lastSeen should be > liveStart
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
      val fetchRequest = """[{"templateId": "Account:Account", "key": ["Alice", "abc123"]}]"""
      val f1 =
        postCreateCommand(accountCreateCommand(domain.Party("Alice"), "abc123"), encoder, uri)
      val f2 =
        postCreateCommand(accountCreateCommand(domain.Party("Alice"), "def456"), encoder, uri)

      def resp(
          cid1: domain.ContractId,
          cid2: domain.ContractId): Sink[JsValue, Future[StreamState]] =
        Sink.foldAsync(NothingYet: StreamState) {

          case (
              NothingYet,
              ContractDelta(Vector((cid, c)), Vector(), None)
              ) =>
            (cid: String) shouldBe (cid1.unwrap: String)
            postArchiveCommand(templateId, cid2, encoder, uri).flatMap {
              case (statusCode, _) =>
                statusCode.isSuccess shouldBe true
                postArchiveCommand(templateId, cid1, encoder, uri).map {
                  case (statusCode, _) =>
                    statusCode.isSuccess shouldBe true
                    GotAcs(cid)
                }
            }: Future[StreamState]

          case (GotAcs(ctid), ContractDelta(Vector(), _, Some(offset))) =>
            Future.successful(GotLive(offset, ctid))

          case (
              GotLive(off, archivedCid),
              ContractDelta(Vector(), Vector(observeArchivedCid), Some(lastSeenOffset))
              ) =>
            Future {
              (observeArchivedCid.contractId.unwrap: String) shouldBe (archivedCid: String)
              (observeArchivedCid.contractId: domain.ContractId) shouldBe (cid1: domain.ContractId)
              ShouldHaveEnded(off, 0, lastSeenOffset)
            }

          case (
              ShouldHaveEnded(liveStartOffset, msgCount, lastSeenOffset),
              ContractDelta(Vector(), Vector(), Some(currentOffset))
              ) =>
            Future {
              // don't count empty events block if lastSeenOffset does not change
              ShouldHaveEnded(
                liveStartOffset = liveStartOffset,
                msgCount = if (lastSeenOffset == currentOffset) msgCount else msgCount + 1,
                lastSeenOffset = currentOffset
              )
            }
        }

      for {
        r1 <- f1
        _ = r1._1 shouldBe 'success
        cid1 = getContractId(getResult(r1._2))

        r2 <- f2
        _ = r2._1 shouldBe 'success
        cid2 = getContractId(getResult(r2._2))

        lastState <- singleClientFetchStream(uri, fetchRequest).via(parseResp) runWith resp(
          cid1,
          cid2)

      } yield
        inside(lastState) {
          case ShouldHaveEnded(liveStart, 0, lastSeen) =>
            import domain.Offset.ordering
            lastSeen should be > liveStart
        }
  }

  "fetch should receive all contracts when empty request specified" in withHttpService {
    (uri, encoder, _) =>
      val f1 =
        postCreateCommand(accountCreateCommand(domain.Party("Alice"), "abc123"), encoder, uri)
      val f2 =
        postCreateCommand(accountCreateCommand(domain.Party("Alice"), "def456"), encoder, uri)

      for {
        r1 <- f1
        _ = r1._1 shouldBe 'success
        cid1 = getContractId(getResult(r1._2))

        r2 <- f2
        _ = r2._1 shouldBe 'success
        cid2 = getContractId(getResult(r2._2))

        clientMsgs <- singleClientFetchStream(uri, "[]").runWith(
          collectResultsAsTextMessageSkipHeartbeats)
      } yield {
        inside(clientMsgs) {
          case Seq(errorMsg) =>
            // TODO(Leo) #4417: expected behavior is to return all active contracts (???). Make sure it is consistent with stream/query
            //            c1 should include(s""""contractId":"${cid1.unwrap: String}"""")
            //            c2 should include(s""""contractId":"${cid2.unwrap: String}"""")
            errorMsg should include(
              s""""error":"Cannot resolve any templateId from request: List()""")
        }
      }
  }

  "ContractKeyStreamRequest" - {
    import spray.json._, json.JsonProtocol._
    val baseVal =
      domain.EnrichedContractKey(domain.TemplateId(Some("ab"), "cd", "ef"), JsString("42"): JsValue)
    val baseMap = baseVal.toJson.asJsObject.fields
    val offsetHintKey = "offsetHint"
    val withSome = JsObject(baseMap + (offsetHintKey -> JsString("hi")))
    val withNone = JsObject(baseMap + (offsetHintKey -> JsNull))

    "initial JSON reader" - {
      type T = domain.ContractKeyStreamRequest[None.type, JsValue]

      "shares EnrichedContractKey format" in {
        JsObject(baseMap).convertTo[T] should ===(domain.ContractKeyStreamRequest(None, baseVal))
      }

      "errors on offsetHint presence" in {
        a[DeserializationException] shouldBe thrownBy {
          withSome.convertTo[T]
        }
        a[DeserializationException] shouldBe thrownBy {
          withNone.convertTo[T]
        }
      }
    }

    "resuming JSON reader" - {
      type T = domain.ContractKeyStreamRequest[Option[Option[domain.Offset]], JsValue]

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
      flow: Flow[Message, Message, M]) =
    Http().singleWebSocketRequest(WebSocketRequest(uri = uri, subprotocol = subprotocol), flow)

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

  private def isHeartbeat(str: String): Boolean =
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
}

object WebsocketServiceIntegrationTest {
  import spray.json._

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

  private case class SimpleScenario(
      id: String,
      path: Uri.Path,
      flow: Flow[Message, Message, NotUsed])

  private sealed abstract class StreamState extends Product with Serializable
  private case object NothingYet extends StreamState
  private final case class GotAcs(firstCid: String) extends StreamState
  private final case class GotLive(liveStartOff: domain.Offset, firstCid: String)
      extends StreamState
  private final case class ShouldHaveEnded(
      liveStartOffset: domain.Offset,
      msgCount: Int,
      lastSeenOffset: domain.Offset)
      extends StreamState

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
}
