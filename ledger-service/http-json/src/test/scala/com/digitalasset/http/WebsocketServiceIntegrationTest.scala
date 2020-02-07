// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.model.{StatusCode, StatusCodes, Uri}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.digitalasset.http.json.DomainJsonEncoder
import com.digitalasset.http.util.TestUtil
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{AsyncFreeSpec, BeforeAndAfterAll, Inside, Matchers}
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.tag._
import spray.json.JsValue

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

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

  private val collectResultsAsRawString: Sink[Message, Future[Seq[String]]] =
    Flow[Message].map(_.toString).filter(v => !(v contains "heartbeat")).toMat(Sink.seq)(Keep.right)

  private def singleClientQueryStream(serviceUri: Uri, query: String) = {
    val webSocketFlow = Http().webSocketClientFlow(
      WebSocketRequest(
        uri = serviceUri.copy(scheme = "ws").withPath(Uri.Path("/v1/stream/query")),
        subprotocol = validSubprotocol))
    Source
      .single(TextMessage(query))
      .via(webSocketFlow)
  }

  private def singleClientFetchStream(serviceUri: Uri, request: String) = {
    val webSocketFlow = Http().webSocketClientFlow(
      WebSocketRequest(
        uri = serviceUri.copy(scheme = "ws").withPath(Uri.Path("/v1/stream/fetch")),
        subprotocol = validSubprotocol))
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
          .runWith(collectResultsAsRawString)
      } yield
        inside(clientMsg) {
          case Seq(result) =>
            result should include(""""issuer":"Alice"""")
            result should include(""""amount":"999.99"""")
        }
  }

  "fetch endpoint should publish transactions when command create is completed" in withHttpService {
    (uri, encoder, _) =>
      for {
        _ <- initialAccountCreate(uri, encoder)

        clientMsg <- singleClientFetchStream(uri, fetchRequest)
          .runWith(collectResultsAsRawString)
      } yield
        inside(clientMsg) {
          case Seq(result) =>
            result should include(""""owner":"Alice"""")
            result should include(""""number":"abc123"""")
        }
  }

  "query endpoint should warn on unknown template IDs" in withHttpService { (uri, _, _) =>
    for {
      _ <- initialIouCreate(uri)

      clientMsg <- singleClientQueryStream(
        uri,
        """{"templateIds": ["Iou:Iou", "Unknown:Template"]}""")
        .runWith(collectResultsAsRawString)
    } yield
      inside(clientMsg) {
        case Seq(warning, result) =>
          warning should include("\"warnings\":{\"unknownTemplateIds\":[\"Unk")
          result should include("\"issuer\":\"Alice\"")
      }
  }

  "fetch endpoint should warn on unknown template IDs" in withHttpService { (uri, encoder, _) =>
    for {
      _ <- initialAccountCreate(uri, encoder)

      clientMsg <- singleClientFetchStream(
        uri,
        """[{"templateId": "Account:Account", "key": ["Alice", "abc123"]}, {"templateId": "Unknown:Template", "key": ["Alice", "abc123"]}]""")
        .runWith(collectResultsAsRawString)
    } yield
      inside(clientMsg) {
        case Seq(warning, result) =>
          warning should include("""{"warnings":{"unknownTemplateIds":["Unk""")
          result should include(""""owner":"Alice"""")
          result should include(""""number":"abc123"""")
      }
  }

  "query endpoint should send error msg when receiving malformed message" in withHttpService {
    (uri, _, _) =>
      val clientMsg = singleClientQueryStream(uri, "{}")
        .runWith(collectResultsAsRawString)

      val result = Await.result(clientMsg, 10.seconds)

      result should have size 1
      result.head should include("error")
  }

  "fetch endpoint should send error msg when receiving malformed message" in withHttpService {
    (uri, _, _) =>
      val clientMsg = singleClientFetchStream(uri, """[abcdefg!]""")
        .runWith(collectResultsAsRawString)

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
            case (NothingYet, ContractDelta(Vector((ctid, ct)), Vector())) =>
              (ctid: String) shouldBe (iouCid.unwrap: String)
              TestUtil.postJsonRequest(
                uri.withPath(Uri.Path("/v1/exercise")),
                exercisePayload(ctid),
                headersWithAuth) map {
                case (statusCode, respBody) =>
                  statusCode.isSuccess shouldBe true
                  GotAcs(ctid)
              }
            case (
                GotAcs(consumedCtid),
                evts @ ContractDelta(
                  Vector((fstId, fst), (sndId, snd)),
                  Vector(observeConsumed))) =>
              Future {
                observeConsumed should ===(consumedCtid)
                Set(fstId, sndId, consumedCtid) should have size 3
                inside(evts) {
                  case JsArray(
                      Vector(
                        Archived(_, _),
                        Created(IouAmount(amt1), MatchedQueries(NumList(ixes1), _)),
                        Created(IouAmount(amt2), MatchedQueries(NumList(ixes2), _)))) =>
                    Set((amt1, ixes1), (amt2, ixes2)) should ===(
                      Set(
                        (BigDecimal("42.42"), Vector(BigDecimal(0), BigDecimal(2))),
                        (BigDecimal("957.57"), Vector(BigDecimal(1), BigDecimal(2))),
                      ))
                }
                ShouldHaveEnded(2)
              }
          }

      for {
        creation <- initialCreate
        _ = creation._1 shouldBe 'success
        iouCid = getContractId(getResult(creation._2))
        lastState <- singleClientQueryStream(uri, query) via parseResp runWith resp(iouCid)
      } yield lastState should ===(ShouldHaveEnded(2))
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
              ContractDelta(Vector((cid, c)), Vector())
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

          case (
              GotAcs(archivedCid),
              ContractDelta(Vector(), Vector(observeArchivedCid))
              ) =>
            Future {
              (observeArchivedCid: String) shouldBe (archivedCid: String)
              (observeArchivedCid: String) shouldBe (cid1.unwrap: String)
              ShouldHaveEnded(0)
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

      } yield lastState shouldBe ShouldHaveEnded(0)
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

        clientMsgs <- singleClientFetchStream(uri, "[]").runWith(collectResultsAsRawString)
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

  private def wsConnectRequest[M](
      uri: Uri,
      subprotocol: Option[String],
      flow: Flow[Message, Message, M]) =
    Http().singleWebSocketRequest(WebSocketRequest(uri = uri, subprotocol = subprotocol), flow)

  val parseResp: Flow[Message, JsValue, NotUsed] = {
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
}

object WebsocketServiceIntegrationTest {
  import spray.json._

  private case class SimpleScenario(
      id: String,
      path: Uri.Path,
      flow: Flow[Message, Message, NotUsed])

  private sealed abstract class StreamState extends Product with Serializable
  private case object NothingYet extends StreamState
  private final case class GotAcs(firstCid: String) extends StreamState
  private final case class ShouldHaveEnded(msgCount: Int) extends StreamState

  private object ContractDelta {
    private val tagKeys = Set("created", "archived", "error")
    def unapply(jsv: JsValue): Option[(Vector[(String, JsValue)], Vector[String])] =
      for {
        JsArray(sums) <- Some(jsv)
        pairs = sums collect { case JsObject(fields) => fields.filterKeys(tagKeys).head }
        if pairs.length == sums.length
        sets = pairs groupBy (_._1)
        creates = sets.getOrElse("created", Vector()) collect {
          case (_, JsObject(fields)) => fields
        }
      } yield
        (creates collect (Function unlift { add =>
          (add get "contractId" collect { case JsString(v) => v }) tuple (add get "payload")
        }), sets.getOrElse("archived", Vector()) collect { case (_, JsString(cid)) => cid })
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
}
