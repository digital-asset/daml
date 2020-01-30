// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.digitalasset.http.util.TestUtil
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{AsyncFreeSpec, BeforeAndAfterAll, Inside, Matchers}
import scalaz.std.option._
import scalaz.syntax.apply._

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

  private val baseFlow: Flow[Message, Message, NotUsed] =
    Flow.fromSinkAndSource(Sink.foreach(println), Source.single(TextMessage.Strict("{}")))

  private val validSubprotocol = Option(s"""$tokenPrefix${jwt.value},$wsProtocol""")

  "ws request with valid protocol token should allow client subscribe to stream" in withHttpService {
    (uri, _, _) =>
      wsConnectRequest(
        uri.copy(scheme = "ws").withPath(Uri.Path("/contracts/searchForever")),
        validSubprotocol,
        baseFlow)._1 flatMap (x => x.response.status shouldBe StatusCodes.SwitchingProtocols)
  }

  "ws request with invalid protocol token should be denied" in withHttpService { (uri, _, _) =>
    wsConnectRequest(
      uri.copy(scheme = "ws").withPath(Uri.Path("/contracts/searchForever")),
      Option("foo"),
      baseFlow
    )._1 flatMap (x => x.response.status shouldBe StatusCodes.Unauthorized)
  }

  "ws request without protocol token should be denied" in withHttpService { (uri, _, _) =>
    wsConnectRequest(
      uri.copy(scheme = "ws").withPath(Uri.Path("/contracts/searchForever")),
      None,
      baseFlow
    )._1 flatMap (x => x.response.status shouldBe StatusCodes.Unauthorized)
  }

  private val collectResultsAsRawString: Sink[Message, Future[Seq[String]]] =
    Flow[Message].map(_.toString).filter(v => !(v contains "heartbeat")).toMat(Sink.seq)(Keep.right)

  private def singleClientStream(serviceUri: Uri, query: String) = {
    val webSocketFlow = Http().webSocketClientFlow(
      WebSocketRequest(
        uri = serviceUri.copy(scheme = "ws").withPath(Uri.Path("/contracts/searchForever")),
        subprotocol = validSubprotocol))
    Source
      .single(TextMessage(query))
      .via(webSocketFlow)
  }

  private def initialIouCreate(serviceUri: Uri) = {
    val payload = TestUtil.readFile("it/iouCreateCommand.json")
    TestUtil.postJsonStringRequest(
      serviceUri.withPath(Uri.Path("/command/create")),
      payload,
      headersWithAuth)
  }

  "websocket should publish transactions when command create is completed" in withHttpService {
    (uri, _, _) =>
      for {
        _ <- initialIouCreate(uri)

        clientMsg <- singleClientStream(uri, """{"templateIds": ["Iou:Iou"]}""")
          .runWith(collectResultsAsRawString)
      } yield
        inside(clientMsg) {
          case Seq(result) =>
            result should include("\"issuer\":\"Alice\"")
        }
  }

  "websocket should warn on unknown template IDs" in withHttpService { (uri, _, _) =>
    for {
      _ <- initialIouCreate(uri)

      clientMsg <- singleClientStream(uri, """{"templateIds": ["Iou:Iou", "Unknown:Template"]}""")
        .runWith(collectResultsAsRawString)
    } yield
      inside(clientMsg) {
        case Seq(warning, result) =>
          warning should include("\"warnings\":{\"unknownTemplateIds\":[\"Unk")
          result should include("\"issuer\":\"Alice\"")
      }
  }

  "websocket should send error msg when receiving malformed message" in withHttpService {
    (uri, _, _) =>
      val clientMsg = singleClientStream(uri, "{}")
        .runWith(collectResultsAsRawString)

      val result = Await.result(clientMsg, 10.seconds)

      result should have size 1
      result.head should include("error")
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

  "websocket should receive deltas as contracts are archived/created" in withHttpService {
    (uri, _, _) =>
      import spray.json._

      val initialCreate = initialIouCreate(uri)
      def exercisePayload(cid: String) =
        baseExercisePayload.copy(
          fields = baseExercisePayload.fields updated ("contractId", JsString(cid)))

      val query = """{"templateIds": ["Iou:Iou"]}"""

      val parseResp: Flow[Message, JsValue, NotUsed] =
        Flow[Message]
          .mapAsync(1) {
            case _: BinaryMessage => fail("shouldn't get BinaryMessage")
            case tm: TextMessage => tm.toStrict(1.second).map(_.text.parseJson)
          }
          .filter {
            case JsObject(fields) => !(fields contains "heartbeat")
            case _ => true
          }

      sealed abstract class StreamState extends Product with Serializable
      case object NothingYet extends StreamState
      final case class GotAcs(firstCid: String) extends StreamState
      final case class ShouldHaveEnded(msgCount: Int) extends StreamState

      val resp: Sink[JsValue, Future[StreamState]] = Sink
        .foldAsync(NothingYet: StreamState) {
          case (NothingYet, ContractDelta(Vector((ctid, ct)), Vector())) =>
            TestUtil.postJsonRequest(
              uri.withPath(Uri.Path("/command/exercise")),
              exercisePayload(ctid),
              headersWithAuth) map {
              case (statusCode, respBody) =>
                statusCode.isSuccess shouldBe true
                GotAcs(ctid)
            }
          case (
              GotAcs(consumedCtid),
              ContractDelta(Vector((fstId, fst), (sndId, snd)), Vector(observeConsumed))) =>
            Future {
              observeConsumed should ===(consumedCtid)
              Set(fstId, sndId, consumedCtid) should have size 3
              ShouldHaveEnded(2)
            }
        }

      for {
        creation <- initialCreate
        _ = creation._1 shouldBe 'success
        lastState <- singleClientStream(uri, query) via parseResp runWith resp
      } yield lastState should ===(ShouldHaveEnded(2))
  }

  private def wsConnectRequest[M](
      uri: Uri,
      subprotocol: Option[String],
      flow: Flow[Message, Message, M]) =
    Http().singleWebSocketRequest(WebSocketRequest(uri = uri, subprotocol = subprotocol), flow)
}

object WebsocketServiceIntegrationTest {
  private object ContractDelta {
    import spray.json._
    def unapply(jsv: JsValue): Option[(Vector[(String, JsValue)], Vector[String])] =
      for {
        JsArray(sums) <- Some(jsv)
        pairs = sums collect { case JsObject(fields) if fields.size == 1 => fields.head }
        if pairs.length == sums.length
        sets = pairs groupBy (_._1)
        if sets.keySet subsetOf Set("created", "archived", "error")
        creates = sets.getOrElse("created", Vector()) collect {
          case (_, JsObject(fields)) => fields
        }
      } yield
        (creates collect (Function unlift { add =>
          (add get "contractId" collect { case JsString(v) => v }) tuple (add get "payload")
        }), sets.getOrElse("archived", Vector()) collect { case (_, JsString(cid)) => cid })
  }
}
