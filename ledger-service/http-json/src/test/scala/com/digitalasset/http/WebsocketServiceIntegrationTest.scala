// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.digitalasset.http.util.TestUtil
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{Assertion, AsyncFreeSpec, BeforeAndAfterAll, Inside, Matchers}
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

  "websocket should publish transactions when command create is completed" in withHttpService {
    (uri, _, _) =>
      val payload = TestUtil.readFile("it/iouCreateCommand.json")
      TestUtil.postJsonStringRequest(
        uri.withPath(Uri.Path("/command/create")),
        payload,
        headersWithAuth)

      val webSocketFlow = Http().webSocketClientFlow(
        WebSocketRequest(
          uri = uri.copy(scheme = "ws").withPath(Uri.Path("/contracts/searchForever")),
          subprotocol = validSubprotocol))

      val clientMsg = Source
        .single(TextMessage("""{"%templates": [{"moduleName": "Iou", "entityName": "Iou"}]}"""))
        .via(webSocketFlow)
        .runWith(Sink.fold(Seq.empty[String])(_ :+ _.toString))

      clientMsg map {
        inside(_) {
          case Seq(result) =>
            result should include("\"issuer\":\"Alice\"")
        }
      }
  }

  "websocket should send error msg when receiving malformed message" in withHttpService {
    (uri, _, _) =>
      val webSocketFlow = Http().webSocketClientFlow(
        WebSocketRequest(
          uri = uri.copy(scheme = "ws").withPath(Uri.Path("/contracts/searchForever")),
          subprotocol = validSubprotocol))

      val clientMsg = Source
        .single(TextMessage("{}"))
        .via(webSocketFlow)
        .runWith(Sink.fold(Seq.empty[String])(_ :+ _.toString))

      val result = Await.result(clientMsg, 10.seconds)

      result should have size 1
      result.head should include("error")
  }

  "websocket should receive deltas as contracts are archived/created" in withHttpService {
    (uri, _, _) =>
      import spray.json._
      object ContractDelta {
        def unapply(jsv: JsValue): Option[(Vector[(String, JsValue)], Vector[String])] =
          for {
            JsObject(fields) <- Some(jsv)
            JsArray(adds) <- fields get "add" orElse Some(JsArray())
            JsArray(removes) <- fields get "remove" orElse Some(JsArray())
          } yield
            (adds collect (Function unlift {
              case JsObject(add) =>
                (add get "contractId" collect { case JsString(v) => v }) tuple (add get "argument")
              case _ => None
            }), removes collect { case JsString(v) => v })
      }

      val webSocketFlow = Http().webSocketClientFlow(
        WebSocketRequest(
          uri = uri.copy(scheme = "ws").withPath(Uri.Path("/contracts/searchForever")),
          subprotocol = validSubprotocol))

      val query =
        TextMessage.Strict("""{"%templates": [{"moduleName": "Iou", "entityName": "Iou"}]}""")

      val parseResp: Flow[Message, JsValue, NotUsed] =
        Flow[Message].mapAsync(1) {
          case _: BinaryMessage => fail("shouldn't get BinaryMessage")
          case tm: TextMessage => tm.toStrict(1.second).map(_.text.parseJson)
        }

      val resp: Sink[JsValue, Future[Assertion]] = Sink
        .foldAsync[(Int, Option[String]), JsValue]((0, None)) {
          case ((0, _), ContractDelta(Vector((ctid, ct)), _)) =>
            (Future(sys.error("TODO submit exercise"): Unit) map { _ =>
              (1, Some(ctid))
            })
          case (
              (1, Some(consumedCtid)),
              ContractDelta(Vector((fstId, fst), (sndId, snd)), Vector(observeConsumed))) =>
            Future {
              observeConsumed should ===(consumedCtid)
              (2, None)
            }
        }
        .mapMaterializedValue {
          _ map {
            case (count, notYetConsumed) =>
              count should ===(2)
              notYetConsumed should ===(None)
          }
        }

      Source single query via webSocketFlow via parseResp runWith resp
  }

  private def wsConnectRequest[M](
      uri: Uri,
      subprotocol: Option[String],
      flow: Flow[Message, Message, M]) =
    Http().singleWebSocketRequest(WebSocketRequest(uri = uri, subprotocol = subprotocol), flow)
}
