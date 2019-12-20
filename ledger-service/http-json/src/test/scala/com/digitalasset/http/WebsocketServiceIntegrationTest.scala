package com.digitalasset.http

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.digitalasset.http.util.TestUtil
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Await
import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.NonUnitStatements"))
class WebsocketServiceIntegrationTest extends AbstractHttpServiceIntegrationTest
  with BeforeAndAfterAll {

  import WebsocketEndpoints._

  override def jdbcConfig: Option[JdbcConfig] = None

  override def staticContentConfig: Option[StaticContentConfig] = None

  private val headersWithAuth = List(Authorization(OAuth2BearerToken(jwt.value)))

  private val baseFlow: Flow[Message, Message, NotUsed] = Flow.fromSinkAndSource(
    Sink.foreach(println), Source.single(TextMessage.Strict("{}")))

  private val validSubprotocol = Option(s"""$tokenPrefix${jwt.value},$wsProtocol""")

  "ws request with valid protocol token should allow client subscribe to stream" in withHttpService {
    (uri, _, _) => {
      wsConnectRequest(
        uri.copy(scheme = "ws").withPath(Uri.Path("/transactions")),
        validSubprotocol,
        baseFlow)
        ._1 flatMap (x => x.response.status shouldBe StatusCodes.SwitchingProtocols)
    }
  }

  "ws request with invalid protocol token should be denied" in withHttpService {
    (uri, _, _) => {
      wsConnectRequest(
          uri.copy(scheme="ws").withPath(Uri.Path("/transactions")),
        Option("foo"),
        baseFlow
      )._1 flatMap (x => x.response.status shouldBe StatusCodes.Unauthorized)
    }
  }

  "ws request without protocol token should be denied" in withHttpService {
    (uri, _, _) => {
      wsConnectRequest(
        uri.copy(scheme="ws").withPath(Uri.Path("/transactions")),
        None,
        baseFlow
      )._1 flatMap (x => x.response.status shouldBe StatusCodes.Unauthorized)
    }
  }

  "websocket should publish transactions when command create is completed" in withHttpService {
    (uri, _, _) =>  {
      val payload = TestUtil.readFile("it/iouCreateCommand.json")
      TestUtil.postJsonStringRequest(uri.withPath(Uri.Path("/command/create")),
        payload, headersWithAuth)

      val webSocketFlow = Http().webSocketClientFlow(
        WebSocketRequest(
          uri = uri.copy(scheme="ws").withPath(Uri.Path("/transactions")),
          subprotocol = validSubprotocol))

      val clientMsg = Source.single(TextMessage("{}"))
        .via(webSocketFlow)
        .runWith(Sink.fold(Seq.empty[String])(_ :+ _.toString))

      val result = Await.result(clientMsg, 10.seconds)
      assert(result.nonEmpty)
      result.size shouldBe 1
    }
  }


  "websocket should send error msg when receiving malformed message" in withHttpService {
    (uri, _, _) =>  {
      val webSocketFlow = Http().webSocketClientFlow(
        WebSocketRequest(
          uri = uri.copy(scheme="ws").withPath(Uri.Path("/transactions")),
          subprotocol = validSubprotocol))

      val clientMsg = Source.single(TextMessage("pie"))
        .via(webSocketFlow)
        .runWith(Sink.fold(Seq.empty[String])(_ :+ _.toString))

      val result = Await.result(clientMsg, 10.seconds)

      assert(result.nonEmpty)
      result.size shouldBe 1
      assert(result.head.contains("error"))
    }
  }

  private def wsConnectRequest[M](uri: Uri, subprotocol: Option[String], flow: Flow[Message, Message, M]) =
    Http().singleWebSocketRequest(WebSocketRequest(uri = uri, subprotocol = subprotocol), flow)
}
