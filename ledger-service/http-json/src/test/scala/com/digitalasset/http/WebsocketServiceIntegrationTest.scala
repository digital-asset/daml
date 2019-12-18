package com.digitalasset.http

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Await
import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.NonUnitStatements"))
class WebsocketServiceIntegrationTest extends AbstractHttpServiceIntegrationTest
  with BeforeAndAfterAll {

  import WebsocketEndpoints._

  override def jdbcConfig: Option[JdbcConfig] = None

  override def staticContentConfig: Option[StaticContentConfig] = None

  private val baseFlow: Flow[Message, Message, _] = Flow.fromSinkAndSourceMat(
    Sink.foreach(println), Source.single(TextMessage.Strict("{}")))(Keep.left)

  private val validSubprotorol = Option(s"""$tokenPrefix${jwt.value},$wsProtocol""")

  "ws request with valid protocol token should allow client subscribe to stream" in withHttpService {
    (uri, _, _) => {
      wsConnectRequest(
        uri.copy(scheme = "ws").withPath(Uri.Path("/transactions")),
        validSubprotorol,
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

  "checking out flow" in withHttpService {
    (uri, _, _) =>  {
      val webSocketFlow = Http().webSocketClientFlow(
        WebSocketRequest(
          uri = uri.copy(scheme="ws").withPath(Uri.Path("/transactions")),
          subprotocol = validSubprotorol))

      val future = Source.single(TextMessage.Strict("{}"))
        .via(webSocketFlow)
        .runWith(Sink.fold(Seq.empty[String])(_ :+ _.toString))

      val result = Await.result(future, 10.seconds)
      println(s"current result: ${result.toString}")
      assert(result.isEmpty)
    }
  }

  private def wsConnectRequest(uri: Uri, subprotocol: Option[String], flow: Flow[Message, Message, Any]) =
    Http().singleWebSocketRequest(WebSocketRequest(uri = uri, subprotocol = subprotocol), flow)


}
