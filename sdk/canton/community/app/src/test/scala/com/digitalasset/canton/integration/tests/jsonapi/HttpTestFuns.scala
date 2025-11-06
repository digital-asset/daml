// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.jwt.Jwt
import com.daml.ledger.api.v2.admin.party_management_service.AllocatePartyResponse
import com.digitalasset.canton.config.TlsClientConfig
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.http.json.v2.JsPartyManagementCodecs.*
import com.digitalasset.canton.http.json.v2.js.AllocatePartyRequest as JsAllocatePartyRequest
import com.digitalasset.canton.http.{HttpService, Party, UserId}
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.*
import com.digitalasset.canton.ledger.client.LedgerClient as DamlLedgerClient
import com.google.protobuf.ByteString as ProtoByteString
import io.circe.Decoder
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.ws.{
  Message,
  TextMessage,
  WebSocketRequest,
  WebSocketUpgradeResponse,
}
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.util.ByteString
import spray.json.*

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
trait HttpTestFuns extends HttpJsonApiTestBase with HttpServiceUserFixture {
  import AbstractHttpServiceIntegrationTestFuns.*
  import HttpTestFuns.*

  implicit val ec: ExecutionContext = system.dispatcher

  // Creation of client context is expensive (due to keystore creation - we cache it)
  private val clientConnectionContextMap =
    new TrieMap[TlsClientConfig, HttpsConnectionContext]()

  protected def withHttpService[A](
      token: Option[Jwt] = None,
      participantSelector: FixtureParam => LocalParticipantReference = _.participant1,
  )(
      testFn: HttpServiceTestFixtureData => Future[A]
  ): FixtureParam => A = usingParticipantLedger[A](token map (_.value), participantSelector) {
    case (jsonApiPort, client) =>
      withHttpService[A](
        jsonApiPort,
        client = client,
      )((u, c) => testFn(HttpServiceTestFixtureData(u, c))).futureValue

    case any => throw new IllegalStateException(s"got unexpected $any")
  }

  private def withHttpService[A](
      jsonApiPort: Int,
      client: DamlLedgerClient,
  )(
      testFn: (Uri, DamlLedgerClient) => Future[A]
  ): Future[A] = {
    val scheme = if (useTls) "https" else "http"
    val uri = Uri.from(scheme = scheme, host = "localhost", port = jsonApiPort)
    testFn(uri, client)

  }

  def postJsonRequest(
      uri: Uri,
      json: JsValue,
      headers: List[HttpHeader],
  ): Future[(StatusCode, JsValue)] =
    postJsonStringRequest(uri, json.prettyPrint, headers)

  def postJsonStringRequestEncoded(
      uri: Uri,
      jsonString: String,
      headers: List[HttpHeader],
  ): Future[(StatusCode, String)] = {
    logger.info(s"postJson: ${uri.toString} json: ${jsonString: String}")
    singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = uri,
        headers = headers,
        entity = HttpEntity(ContentTypes.`application/json`, jsonString),
      )
    )
      .flatMap { resp =>
        val bodyF: Future[String] = getResponseDataString(resp, debug = true)
        bodyF.map(body => (resp.status, body))
      }
  }

  def getRequestInternal(uri: Uri, headers: List[HttpHeader]): Future[(StatusCode, JsValue)] =
    getRequestEncoded(uri, headers).map { case (status, body) =>
      (status, body.parseJson)
    }

  def getRequestEncoded(
      uri: Uri,
      headers: List[HttpHeader] = List(),
  ): Future[(StatusCode, String)] =
    singleRequest(
      HttpRequest(method = HttpMethods.GET, uri = uri, headers = headers)
    )
      .flatMap { resp =>
        val bodyF: Future[String] = getResponseDataString(resp, debug = true)
        bodyF.map(body => (resp.status, body))
      }

  def getRequestBinaryData(
      uri: Uri,
      headers: List[HttpHeader] = List(),
  ): Future[(StatusCode, ByteString)] =
    singleRequest(
      HttpRequest(method = HttpMethods.GET, uri = uri, headers = headers)
    ).flatMap { resp =>
      val bodyF = getResponseDataBytes(resp)
      bodyF.map(body => (resp.status, body))
    }

  def postJsonStringRequest(
      uri: Uri,
      jsonString: String,
      headers: List[HttpHeader],
  ): Future[(StatusCode, JsValue)] =
    postJsonStringRequestEncoded(uri, jsonString, headers).map { case (status, body) =>
      (status, body.parseJson)
    }

  def postRequest(
      uri: Uri,
      json: JsValue,
      headers: List[HttpHeader] = Nil,
  ): Future[(StatusCode, JsValue)] =
    singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = uri,
        headers = headers,
        entity = HttpEntity(ContentTypes.`application/json`, json.prettyPrint),
      )
    )
      .flatMap { resp =>
        val bodyF: Future[String] = getResponseDataString(resp, debug = true)
        bodyF.map(body => (resp.status, body.parseJson))
      }

  protected def singleRequest(request: HttpRequest): Future[HttpResponse] = {
    val http = Http()
    http
      .singleRequest(
        request = request,
        connectionContext =
          if (useTls)
            cachedClientContext(clientTlsConfig)
          else
            http.defaultClientHttpsContext,
      )
  }

  protected def websocket(
      uri: Uri,
      jwt: Jwt,
  ): Flow[Message, Message, Future[WebSocketUpgradeResponse]] = {
    val http = Http()
    val scheme = if (useTls) "wss" else "ws"
    http.webSocketClientFlow(
      request = WebSocketRequest(uri.copy(scheme = scheme), subprotocol = validSubprotocol(jwt)),
      connectionContext =
        if (useTls)
          cachedClientContext(clientTlsConfig)
        else
          http.defaultClientHttpsContext,
    )
  }

  protected def httpTestFixture[A](
      f: HttpServiceTestFixtureData => Future[A]
  ): FixtureParam => A = httpTestFixtureForParticipant()(f)

  protected def httpTestFixtureForParticipant[A](
      participantSelector: FixtureParam => LocalParticipantReference = _.participant1
  )(
      f: HttpServiceTestFixtureData => Future[A]
  ): FixtureParam => A =
    withHttpService(None, participantSelector)(f)(_)
  implicit protected final class `AHS Funs Uri functions`(private val self: UriFixture) {

    import self.uri

    def jwt(uri: Uri): Future[Jwt] =
      getUniquePartyTokenUserIdAndAuthHeaders("Alice", uri).map(_._2)

    def getUniquePartyAndAuthHeaders(
        name: String
    ): Future[(Party, List[HttpHeader])] =
      self
        .getUniquePartyTokenUserIdAndAuthHeaders(name)
        .map { case (p, _, _, h) => (p, h) }
        .transform {
          case Success(a) => Success(a)
          case Failure(err) =>
            logger.info(s"err: $err")
            Failure(err)
        }

    def getStream[T](
        path: Uri.Path,
        jwt: Jwt,
        message: TextMessage,
        decoder: String => T = identity[String],
        filter: T => Boolean = { (_: Any) => true },
        maxMessages: Long = 1,
    ): Future[Seq[T]] = {
      val ac = uri.copy(scheme = "ws") withPath path
      val webSocketFlow =
        Http().webSocketClientFlow(
          WebSocketRequest(uri = ac, subprotocol = validSubprotocol(jwt))
        )
      Source
        .single(
          message
        )
        .concatMat(Source.maybe[Message])(Keep.left)
        .via(webSocketFlow)
        .collect { case m: TextMessage =>
          m.getStrictText
        }
        .map(decoder)
        .filter(filter)
        .take(maxMessages)
        .toMat(Sink.seq)(Keep.right)
        .run()
    }

    def getUniquePartyTokenUserIdAndAuthHeaders(
        name: String,
        uriOverride: Uri = uri,
    ): Future[(Party, Jwt, UserId, List[HttpHeader])] = {
      val party = getUniqueParty(name)
      val jsAllocate = JsonParser(
        JsAllocatePartyRequest(partyIdHint = party.toString).asJson
          .toString()
      )
      for {
        newParty <-
          postJsonRequest(
            Uri.Path("/v2/parties"),
            json = jsAllocate,
            headers = headersWithAdminAuth,
          )
            .flatMap {
              case (StatusCodes.OK, result) =>
                decode[AllocatePartyResponse](result.toString()).left
                  .map(_.toString)
                  .flatMap(_.partyDetails.toRight("Missing party details"))
                  .map(_.party)
                  .map(Party.apply) match {
                  case Left(err) => Future.failed(new RuntimeException(err))
                  case Right(party) => Future.successful(party)
                }
              case (status, _) => Future.failed(new RuntimeException(status.value))
            }
        (jwt, userId) <- jwtUserIdForParties(uriOverride)(
          List(newParty),
          List.empty,
          false,
          false,
        )
        headers = authorizationHeader(jwt)
      } yield (newParty, jwt, userId, headers)
    }

    def headersWithAuth: Future[List[HttpHeader]] =
      jwt(uri).map(jwt => authorizationHeader(jwt) ++ getTraceContextHeaders())

    def headersWithPartyAuth(
        actAs: List[Party],
        readAs: List[Party] = List.empty,
        withoutNamespace: Boolean = false,
        admin: Boolean = false,
    )(implicit ec: ExecutionContext): Future[List[HttpHeader]] =
      jwtForParties(uri)(actAs, readAs, withoutNamespace, admin)(ec)
        .map(jwt => authorizationHeader(jwt) ++ getTraceContextHeaders())

    def postJsonStringRequest(
        path: Uri.Path,
        jsonString: String,
    ): Future[(StatusCode, JsValue)] =
      headersWithAuth.flatMap(
        postJsonStringRequest(uri withPath path, jsonString, _)
      )

    def jsonRequest(
        method: HttpMethod,
        path: Uri.Path,
        json: Option[JsValue],
        headers: List[HttpHeader],
    ): Future[(StatusCode, JsValue)] =
      jsonStringRequest(method, uri withPath path, json.map(_.prettyPrint), headers)

    def jsonStringRequest(
        method: HttpMethod,
        uri: Uri,
        jsonString: Option[String],
        headers: List[HttpHeader],
    ): Future[(StatusCode, JsValue)] =
      jsonStringRequestEncoded(method, uri, jsonString, headers).map { case (status, body) =>
        if (status.isSuccess())
          (status, body.parseJson)
        else
          (status, JsObject.empty)
      }

    def jsonStringRequestEncoded(
        method: HttpMethod,
        uri: Uri,
        bodyString: Option[String],
        headers: List[HttpHeader],
    ): Future[(StatusCode, String)] = {
      logger.info(s"HTTP $method to: ${uri.toString} json: $bodyString")
      singleRequest(
        HttpRequest(
          method = method,
          uri = uri,
          headers = headers,
          entity = bodyString
            .map(b => HttpEntity(ContentTypes.`application/json`, b))
            .getOrElse(HttpEntity.Empty),
        )
      )
        .flatMap { resp =>
          val bodyF: Future[String] = getResponseDataString(resp, debug = true)
          bodyF.map(body => (resp.status, body))
        }
    }

    def postJsonRequest(
        path: Uri.Path,
        json: JsValue,
        headers: List[HttpHeader],
    ): Future[(StatusCode, JsValue)] =
      postJsonStringRequest(uri withPath path, json.prettyPrint, headers)

    def postJsonStringRequest(
        uri: Uri,
        jsonString: String,
        headers: List[HttpHeader],
    ): Future[(StatusCode, JsValue)] =
      postJsonStringRequestEncoded(uri, jsonString, headers).map { case (status, body) =>
        (status, body.parseJson)
      }

    def postJsonStringRequestEncoded(
        uri: Uri,
        jsonString: String,
        headers: List[HttpHeader],
    ): Future[(StatusCode, String)] = {
      logger.info(s"postJson: ${uri.toString} json: ${jsonString: String}")
      singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri,
          headers = headers,
          entity = HttpEntity(ContentTypes.`application/json`, jsonString),
        )
      )
        .flatMap { resp =>
          val bodyF: Future[String] = getResponseDataString(resp, debug = true)
          bodyF.map(body => (resp.status, body))
        }
    }

    def postBinaryContent(
        path: Uri.Path,
        body: ProtoByteString,
        headers: List[HttpHeader],
    ): Future[(StatusCode, String)] =
      singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri withPath path,
          headers = headers,
          entity = HttpEntity(ContentTypes.`application/octet-stream`, body.toByteArray),
        )
      )
        .flatMap { resp =>
          val bodyF: Future[String] = getResponseDataString(resp, debug = true)
          bodyF.map(body => (resp.status, body))
        }

    def getRequest(
        path: Uri.Path,
        headers: List[HttpHeader],
    ): Future[(StatusCode, JsValue)] =
      getRequestInternal(uri withPath path, headers)

    def getRequestBinary(
        path: Uri.Path,
        headers: List[HttpHeader],
    ): Future[(StatusCode, ByteString)] =
      getRequestBinaryData(uri withPath path, headers)

    def getRequestWithMinimumAuth_(
        path: Uri.Path
    ): Future[(StatusCode, JsValue)] =
      headersWithAuth
        .flatMap(getRequest(path, _))

    def getRequestWithMinimumAuth[Resp](
        path: Uri.Path
    )(implicit decoder: Decoder[Resp]): Future[Resp] =
      headersWithAuth
        .flatMap(getRequest(path, _))
        .flatMap {
          case (StatusCodes.OK, result) =>
            decode[Resp](result.toString()).left
              .map(_.toString) match {
              case Left(err) => Future.failed(new RuntimeException(err))
              case Right(ok) => Future.successful(ok)
            }
          case (status, _) => Future.failed(new RuntimeException(status.value))
        }

    def getRequestString(
        path: Uri.Path,
        headers: List[HttpHeader],
    ): Future[(StatusCode, String)] =
      getRequestEncoded(uri withPath path, headers)

  }

  private def cachedClientContext(config: TlsClientConfig): HttpsConnectionContext =
    this.clientConnectionContextMap.getOrElseUpdate(config, clientConnectionContext(config))

  protected def clientConnectionContext(config: TlsClientConfig): HttpsConnectionContext =
    ConnectionContext.httpsClient(HttpService.buildSSLContext(config))

}

object HttpTestFuns {
  val tokenPrefix: String = "jwt.token."
  val wsProtocol: String = "daml.ws.auth"

  def validSubprotocol(jwt: Jwt): Option[String] = Option(
    s"""$tokenPrefix${jwt.value},$wsProtocol"""
  )
}
