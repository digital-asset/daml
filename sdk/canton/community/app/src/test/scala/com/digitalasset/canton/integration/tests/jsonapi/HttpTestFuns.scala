// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.jwt.Jwt
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.config.TlsClientConfig
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.http.json.*
import com.digitalasset.canton.http.json.SprayJson.decode1
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, instanceUUIDLogCtx}
import com.digitalasset.canton.http.{
  AllocatePartyRequest as HttpAllocatePartyRequest,
  HttpService,
  OkResponse,
  Party,
  PartyDetails as HttpPartyDetails,
  SyncResponse,
  UserId,
}
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.*
import com.digitalasset.canton.integration.tests.jsonapi.WebsocketTestFixture.validSubprotocol
import com.digitalasset.canton.ledger.client.LedgerClient as DamlLedgerClient
import com.google.protobuf.ByteString as ProtoByteString
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
import scalaz.syntax.show.*
import spray.json.*

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
trait HttpTestFuns extends HttpJsonApiTestBase with HttpServiceUserFixture {
  import AbstractHttpServiceIntegrationTestFuns.*
  import JsonProtocol.*

  implicit val ec: ExecutionContext = system.dispatcher

  // Creation of client context is expensive (due to keystore creation - we cache it)
  private val clientConnectionContextMap =
    new TrieMap[TlsClientConfig, HttpsConnectionContext]()

  protected def withHttpServiceAndClient[A](
      testFn: (Uri, ApiJsonEncoder, ApiJsonDecoder, DamlLedgerClient) => Future[A]
  ): FixtureParam => A =
    withHttpService() { case HttpServiceTestFixtureData(a, b, c, d) => testFn(a, b, c, d) }

  protected def withHttpService[A](
      token: Option[Jwt] = None,
      participantSelector: FixtureParam => LocalParticipantReference = _.participant1,
  )(
      testFn: HttpServiceTestFixtureData => Future[A]
  ): FixtureParam => A = usingParticipantLedger[A](token map (_.value), participantSelector) {
    case (jsonApiPort, client) =>
      withHttpService[A](
        jsonApiPort,
        token = token orElse Some(jwtAdminNoParty),
        client = client,
      )((u, e, d, c) => testFn(HttpServiceTestFixtureData(u, e, d, c))).futureValue

    case any => throw new IllegalStateException(s"got unexpected $any")
  }

  protected def withHttpServiceAndClient[A](token: Jwt)(
      testFn: (Uri, ApiJsonEncoder, ApiJsonDecoder, DamlLedgerClient) => Future[A]
  ): FixtureParam => A = usingLedger[A](Some(token.value)) { case (jsonApiPort, client) =>
    withHttpService[A](
      jsonApiPort,
      token = Some(token),
      client = client,
    )(testFn(_, _, _, _)).futureValue
  }

  protected def withHttpService[A](
      f: (Uri, ApiJsonEncoder, ApiJsonDecoder) => Future[A]
  ): FixtureParam => A =
    withHttpServiceAndClient((a, b, c, _) => f(a, b, c))(_)

  private def withHttpService[A](
      jsonApiPort: Int,
      client: DamlLedgerClient,
      token: Option[Jwt],
  )(
      testFn: (Uri, ApiJsonEncoder, ApiJsonDecoder, DamlLedgerClient) => Future[A]
  ): Future[A] = {
    implicit val lc: LoggingContextOf[InstanceUUID] = instanceUUIDLogCtx(identity)
    val scheme = if (useTls) "https" else "http"

    for {
      codecs <- jsonCodecs(client, token)
      uri = Uri.from(scheme = scheme, host = "localhost", port = jsonApiPort)
      (encoder, decoder) = codecs
      a <- testFn(uri, encoder, decoder, client)
    } yield a
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

  protected def withHttpServiceOnly[A](jsonApiPort: Int, client: DamlLedgerClient)(
      f: HttpServiceOnlyTestFixtureData => Future[A]
  ): A =
    withHttpService[A](
      jsonApiPort,
      token = Some(jwtAdminNoParty),
      client = client,
    )((uri, encoder, decoder, _) =>
      f(HttpServiceOnlyTestFixtureData(uri, encoder, decoder))
    ).futureValue
  implicit protected final class `AHS Funs Uri functions`(private val self: UriFixture) {

    import self.uri

    def jwt(uri: Uri): Future[Jwt] =
      getUniquePartyTokenUserIdAndAuthHeaders("Alice", uri).map(_._2)

    def getUniquePartyAndAuthHeaders(
        name: String
    ): Future[(Party, List[HttpHeader])] =
      self.getUniquePartyTokenUserIdAndAuthHeaders(name).map { case (p, _, _, h) => (p, h) }

    def postJsonRequestWithMinimumAuth[Result: JsonReader](
        path: Uri.Path,
        json: JsValue,
    ): Future[SyncResponse[Result]] =
      headersWithAuth
        .flatMap(postJsonRequest(path, json, _))
        .parseResponse[Result]

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
      val request = HttpAllocatePartyRequest(
        Some(party),
        None,
      )
      val json = SprayJson.encode(request).valueOr(e => fail(e.shows))
      for {
        OkResponse(newParty, _, StatusCodes.OK) <-
          postJsonRequest(
            Uri.Path("/v1/parties/allocate"),
            json = json,
            headers = headersWithAdminAuth,
          )
            .parseResponse[HttpPartyDetails]
        (jwt, userId) <- jwtUserIdForParties(uriOverride)(
          List(newParty.identifier),
          List.empty,
          false,
          false,
        )
        headers = authorizationHeader(jwt)
      } yield (newParty.identifier, jwt, userId, headers)
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

    def getRequestWithMinimumAuth[Result: JsonReader](
        path: Uri.Path
    ): Future[SyncResponse[Result]] =
      headersWithAuth
        .flatMap(getRequest(path, _))
        .parseResponse[Result]

    def getRequestString(
        path: Uri.Path,
        headers: List[HttpHeader],
    ): Future[(StatusCode, String)] =
      getRequestEncoded(uri withPath path, headers)

  }

  implicit protected final class `Future JsValue functions`(
      private val self: Future[(StatusCode, JsValue)]
  ) {
    def parseResponse[Result: JsonReader]: Future[SyncResponse[Result]] =
      self.map { case (status, jsv) =>
        val r = decode1[SyncResponse, Result](jsv).fold(e => fail(e.shows), identity)
        r.status should ===(status)
        r
      }
  }

  private def cachedClientContext(config: TlsClientConfig): HttpsConnectionContext =
    this.clientConnectionContextMap.getOrElseUpdate(config, clientConnectionContext(config))

  protected def clientConnectionContext(config: TlsClientConfig): HttpsConnectionContext =
    ConnectionContext.httpsClient(HttpService.buildSSLContext(config))

}
