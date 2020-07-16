package com.daml.lf.engine.trigger

import akka.actor.typed.scaladsl.ActorContext
import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.stream.Materializer
import akka.util.ByteString
import com.daml.grpc.adapter.ExecutionSequencerFactory
import spray.json.{JsObject, JsValue}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

// OAuth2 bearer token for requests to authentication service
case class AuthServiceToken(token: String)

class AuthServiceClient(
    secretKey: SecretKey
)(implicit ctx: ActorContext[Message], materializer: Materializer, esf: ExecutionSequencerFactory) {

  implicit val ec: ExecutionContext = ctx.system.executionContext

  val http: HttpExt = Http(ctx.system)

  def responseBodyToString(resp: HttpResponse): Future[String] = {
    resp.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
  }

  // Extract a field from the JSON object in an HTTP response.
  def parseResult(response: HttpResponse, field: String): Future[Option[JsValue]] = {
    responseBodyToString(response) map { body =>
      body.parseJson match {
        case JsObject(fields) => fields.get(field)
        case _ => None
      }
    }
  }

  def authorize(
      username: String,
      password: String): Future[Either[String, AuthServiceToken]] = {
    val encryptedToken: EncryptedToken = TokenManagement.encrypt(secretKey, username, password)
    for {
      authResponse <- http.singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = "http://localhost:8089/sa/secure/authorize",
          headers = List(Authorization(BasicHttpCredentials(username, password)))
        ))
      responseToken <- parseResult(authResponse, "token")
      result = responseToken match {
        case None => Left("Response from authorize did not contain token")
        case Some(JsString(token)) => Right(AuthServiceToken(token))
        case Some(_) => Left("Token field from authorize was not a string")
      }
    } yield result
  }

}

object AuthServiceClient {
  def apply(secretKey: SecretKey)(
      implicit ctx: ActorContext[Message],
      materializer: Materializer,
      esf: ExecutionSequencerFactory): AuthServiceClient = {
    new AuthServiceClient(secretKey)
  }
}
