package com.daml.lf.engine.trigger

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.stream.Materializer
import akka.util.ByteString
import com.daml.ports.Port
import spray.json.{JsObject, JsValue}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

// OAuth2 bearer token for requests to authentication service
case class AuthServiceToken(token: String)

class AuthServiceClient(
    host: InetAddress,
    port: Port,
)(implicit system: ActorSystem, materializer: Materializer, ec: ExecutionContext) {

  private val http: HttpExt = Http(system)

  private val authServiceBaseUri = "http://" + host.getHostAddress + ":" + port.toString + "/sa/"

  private def responseBodyToString(resp: HttpResponse): Future[String] = {
    resp.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
  }

  // Extract a field from the JSON object in an HTTP response.
  private def parseResponseField(response: HttpResponse, field: String): Future[Option[JsValue]] = {
    responseBodyToString(response) map { body =>
      body.parseJson match {
        case JsObject(fields) => fields.get(field)
        case _ => None
      }
    }
  }

  def authorize(username: String, password: String): Future[Either[String, AuthServiceToken]] = {
    for {
      authResponse <- http.singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = authServiceBaseUri + "secure/authorize",
          headers = List(Authorization(BasicHttpCredentials(username, password)))
        ))
      responseToken <- parseResponseField(authResponse, "token")
      result = responseToken match {
        case None => Left("Response from authorize did not contain token")
        case Some(JsString(token)) => Right(AuthServiceToken(token))
        case Some(_) => Left("Token field from authorize was not a string")
      }
    } yield result
  }

}

object AuthServiceClient {
  def apply(host: InetAddress, port: Port)(
      implicit system: ActorSystem,
      materializer: Materializer,
      ec: ExecutionContext,
  ): AuthServiceClient = {
    new AuthServiceClient(host, port)
  }
}
