package com.daml.lf.engine.trigger

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, Uri}
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.util.ByteString
import com.daml.ports.Port
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

//case class AuthServiceToken(token: String)

//trait AuthServiceTokenJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
//  implicit val authServiceTokenFormat: RootJsonFormat[AuthServiceToken] = jsonFormat1(
//    AuthServiceToken)
//}

object AuthServiceDomain extends DefaultJsonProtocol {
  // OAuth2 bearer token for requests to authentication service
  case class AuthServiceToken(token: String)
  implicit val authServiceTokenFormat: RootJsonFormat[AuthServiceToken] = jsonFormat1(
    AuthServiceToken)
}

class AuthServiceClient(
    host: InetAddress,
    port: Port,
)(implicit system: ActorSystem, materializer: Materializer, ec: ExecutionContext) {
  import AuthServiceDomain._

  private val http: HttpExt = Http(system)

  private val authServiceBaseUri: Uri =
    Uri.from(scheme = "http", host = host.getHostAddress, port = port.value, path = "/sa")

  private def responseBodyToString(resp: HttpResponse): Future[String] = {
    resp.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
  }

  def authorize(username: String, password: String): Future[AuthServiceToken] = {
    for {
      authResponse <- http.singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = authServiceBaseUri.withPath(Path("/secure/authorize")),
          headers = List(Authorization(BasicHttpCredentials(username, password)))
        ))
      authServiceToken <- Unmarshal(authResponse).to[AuthServiceToken]
    } yield authServiceToken
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
