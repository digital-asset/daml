package com.digitalasset.http.util

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethod, HttpRequest, HttpResponse, StatusCode, StatusCodes, Uri}
import akka.util.ByteString
import com.digitalasset.http.Endpoints.{Error, InvalidUserInput, NotFound, ServerError, Unauthorized}
import com.digitalasset.http.json.ResponseFormats
import spray.json.{JsObject, JsValue}

import scala.concurrent.Future

object EndpointUtil { // TODO: transfer this to a trait to place functions shared by websocketEndpoint + endpoints

  lazy val notFound: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case HttpRequest(method, uri, _, _, _) =>
      Future.successful(httpResponseError(NotFound(s"${method: HttpMethod}, uri: ${uri: Uri}")))
  }

  private def httpResponseError(error: Error): HttpResponse = {
    val (status, jsObject) = errorsJsObject(error)
    httpResponse(status, jsObject)
  }

  private def errorsJsObject(error: Error): (StatusCode, JsObject) = {
    val (status, errorMsg): (StatusCode, String) = error match {
      case InvalidUserInput(e) => StatusCodes.BadRequest -> e
      case ServerError(e) => StatusCodes.InternalServerError -> e
      case Unauthorized(e) => StatusCodes.Unauthorized -> e
      case NotFound(e) => StatusCodes.NotFound -> e
    }
    (status, ResponseFormats.errorsJsObject(status, errorMsg))
  }

  private def httpResponse(status: StatusCode, data: JsValue): HttpResponse = {
    HttpResponse(
      status = status,
      entity = HttpEntity.Strict(ContentTypes.`application/json`, format(data)))
  }

  private def format(a: JsValue): ByteString = ByteString(a.compactPrint)



}
