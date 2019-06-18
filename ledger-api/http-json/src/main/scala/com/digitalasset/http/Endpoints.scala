package com.digitalasset.http

import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._

class Endpoints {

  private val ok = HttpEntity(ContentTypes.`application/json`, """{"status": "OK"}""")

  lazy val all = command orElse contracts orElse notFound

  lazy val command: PartialFunction[HttpRequest, HttpResponse] = {
    case HttpRequest(
        POST,
        Uri.Path("/command/create"),
        _,
        HttpEntity.Strict(ContentTypes.`application/json`, data),
        _) =>
      HttpResponse(entity = HttpEntity.Strict(ContentTypes.`application/json`, data))

    case HttpRequest(
        POST,
        Uri.Path("/command/exercise"),
        _,
        HttpEntity.Strict(ContentTypes.`application/json`, data),
        _) =>
      HttpResponse(entity = HttpEntity.Strict(ContentTypes.`application/json`, data))
  }

  lazy val contracts: PartialFunction[HttpRequest, HttpResponse] = {

    case HttpRequest(GET, Uri.Path("/contracts/lookup"), _, _, _) =>
      HttpResponse(entity = ok)

    case HttpRequest(GET, Uri.Path("/contracts/search"), _, _, _) =>
      HttpResponse(entity = ok)

    case HttpRequest(
        POST,
        Uri.Path("/contracts/search"),
        _,
        HttpEntity.Strict(ContentTypes.`application/json`, data),
        _) =>
      HttpResponse(entity = HttpEntity.Strict(ContentTypes.`application/json`, data))
  }

  lazy val notFound: PartialFunction[HttpRequest, HttpResponse] = {
    case HttpRequest(_, _, _, _, _) => HttpResponse(status = StatusCodes.NotFound)
  }
}
