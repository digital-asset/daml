package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow

object DummyMain extends App {

  implicit val system = ActorSystem("dummy-http-json-ledger-api")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  val endpoints = new Endpoints()
  val bindingFuture = Http().bindAndHandle(Flow.fromFunction(endpoints.all), "localhost", 8080)

  Thread.sleep(Long.MaxValue)

  bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate())
}
