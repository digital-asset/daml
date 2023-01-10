// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.daml.navigator.config.Arguments
import com.daml.navigator.store.Store._
import spray.json._
import DefaultJsonProtocol._
import com.daml.navigator.store.Store
import com.daml.navigator.time.TimeProviderType

import scala.concurrent.{ExecutionContext, Future}

/** Provides a way of getting debug information about the application state.
  * The information is returned in an opaque JSON format,
  * and is useful for displaying debug information in the frontend.
  */
trait InfoHandler {
  def getInfo: Future[JsValue]
}

case class DefaultInfoHandler(arguments: Arguments, platformStore: ActorRef)(implicit
    executionContext: ExecutionContext
) extends InfoHandler {

  private case class Info(arguments: Arguments, appInfo: Either[String, ApplicationStateInfo])
  private implicit object argumentsWriter extends RootJsonWriter[Arguments] {
    override def write(obj: Arguments): JsValue = JsObject(
      "port" -> obj.port.toJson,
      "assets" -> obj.assets.toJson,
      "time" -> TimeProviderType.write(obj.time).toJson,
      "configFile" -> obj.configFile.map(p => p.toString).toJson,
      "tlsConfig" -> obj.tlsConfig
        .fold[JsValue](JsNull)(c =>
          JsObject(
            "enabled" -> c.enabled.toJson,
            "privateKeyFile" -> c.privateKeyFile.fold[JsValue](JsNull)(f => JsString(f.toString)),
            "certChainFile" -> c.certChainFile.fold[JsValue](JsNull)(f => JsString(f.toString)),
            "trustCollectionFile" -> c.trustCollectionFile.fold[JsValue](JsNull)(f =>
              JsString(f.toString)
            ),
          )
        ),
    )
  }
  private implicit object actorInfoWriter extends RootJsonWriter[PartyActorInfo] {
    override def write(obj: PartyActorInfo): JsValue = obj match {
      case _: PartyActorStarting => JsString("Starting")
      case _: PartyActorStarted => JsString("Started")
      case info: PartyActorFailed => JsString(s"Failed: ${info.error.getMessage}")
    }
  }
  private implicit object actorResponseWriter extends RootJsonWriter[PartyActorResponse] {
    override def write(obj: PartyActorResponse): JsValue = obj match {
      case PartyActorRunning(info) => info.toJson
      case Store.PartyActorUnresponsive => JsString("Unresponsive")
    }
  }
  private implicit object appInfoWriter extends RootJsonWriter[ApplicationStateInfo] {
    override def write(obj: ApplicationStateInfo): JsValue = obj match {
      case info: ApplicationStateConnecting =>
        JsObject(
          "status" -> JsString("Connecting"),
          "platformHost" -> info.platformHost.toJson,
          "platformPort" -> info.platformPort.toJson,
          "tls" -> info.tls.toJson,
          "applicationId" -> info.applicationId.toJson,
        )
      case info: ApplicationStateConnected =>
        JsObject(
          "status" -> JsString("Connected"),
          "platformHost" -> info.platformHost.toJson,
          "platformPort" -> info.platformPort.toJson,
          "tls" -> info.tls.toJson,
          "applicationId" -> info.applicationId.toJson,
          "ledgerId" -> info.ledgerId.toJson,
          "ledgerTime" -> JsObject(
            "time" -> DateTimeFormatter.ISO_INSTANT
              .format(info.ledgerTime.time.getCurrentTime)
              .toJson,
            "type" -> TimeProviderType.write(info.ledgerTime.`type`).toJson,
          ),
          "partyActors" -> JsObject(
            info.partyActors.map { case (p, s) => p -> s.toJson }.toMap
          ),
        )
      case info: ApplicationStateFailed =>
        JsObject(
          "status" -> JsString("Failed"),
          "platformHost" -> info.platformHost.toJson,
          "platformPort" -> info.platformPort.toJson,
          "tls" -> info.tls.toJson,
          "applicationId" -> info.applicationId.toJson,
          "error" -> info.error.toString.toJson,
        )
    }
  }
  private implicit object infoWriter extends RootJsonWriter[Info] {
    override def write(obj: Info): JsValue = JsObject(
      "arguments" -> obj.arguments.toJson,
      "appInfo" -> obj.appInfo.fold(_.toJson, _.toJson),
    )
  }

  private def getStoreInfo: Future[ApplicationStateInfo] = {
    implicit val actorTimeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    (platformStore ? GetApplicationStateInfo).mapTo[ApplicationStateInfo]
  }

  def getInfo: Future[JsValue] = {
    getStoreInfo
      .map(i => Info(arguments, Right(i)))
      .recover[Info] { case e: Throwable => Info(arguments, Left(e.getMessage)) }
      .map(i => i.toJson)
  }
}
