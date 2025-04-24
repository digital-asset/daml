// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.http.json.v2.ProtoInfo.{camelToSnake, normalizeName}
import io.circe

import scala.io.Source

/** Reads stored proto data in order to extract comments (and put into openapi).
  *
  * The algorithms used are inefficient, but since the code is used only to generate documentation
  * it should not cause any problems.
  */

final case class ProtoInfo(protoComments: ExtractedProtoComments) {

  def findMessageInfo(msgName: String): Option[MessageInfo] = protoComments.messages
    .get(msgName)
    .orElse(protoComments.messages.get(normalizeName(msgName)))
    .orElse(protoComments.oneOfs.get(camelToSnake(normalizeName(msgName))))
}

final case class MessageInfo(message: FieldData) {
  def getComments(): Option[String] = message.comments

  def getFieldComment(name: String): Option[String] =
    message.fieldComments
      .get(name)
      .orElse(message.fieldComments.get(camelToSnake(name)))
}

final case class FieldData(comments: Option[String], fieldComments: Map[String, String])

final case class ExtractedProtoComments(
    messages: Map[String, MessageInfo],
    oneOfs: Map[String, MessageInfo],
) {
  def toYaml(): String = {
    import io.circe.syntax.*
    import io.circe.yaml.syntax.AsYaml
    import io.circe.generic.auto.*
    val yaml = this.asJson.asYaml
    yaml.spaces2
  }
}

object ProtoInfo {
  val LedgerApiDescriptionResourceLocation = "ledger-api/proto-data.yml"
  def camelToSnake(name: String): String =
    name
      .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
      .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .toLowerCase

  /** We drop initial `Js` prefix and single digits suffixes.
    */
  def normalizeName(s: String): String =
    if (s.nonEmpty && s.last.isDigit) s.dropRight(1) else if (s.startsWith("Js")) s.drop(2) else s

  def loadData(): Either[circe.Error, ProtoInfo] = {
    val res = Source.fromResource(
      LedgerApiDescriptionResourceLocation,
      classOf[com.digitalasset.canton.http.json.v2.ProtoInfo.type].getClassLoader,
    )
    val yaml = res.getLines().mkString("\n")
    import io.circe.generic.auto.*
    import io.circe.yaml.parser
    parser
      .parse(yaml)
      .flatMap(_.as[ExtractedProtoComments])
      .map(ProtoInfo(_))
  }
}
