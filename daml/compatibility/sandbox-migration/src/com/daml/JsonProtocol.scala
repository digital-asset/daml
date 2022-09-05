// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.nio.file.{Files, Path}

import com.daml.ledger.api.v1.value.Record
import com.daml.ledger.api.validation.{NoLoggingValueValidator => ValueValidator}
import com.daml.lf.value.json.ApiCodecCompressed
import spray.json._

object JsonProtocol extends DefaultJsonProtocol {

  private def cannotReadDamlLf(): RuntimeException =
    new UnsupportedOperationException("Reading JSON-encoded Daml-LF value is not supported")

  implicit object RecordJsonFormat extends JsonFormat[Record] {
    override def read(json: JsValue): Record =
      throw cannotReadDamlLf()
    override def write(record: Record): JsValue =
      ApiCodecCompressed.apiValueToJsValue(
        ValueValidator.validateRecord(record).fold(err => throw err, identity)
      )
  }

  private implicit class JsObjectWith(val jsObject: JsObject) extends AnyVal {
    def +(pair: (String, JsValue)): JsObject =
      jsObject.copy(fields = jsObject.fields + pair)
  }

  import Application._
  implicit val createdFormat: RootJsonFormat[CreatedResult] =
    jsonFormat4(CreatedResult.apply)
  implicit val archivedFormat: RootJsonFormat[ArchivedResult] =
    jsonFormat3(ArchivedResult.apply)
  implicit val eventFormat: RootJsonFormat[EventResult] =
    new RootJsonFormat[EventResult] {
      override def read(json: JsValue): Application.EventResult =
        throw cannotReadDamlLf()
      override def write(eventResult: EventResult): JsValue =
        eventResult match {
          case create: CreatedResult =>
            createdFormat
              .write(create)
              .asJsObject + ("type" -> JsString("created"))
          case archive: ArchivedResult =>
            archivedFormat
              .write(archive)
              .asJsObject + ("type" -> JsString("archived"))
        }
    }
  implicit val contractFormat: RootJsonFormat[ContractResult] =
    jsonFormat2(Application.ContractResult.apply)
  implicit val transactionFormat: RootJsonFormat[TransactionResult] =
    jsonFormat3(Application.TransactionResult.apply)

  def saveAsJson[A: JsonWriter](outputFile: Path, a: A): Unit = {
    val _ = Files.write(outputFile, a.toJson.prettyPrint.getBytes())
  }

}
