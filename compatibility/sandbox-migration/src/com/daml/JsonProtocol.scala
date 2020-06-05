// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.ledger.api.v1.value.Record
import com.daml.ledger.api.validation.ValueValidator
import com.daml.lf.value.json.ApiCodecCompressed
import spray.json._

object JsonProtocol extends DefaultJsonProtocol {

  implicit object RecordJsonFormat extends JsonFormat[Record] {
    override def read(json: JsValue): Record =
      throw new UnsupportedOperationException("Reading JSON-encoded DAML-LF value is not supported")
    override def write(record: Record): JsValue =
      ApiCodecCompressed.apiValueToJsValue(
        ValueValidator.validateRecord(record).right.get.mapContractId(_.coid))
  }

  private implicit class JsObjectWith(val jsObject: JsObject) extends AnyVal {
    def +(pair: (String, JsValue)): JsObject =
      jsObject.copy(fields = jsObject.fields + pair)
  }

  import ProposeAccept._
  implicit val ProposeAcceptCreatedResult: RootJsonFormat[CreatedResult] =
    jsonFormat4(CreatedResult.apply)
  implicit val ProposeAcceptArchivedResult: RootJsonFormat[ArchivedResult] =
    jsonFormat3(ArchivedResult.apply)
  implicit val ProposeAcceptEventResult: RootJsonFormat[EventResult] =
    new RootJsonFormat[EventResult] {
      override def read(json: JsValue): EventResult =
        throw new UnsupportedOperationException(
          "Reading JSON-encoded DAML-LF value is not supported")
      override def write(eventResult: EventResult): JsValue =
        eventResult match {
          case create: CreatedResult =>
            ProposeAcceptCreatedResult
              .write(create)
              .asJsObject + ("type" -> JsString("created"))
          case archive: ArchivedResult =>
            ProposeAcceptArchivedResult
              .write(archive)
              .asJsObject + ("type" -> JsString("archived"))
        }
    }
  implicit val ProposeAcceptContractResult: RootJsonFormat[ContractResult] =
    jsonFormat2(ContractResult.apply)
  implicit val ProposeAcceptTransactionResult: RootJsonFormat[TransactionResult] = jsonFormat2(
    TransactionResult.apply)
  implicit val ProposeAcceptResult: RootJsonFormat[Result] = jsonFormat6(Result.apply)

}
