// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.json

import com.digitalasset.ledger.api.refinements.ApiTypes
import com.digitalasset.navigator.model._
import com.digitalasset.navigator.json.Util._
import com.digitalasset.navigator.json.DamlLfCodec.JsonImplicits._
import com.digitalasset.navigator.json.ApiCodecCompressed.JsonImplicits._
import spray.json._

/**
  * An encoding of Model types.
  *
  */
object ModelCodec {

  // ------------------------------------------------------------------------------------------------------------------
  // Constants used in the encoding
  // ------------------------------------------------------------------------------------------------------------------
  private[this] final val propType: String = "type"
  private[this] final val contractCreatedTag: String = "ContractCreated"
  private[this] final val contractArchivedTag: String = "ContractArchived"
  private[this] final val choiceExercisedTag: String = "ChoiceExercised"

  // ------------------------------------------------------------------------------------------------------------------
  // Encoding
  // ------------------------------------------------------------------------------------------------------------------
  /*
  def eventToJsValue(event: Event): JsValue = {
    import JsonImplicits._

    val fields: List[JsField] = event match {
      case c: ContractCreated   => c.toJson.asJsObject.fields.toList :+ propType -> JsString(contractCreatedTag)
      case a: ContractArchived  => a.toJson.asJsObject.fields.toList :+ propType -> JsString(contractArchivedTag)
      case e: ChoiceExercised   => e.toJson.asJsObject.fields.toList :+ propType -> JsString(choiceExercisedTag)
    }
    JsObject(fields: _*)
  }
   */

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding
  // ------------------------------------------------------------------------------------------------------------------
  /*
  def jsValueToEvent(value: JsValue): Event = {
    import JsonImplicits._

    strField(value, propType, "Event") match {
      case `contractCreatedTag`   => value.convertTo[ContractCreated]
      case `contractArchivedTag`  => value.convertTo[ContractArchived]
      case `choiceExercisedTag`   => value.convertTo[ChoiceExercised]
      case subclassType           => deserializationError(s"Unknown Event type: $subclassType")
    }
  }
   */

  // ------------------------------------------------------------------------------------------------------------------
  // Implicits that can be imported to convert from and to JSON
  // ------------------------------------------------------------------------------------------------------------------
  object JsonImplicits extends DefaultJsonProtocol {

    implicit object ContractIdJsonFormat extends RootJsonFormat[ApiTypes.ContractId] {
      def write(value: ApiTypes.ContractId): JsValue = JsString(ApiTypes.ContractId.unwrap(value))
      def read(value: JsValue): ApiTypes.ContractId =
        ApiTypes.ContractId(asString(value, "ContractId"))
    }

    implicit object EventIdJsonFormat extends RootJsonFormat[ApiTypes.EventId] {
      def write(value: ApiTypes.EventId): JsValue = JsString(ApiTypes.EventId.unwrap(value))
      def read(value: JsValue): ApiTypes.EventId = ApiTypes.EventId(asString(value, "EventId"))
    }

    implicit object TransactionIdJsonFormat extends RootJsonFormat[ApiTypes.TransactionId] {
      def write(value: ApiTypes.TransactionId): JsValue =
        JsString(ApiTypes.TransactionId.unwrap(value))
      def read(value: JsValue): ApiTypes.TransactionId =
        ApiTypes.TransactionId(asString(value, "TransactionId"))
    }

    implicit object WorkflowIdJsonFormat extends RootJsonFormat[ApiTypes.WorkflowId] {
      def write(value: ApiTypes.WorkflowId): JsValue = JsString(ApiTypes.WorkflowId.unwrap(value))
      def read(value: JsValue): ApiTypes.WorkflowId =
        ApiTypes.WorkflowId(asString(value, "WorkflowId"))
    }

    implicit object ChoiceJsonFormat extends RootJsonFormat[ApiTypes.Choice] {
      def write(value: ApiTypes.Choice): JsValue = JsString(ApiTypes.Choice.unwrap(value))
      def read(value: JsValue): ApiTypes.Choice = ApiTypes.Choice(asString(value, "Choice"))
    }

    implicit object PartyJsonFormat extends RootJsonFormat[ApiTypes.Party] {
      def write(value: ApiTypes.Party): JsValue = JsString(ApiTypes.Party.unwrap(value))
      def read(value: JsValue): ApiTypes.Party = ApiTypes.Party(asString(value, "Party"))
    }

    implicit object ContractFormat extends RootJsonWriter[Contract] {
      def write(value: Contract): JsValue = JsObject(
        "id" -> value.id.toJson,
        "template" -> value.template.toJson,
        "argument" -> value.argument.toJson,
        "agreementText" -> value.agreementText.toJson
      )
      def read(value: JsValue, types: DamlLfTypeLookup): Contract = {
        val id = anyField(value, "id", "Contract").convertTo[ApiTypes.ContractId]
        val template = anyField(value, "template", "Contract").convertTo[Template]
        val argument = ApiCodecCompressed
          .jsValueToApiType(anyField(value, "record", "Contract"), template.id, types)
          .asInstanceOf[ApiRecord]
        val agreementText = anyField(value, "agreementText", "Contract").convertTo[Option[String]]
        Contract(id, template, argument, agreementText)
      }
    }

    implicit val PartyListJsonFormat: RootJsonFormat[List[ApiTypes.Party]] =
      listFormat[ApiTypes.Party]
    implicit val choiceFormat: RootJsonFormat[Choice] = jsonFormat4(Choice.apply)
    implicit val templateFormat: RootJsonFormat[Template] = jsonFormat2(Template.apply)

  }
}
