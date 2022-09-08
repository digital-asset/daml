// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.json

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.model._
import com.daml.navigator.json.Util._
import com.daml.navigator.json.DamlLfCodec.JsonImplicits._
import spray.json._

/** An encoding of Model types.
  */
object ModelCodec {

  // ------------------------------------------------------------------------------------------------------------------
  // Constants used in the encoding
  // ------------------------------------------------------------------------------------------------------------------

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

    implicit val PartyListJsonFormat: RootJsonFormat[List[ApiTypes.Party]] =
      listFormat[ApiTypes.Party]
    implicit val choiceFormat: RootJsonFormat[Choice] = jsonFormat5(Choice.apply)
    implicit val templateFormat: RootJsonFormat[Template] = jsonFormat4(Template.apply)
  }
}
