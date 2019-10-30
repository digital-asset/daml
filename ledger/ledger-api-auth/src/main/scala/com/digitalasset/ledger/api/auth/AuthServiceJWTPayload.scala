// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth

import java.time.Instant
import java.time.format.DateTimeFormatter

import spray.json._

/** The JWT token payload used in [[AuthServiceJWT]]
  *
  *
  * @param ledgerId       If set, the token is only valid for the given ledger ID.
  *                       May also be used to fill in missing ledger ID fields in ledger API requests.
  *
  * @param participantId  If set, the token is only valid for the given participant ID.
  *                       May also be used to fill in missing participant ID fields in ledger API requests.
  *
  * @param applicationId  If set, the token is only valid for the given application ID.
  *                       May also be used to fill in missing application ID fields in ledger API requests.
  *
  * @param exp            If set, the token is only valid before the given instant.
  *                       Note: This is a registered claim in JWT
  *
  * @param admin          Whether the token bearer is authorized to use admin endpoints of the ledger API.
  *
  * @param actAs          List of parties the token bearer can act as.
  *                       May also be used to fill in missing party fields in ledger API requests (e.g., submitter).
  *
  * @param readAs         List of parties the token bearer can read data for.
  *                       May also be used to fill in missing party fields in ledger API requests (e.g., transaction filter).
  */
case class AuthServiceJWTPayload(
    ledgerId: Option[String],
    participantId: Option[String],
    applicationId: Option[String],
    exp: Option[Instant],
    admin: Boolean,
    actAs: List[String],
    readAs: List[String]
)

/**
  * Codec for writing and reading [[AuthServiceJWTPayload]] to and from JSON.
  *
  * In general:
  * - All fields are optional in JSON for forward/backward compatibility reasons.
  * - Extra JSON fields are ignored when reading.
  * - Null values and missing JSON fields map to None or a safe default value (if there is one).
  *
  * Example:
  * ```
  * {
  *   "ledgerId": "aaaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
  *   "participantId": null,
  *   "applicationId": null,
  *   "exp": "2019-10-30T14:11:28Z",
  *   "admin": true,
  *   "actAs": ["Alice"],
  *   "readAs": ["Alice", "Bob"],
  * }
  * ```
  */
object AuthServiceJWTCodec {

  // ------------------------------------------------------------------------------------------------------------------
  // Constants used in the encoding
  // ------------------------------------------------------------------------------------------------------------------
  private[this] final val propLedgerId: String = "ledgerId"
  private[this] final val propParticipantId: String = "participantId"
  private[this] final val propApplicationId: String = "applicationId"
  private[this] final val propAdmin: String = "admin"
  private[this] final val propActAs: String = "actAs"
  private[this] final val propReadAs: String = "readAs"
  private[this] final val propExp: String = "readAs"

  // ------------------------------------------------------------------------------------------------------------------
  // Encoding
  // ------------------------------------------------------------------------------------------------------------------
  def writePayload(v: AuthServiceJWTPayload): JsValue = JsObject(
    propLedgerId -> writeOptionalString(v.ledgerId),
    propParticipantId -> writeOptionalString(v.participantId),
    propApplicationId -> writeOptionalString(v.applicationId),
    propAdmin -> JsBoolean(v.admin),
    propExp -> writeOptionalInstant(v.exp),
    propActAs -> writeStringList(v.actAs),
    propReadAs -> writeStringList(v.readAs)
  )

  private[this] def writeOptionalString(value: Option[String]): JsValue =
    value.fold[JsValue](JsNull)(JsString(_))

  private[this] def writeStringList(value: List[String]): JsValue =
    JsArray(value.map(JsString(_)): _*)

  private[this] def writeOptionalInstant(value: Option[Instant]): JsValue =
    value.fold[JsValue](JsNull)(i => JsString(DateTimeFormatter.ISO_INSTANT.format(i)))

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding
  // ------------------------------------------------------------------------------------------------------------------
  def readPayload(value: JsValue): AuthServiceJWTPayload = value match {
    case JsObject(fields) =>
      AuthServiceJWTPayload(
        ledgerId = readOptionalString(propLedgerId, fields),
        participantId = readOptionalString(propParticipantId, fields),
        applicationId = readOptionalString(propApplicationId, fields),
        exp = readInstant(propExp, fields),
        admin = readOptionalBoolean(propAdmin, fields).getOrElse(false),
        actAs = readOptionalStringList(propActAs, fields),
        readAs = readOptionalStringList(propReadAs, fields)
      )
    case _ =>
      deserializationError(s"Can't read ${value.prettyPrint} as AuthServiceJWTPayload")
  }

  private[this] def readOptionalString(name: String, fields: Map[String, JsValue]): Option[String] =
    fields.get(name) match {
      case None => None
      case Some(JsNull) => None
      case Some(JsString(value)) => Some(value)
      case Some(value) =>
        deserializationError(s"Can't read ${value.prettyPrint} as string for $name")
    }

  private[this] def readOptionalStringList(
      name: String,
      fields: Map[String, JsValue]): List[String] = fields.get(name) match {
    case None => List.empty
    case Some(JsNull) => List.empty
    case Some(JsArray(values)) =>
      values.toList.map {
        case JsString(value) => value
        case value =>
          deserializationError(s"Can't read ${value.prettyPrint} as string element for $name")
      }
    case Some(value) =>
      deserializationError(s"Can't read ${value.prettyPrint} as string list for $name")
  }

  private[this] def readOptionalBoolean(
      name: String,
      fields: Map[String, JsValue]): Option[Boolean] = fields.get(name) match {
    case None => None
    case Some(JsNull) => None
    case Some(JsBoolean(value)) => Some(value)
    case Some(value) =>
      deserializationError(s"Can't read ${value.prettyPrint} as boolean for $name")
  }

  private[this] def readInstant(name: String, fields: Map[String, JsValue]): Option[Instant] =
    fields.get(name) match {
      case None => None
      case Some(JsNull) => None
      case Some(JsString(value)) => Some(Instant.parse(value))
      case Some(value) =>
        deserializationError(s"Can't read ${value.prettyPrint} as ISO8601 time for $name")
    }

  // ------------------------------------------------------------------------------------------------------------------
  // Implicits that can be imported to write JSON
  // ------------------------------------------------------------------------------------------------------------------
  object JsonImplicits extends DefaultJsonProtocol {
    implicit object AuthServiceJWTPayloadFormat extends RootJsonFormat[AuthServiceJWTPayload] {
      override def write(v: AuthServiceJWTPayload): JsValue = writePayload(v)
      override def read(json: JsValue): AuthServiceJWTPayload = readPayload(json)
    }
  }
}
