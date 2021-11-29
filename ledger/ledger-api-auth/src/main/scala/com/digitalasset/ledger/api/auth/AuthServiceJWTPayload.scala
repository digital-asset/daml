// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant

import org.slf4j.{Logger, LoggerFactory}
import spray.json._

import scala.util.Try

/** The JWT token payload used in [[AuthServiceJWT]]
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
    readAs: List[String],
) {

  /** If this token is associated with exactly one party, returns that party name.
    * Otherwise, returns None.
    */
  def party: Option[String] = {
    val allParties = (actAs ++ readAs).toSet
    if (allParties.size == 1) allParties.headOption else None
  }
}

/** Codec for writing and reading [[AuthServiceJWTPayload]] to and from JSON.
  *
  * In general:
  * - All custom claims are placed in a namespace field according to the OpenID Connect standard.
  * - All fields are optional in JSON for forward/backward compatibility reasons.
  * - Extra JSON fields are ignored when reading.
  * - Null values and missing JSON fields map to None or a safe default value (if there is one).
  *
  * Example:
  * ```
  * {
  *   "https://daml.com/ledger-api": {
  *     "ledgerId": "aaaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
  *     "participantId": null,
  *     "applicationId": null,
  *     "admin": true,
  *     "actAs": ["Alice"],
  *     "readAs": ["Alice", "Bob"]
  *   },
  *   "exp": 1300819380
  * }
  * ```
  */
object AuthServiceJWTCodec {

  protected val logger: Logger = LoggerFactory.getLogger(AuthServiceJWTCodec.getClass)

  // ------------------------------------------------------------------------------------------------------------------
  // Constants used in the encoding
  // ------------------------------------------------------------------------------------------------------------------
  // OpenID Connect (OIDC) namespace for custom JWT claims
  final val oidcNamespace: String = "https://daml.com/ledger-api"

  private[this] final val propLedgerId: String = "ledgerId"
  private[this] final val propParticipantId: String = "participantId"
  private[this] final val propApplicationId: String = "applicationId"
  private[this] final val propAdmin: String = "admin"
  private[this] final val propActAs: String = "actAs"
  private[this] final val propReadAs: String = "readAs"
  private[this] final val propExp: String = "exp"
  private[this] final val propParty: String = "party" // Legacy JSON API payload

  // The presence of any of these properties signals that a custom JWT token is being used.
  private[this] final val customProperties =
    Array(propLedgerId, propParticipantId, propApplicationId, propReadAs, propActAs)

  // ------------------------------------------------------------------------------------------------------------------
  // Encoding
  // ------------------------------------------------------------------------------------------------------------------
  def writeToString(v: AuthServiceJWTPayload): String =
    writePayload(v).compactPrint

  def writePayload(v: AuthServiceJWTPayload): JsValue = JsObject(
    oidcNamespace -> JsObject(
      propLedgerId -> writeOptionalString(v.ledgerId),
      propParticipantId -> writeOptionalString(v.participantId),
      propApplicationId -> writeOptionalString(v.applicationId),
      propAdmin -> JsBoolean(v.admin),
      propActAs -> writeStringList(v.actAs),
      propReadAs -> writeStringList(v.readAs),
    ),
    propExp -> writeOptionalInstant(v.exp),
  )

  /** Writes the given payload to a compact JSON string */
  def compactPrint(v: AuthServiceJWTPayload): String = writePayload(v).compactPrint

  private[this] def writeOptionalString(value: Option[String]): JsValue =
    value.fold[JsValue](JsNull)(JsString(_))

  private[this] def writeStringList(value: List[String]): JsValue =
    JsArray(value.map(JsString(_)): _*)

  private[this] def writeOptionalInstant(value: Option[Instant]): JsValue =
    value.fold[JsValue](JsNull)(i => JsNumber(i.getEpochSecond))

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding
  // ------------------------------------------------------------------------------------------------------------------
  def readFromString(value: String): Try[AuthServiceJWTPayload] = {
    import AuthServiceJWTCodec.JsonImplicits._
    for {
      json <- Try(value.parseJson)
      parsed <- Try(json.convertTo[AuthServiceJWTPayload])
    } yield parsed
  }

  def readPayload(value: JsValue): AuthServiceJWTPayload = value match {
    case JsObject(fields) if !fields.contains(oidcNamespace) =>
      if (customProperties.exists(fields.contains)) {
        // Legacy format
        logger.warn(s"Token ${value.compactPrint} is using a deprecated JWT payload format")
        AuthServiceJWTPayload(
          ledgerId = readOptionalString(propLedgerId, fields),
          participantId = readOptionalString(propParticipantId, fields),
          applicationId = readOptionalString(propApplicationId, fields),
          exp = readInstant(propExp, fields),
          admin = readOptionalBoolean(propAdmin, fields).getOrElse(false),
          actAs =
            readOptionalStringList(propActAs, fields) ++ readOptionalString(propParty, fields).toList,
          readAs = readOptionalStringList(propReadAs, fields),
        )
      } else {
        // FIXME: consider whether this is really the right place
        AuthServiceJWTPayload(
          ledgerId = None,
          // FIXME: allow for an array of audiences
          participantId = readOptionalString("aud", fields),
          applicationId = readOptionalString("sub", fields),
          exp = readInstant("exp", fields),
          admin = false,
          actAs = List.empty,
          readAs = List.empty
        )
      }
    case JsObject(fields) =>
      // New format: OIDC compliant
      val customClaims = fields
        .getOrElse(
          oidcNamespace,
          deserializationError(
            s"Can't read ${value.prettyPrint} as AuthServiceJWTPayload: namespace missing"
          ),
        )
        .asJsObject(
          s"Can't read ${value.prettyPrint} as AuthServiceJWTPayload: namespace is not an object"
        )
        .fields
      AuthServiceJWTPayload(
        ledgerId = readOptionalString(propLedgerId, customClaims),
        participantId = readOptionalString(propParticipantId, customClaims),
        applicationId = readOptionalString(propApplicationId, customClaims),
        exp = readInstant(propExp, fields),
        admin = readOptionalBoolean(propAdmin, customClaims).getOrElse(false),
        actAs = readOptionalStringList(propActAs, customClaims),
        readAs = readOptionalStringList(propReadAs, customClaims),
      )
    case _ =>
      deserializationError(
        s"Can't read ${value.prettyPrint} as AuthServiceJWTPayload: value is not an object"
      )
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
      fields: Map[String, JsValue],
  ): List[String] = fields.get(name) match {
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
      fields: Map[String, JsValue],
  ): Option[Boolean] = fields.get(name) match {
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
      case Some(JsNumber(epochSeconds)) => Some(Instant.ofEpochSecond(epochSeconds.longValue))
      case Some(value) =>
        deserializationError(s"Can't read ${value.prettyPrint} as epoch seconds for $name")
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
