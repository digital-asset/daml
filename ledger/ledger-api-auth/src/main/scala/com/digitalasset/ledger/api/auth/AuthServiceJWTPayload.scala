// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant

import org.slf4j.{Logger, LoggerFactory}
import spray.json._

import scala.util.Try

/** All the JWT payloads that can be used with the JWT auth service. */
sealed abstract class AuthServiceJWTPayload extends Product with Serializable

/** A JWT token payload constructed from custom claims specific to Daml ledgers.
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
final case class CustomDamlJWTPayload(
    ledgerId: Option[String],
    participantId: Option[String],
    applicationId: Option[String],
    exp: Option[Instant],
    admin: Boolean,
    actAs: List[String],
    readAs: List[String],
) extends AuthServiceJWTPayload {

  /** If this token is associated with exactly one party, returns that party name.
    * Otherwise, returns None.
    */
  def party: Option[String] = {
    val allParties = (actAs ++ readAs).toSet
    if (allParties.size == 1) allParties.headOption else None
  }
}

/** There are two JWT token formats which are currently supported by `StandardJWTPayload`.
  * The format is identified by `aud` claim.
  */
sealed trait StandardJWTTokenFormat
object StandardJWTTokenFormat {

  /** `Scope` format is for the tokens where scope field contains `daml_ledger_api`.
    */
  final case object Scope extends StandardJWTTokenFormat

  /** `ParticipantId` format is for the tokens where `aud` claim starts with `https://daml.com/jwt/aud/participant/`
    */
  final case object ParticipantId extends StandardJWTTokenFormat
}

/** Payload parsed from the standard "sub", "aud", "exp" claims as specified in
  * https://datatracker.ietf.org/doc/html/rfc7519#section-4.1
  *
  * @param issuer  The issuer of the JWT.
  *
  * @param userId  The user that is authenticated by this payload.
  *
  * @param participantId  If not set, then the user is authenticated for any participant node that accepts
  *                       the JWT issuer. We expect this to be used for development only.
  *                       If set then the user is authenticated for the given participantId.
  *
  * @param exp            If set, the token is only valid before the given instant.
  */
final case class StandardJWTPayload(
    issuer: Option[String],
    userId: String,
    participantId: Option[String],
    exp: Option[Instant],
    format: StandardJWTTokenFormat,
) extends AuthServiceJWTPayload

/** Codec for writing and reading [[AuthServiceJWTPayload]] to and from JSON.
  *
  * In general:
  * - All custom claims are placed in a namespace field according to the OpenID Connect standard.
  * - Access tokens use a Daml-specific scope to distinguish them from other access tokens
  *   issued by the same issuer for different systems or APIs.
  * - All fields are optional in JSON for forward/backward compatibility reasons, where appropriate.
  * - Extra JSON fields are ignored when reading.
  * - Null values and missing JSON fields map to None or a safe default value (if there is one).
  */
object AuthServiceJWTCodec {

  protected val logger: Logger = LoggerFactory.getLogger(AuthServiceJWTCodec.getClass)

  // ------------------------------------------------------------------------------------------------------------------
  // Constants used in the encoding
  // ------------------------------------------------------------------------------------------------------------------
  // OpenID Connect (OIDC) namespace for custom JWT claims
  final val oidcNamespace: String = "https://daml.com/ledger-api"
  // Unique scope for standard tokens, following the pattern of https://developers.google.com/identity/protocols/oauth2/scopes
  final val scopeLedgerApiFull: String = "daml_ledger_api"

  private[this] final val audPrefix: String = "https://daml.com/jwt/aud/participant/"
  private[this] final val propLedgerId: String = "ledgerId"
  private[this] final val propParticipantId: String = "participantId"
  private[this] final val propApplicationId: String = "applicationId"
  private[this] final val propAud: String = "aud"
  private[this] final val propIss: String = "iss"
  private[this] final val propAdmin: String = "admin"
  private[this] final val propActAs: String = "actAs"
  private[this] final val propReadAs: String = "readAs"
  private[this] final val propExp: String = "exp"
  private[this] final val propParty: String = "party" // Legacy JSON API payload

  // ------------------------------------------------------------------------------------------------------------------
  // Encoding
  // ------------------------------------------------------------------------------------------------------------------
  def writeToString(v: AuthServiceJWTPayload): String =
    writePayload(v).compactPrint

  def writePayload: AuthServiceJWTPayload => JsValue = {
    case v: CustomDamlJWTPayload =>
      JsObject(
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
    case v: StandardJWTPayload if v.format == StandardJWTTokenFormat.Scope =>
      JsObject(
        propIss -> writeOptionalString(v.issuer),
        propAud -> writeOptionalString(v.participantId),
        "sub" -> JsString(v.userId),
        "exp" -> writeOptionalInstant(v.exp),
        "scope" -> JsString(scopeLedgerApiFull),
      )
    case v: StandardJWTPayload =>
      JsObject(
        propIss -> writeOptionalString(v.issuer),
        propAud -> JsString(audPrefix + v.participantId.getOrElse("")),
        "sub" -> JsString(v.userId),
        "exp" -> writeOptionalInstant(v.exp),
      )
  }

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
    for {
      json <- Try(value.parseJson)
      parsed <- Try(readPayload(json))
    } yield parsed
  }

  def readPayload(value: JsValue): AuthServiceJWTPayload = value match {
    case JsObject(fields) =>
      val scope = fields.get("scope")
      val scopes = scope.toList.collect({ case JsString(scope) => scope.split(" ") }).flatten
      // We're using this rather restrictive test to ensure we continue parsing all legacy sandbox tokens that
      // are in use before the 2.0 release; and thereby maintain full backwards compatibility.
      val audienceValue = readOptionalStringOrSingletonArray(propAud, fields)
      if (audienceValue.exists(_.startsWith(audPrefix))) {
        // Tokens with audience which starts with `https://daml.com/participant/jwt/aud/participant/${participantId}`
        // where `${participantId}` is non-empty string are supported.
        // As required for JWTs, additional fields can be in a token but will be ignored (including scope)
        audienceValue.map(_.substring(audPrefix.length)).filter(_.nonEmpty) match {
          case Some(participantId) =>
            StandardJWTPayload(
              issuer = readOptionalString("iss", fields),
              participantId = Some(participantId),
              userId = readOptionalString("sub", fields).get, // guarded by if-clause above
              exp = readInstant("exp", fields),
              format = StandardJWTTokenFormat.ParticipantId,
            )
          case _ =>
            deserializationError(
              s"Could not read ${value.prettyPrint} as AuthServiceJWTPayload: " +
                s"`aud` must include participantId value prefixed by $audPrefix"
            )
        }
      } else if (scopes.contains(scopeLedgerApiFull)) {
        // We support the tokens with scope containing `daml_ledger_api`, there is no restriction of `aud` field.
        StandardJWTPayload(
          issuer = readOptionalString("iss", fields),
          participantId = audienceValue,
          userId = readOptionalString("sub", fields).get, // guarded by if-clause above
          exp = readInstant("exp", fields),
          format = StandardJWTTokenFormat.Scope,
        )
      } else {
        if (scope.nonEmpty)
          logger.warn(
            s"Access token with unknown scope \"${scope.get}\" is being parsed as a custom claims token. Issue tokens with adjusted or no scope to get rid of this warning."
          )
        if (!fields.contains(oidcNamespace)) {
          // Legacy format
          logger.warn(s"Token ${value.compactPrint} is using a deprecated JWT payload format")
          CustomDamlJWTPayload(
            ledgerId = readOptionalString(propLedgerId, fields),
            participantId = readOptionalString(propParticipantId, fields),
            applicationId = readOptionalString(propApplicationId, fields),
            exp = readInstant(propExp, fields),
            admin = readOptionalBoolean(propAdmin, fields).getOrElse(false),
            actAs = readOptionalStringList(propActAs, fields) ++ readOptionalString(
              propParty,
              fields,
            ).toList,
            readAs = readOptionalStringList(propReadAs, fields),
          )
        } else {
          // New format: OIDC compliant
          val customClaims = fields
            .getOrElse(
              oidcNamespace,
              deserializationError(
                s"Could not read ${value.prettyPrint} as AuthServiceJWTPayload: namespace missing"
              ),
            )
            .asJsObject(
              s"Could not read ${value.prettyPrint} as AuthServiceJWTPayload: namespace is not an object"
            )
            .fields
          CustomDamlJWTPayload(
            ledgerId = readOptionalString(propLedgerId, customClaims),
            participantId = readOptionalString(propParticipantId, customClaims),
            applicationId = readOptionalString(propApplicationId, customClaims),
            exp = readInstant(propExp, fields),
            admin = readOptionalBoolean(propAdmin, customClaims).getOrElse(false),
            actAs = readOptionalStringList(propActAs, customClaims),
            readAs = readOptionalStringList(propReadAs, customClaims),
          )
        }
      }
    case _ =>
      deserializationError(
        s"Could not read ${value.prettyPrint} as AuthServiceJWTPayload: value is not an object"
      )
  }

  private[this] def readOptionalString(name: String, fields: Map[String, JsValue]): Option[String] =
    fields.get(name) match {
      case None => None
      case Some(JsNull) => None
      case Some(JsString(value)) => Some(value)
      case Some(value) =>
        deserializationError(s"Could not read ${value.prettyPrint} as string for $name")
    }

  private[this] def readOptionalStringOrSingletonArray(
      name: String,
      fields: Map[String, JsValue],
  ): Option[String] =
    fields.get(name) match {
      case None => None
      case Some(JsNull) => None
      case Some(JsString(value)) => Some(value)
      case Some(JsArray(Vector(JsString(value)))) => Some(value)
      case Some(value) =>
        deserializationError(s"Could not read ${value.prettyPrint} as string for $name")
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
          deserializationError(s"Could not read ${value.prettyPrint} as string element for $name")
      }
    case Some(value) =>
      deserializationError(s"Could not read ${value.prettyPrint} as string list for $name")
  }

  private[this] def readOptionalBoolean(
      name: String,
      fields: Map[String, JsValue],
  ): Option[Boolean] = fields.get(name) match {
    case None => None
    case Some(JsNull) => None
    case Some(JsBoolean(value)) => Some(value)
    case Some(value) =>
      deserializationError(s"Could not read ${value.prettyPrint} as boolean for $name")
  }

  private[this] def readInstant(name: String, fields: Map[String, JsValue]): Option[Instant] =
    fields.get(name) match {
      case None => None
      case Some(JsNull) => None
      case Some(JsNumber(epochSeconds)) => Some(Instant.ofEpochSecond(epochSeconds.longValue))
      case Some(value) =>
        deserializationError(s"Could not read ${value.prettyPrint} as epoch seconds for $name")
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
