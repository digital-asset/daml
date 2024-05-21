// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import org.slf4j.{Logger, LoggerFactory}
import spray.json.*

import java.time.Instant
import scala.util.Try

/** All the JWT payloads that can be used with the JWT auth service. */
sealed abstract class AuthServiceJWTPayload extends Product with Serializable

/** There are two JWT token formats which are currently supported by `StandardJWTPayload`.
  * The format is identified by `aud` claim.
  */
sealed trait StandardJWTTokenFormat
object StandardJWTTokenFormat {

  /** `Scope` format is for the tokens where scope field contains `daml_ledger_api`
    * or if it contains a bespoke string configured through a target-scope parameter.
    */
  final case object Scope extends StandardJWTTokenFormat

  /** `Audience` format is for the tokens where `aud` claim starts with `https://daml.com/jwt/aud/participant/`
    * or if it contains a bespoke string configured through a target-audience parameter.
    */
  final case object Audience extends StandardJWTTokenFormat
}

/** Payload parsed from the standard "sub", "aud", "exp", "iss" claims as specified in
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
  * @param audiences      If non-empty and it is an audience-based token,
  *                       the token is only valid for the intended recipients.
  */
final case class StandardJWTPayload(
    issuer: Option[String],
    userId: String,
    participantId: Option[String],
    exp: Option[Instant],
    format: StandardJWTTokenFormat,
    audiences: List[String],
    scope: Option[String],
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
  // Unique scope for standard tokens, following the pattern of https://developers.google.com/identity/protocols/oauth2/scopes
  final val scopeLedgerApiFull: String = "daml_ledger_api"

  private[this] final val audPrefix: String = "https://daml.com/jwt/aud/participant/"
  private[this] final val propAud: String = "aud"
  private[this] final val propIss: String = "iss"
  private[this] final val propExp: String = "exp"
  private[this] final val propSub: String = "sub"
  private[this] final val propScope: String = "scope"

  // ------------------------------------------------------------------------------------------------------------------
  // Encoding
  // ------------------------------------------------------------------------------------------------------------------
  def writePayload: AuthServiceJWTPayload => JsValue = {
    case v: StandardJWTPayload if v.format == StandardJWTTokenFormat.Scope =>
      JsObject(
        propIss -> writeOptionalString(v.issuer),
        propAud -> writeOptionalString(v.participantId),
        propSub -> JsString(v.userId),
        propExp -> writeOptionalInstant(v.exp),
        propScope -> writeOptionalString(v.scope),
      )
    case v: StandardJWTPayload =>
      JsObject(
        propIss -> writeOptionalString(v.issuer),
        propAud -> v.audiences.headOption.fold[JsValue](
          JsString(audPrefix + v.participantId.getOrElse(""))
        )(_ => writeStringList(v.audiences)),
        propSub -> JsString(v.userId),
        propExp -> writeOptionalInstant(v.exp),
        propScope -> writeOptionalString(v.scope),
      )
  }

  def writeAudienceBasedPayload: AuthServiceJWTPayload => JsValue = {
    case v: StandardJWTPayload if v.format == StandardJWTTokenFormat.Audience =>
      JsObject(
        propIss -> writeOptionalString(v.issuer),
        propAud -> writeStringList(v.audiences),
        propSub -> JsString(v.userId),
        propExp -> writeOptionalInstant(v.exp),
        propScope -> writeOptionalString(v.scope),
      )
    case _: StandardJWTPayload =>
      serializationError(
        s"Could not write StandardJWTPayload of scope format as audience-based payload"
      )
  }

  def writeScopeBasedPayload: AuthServiceJWTPayload => JsValue = {
    case v: StandardJWTPayload if v.format == StandardJWTTokenFormat.Scope =>
      JsObject(
        propIss -> writeOptionalString(v.issuer),
        propAud -> writeStringList(v.audiences),
        propSub -> JsString(v.userId),
        propExp -> writeOptionalInstant(v.exp),
        propScope -> writeOptionalString(v.scope),
      )
    case _: StandardJWTPayload =>
      serializationError(
        s"Could not write StandardJWTPayload of audience-based format as scope payload"
      )
  }

  /** Writes the given payload to a compact JSON string */
  def compactPrint(
      v: AuthServiceJWTPayload,
      enforceFormat: Option[StandardJWTTokenFormat] = None,
  ): String =
    enforceFormat match {
      case Some(StandardJWTTokenFormat.Audience) => writeAudienceBasedPayload(v).compactPrint
      case Some(StandardJWTTokenFormat.Scope) => writeScopeBasedPayload(v).compactPrint
      case _ => writePayload(v).compactPrint
    }

  private[this] def writeOptionalString(value: Option[String]): JsValue =
    value.fold[JsValue](JsNull)(JsString(_))

  private[this] def writeStringList(value: List[String]): JsValue =
    JsArray(value.map(JsString(_))*)

  private[this] def writeOptionalInstant(value: Option[Instant]): JsValue =
    value.fold[JsValue](JsNull)(i => JsNumber(i.getEpochSecond))

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding
  // ------------------------------------------------------------------------------------------------------------------
  def readAudienceBasedToken(value: JsValue): AuthServiceJWTPayload = value match {
    case JsObject(fields) =>
      StandardJWTPayload(
        issuer = readOptionalString(propIss, fields),
        participantId = None,
        userId = readString(propSub, fields),
        exp = readInstant(propExp, fields),
        format = StandardJWTTokenFormat.Audience,
        audiences = readOptionalStringOrArray(propAud, fields),
        scope = readOptionalString(propScope, fields),
      )
    case _ =>
      deserializationError(
        s"Could not read ${value.prettyPrint} as AuthServiceJWTPayload: value is not an object"
      )
  }

  def readScopeBasedToken(value: JsValue): AuthServiceJWTPayload = value match {
    case JsObject(fields) =>
      StandardJWTPayload(
        issuer = readOptionalString(propIss, fields),
        participantId = None,
        userId = readString(propSub, fields),
        exp = readInstant(propExp, fields),
        format = StandardJWTTokenFormat.Scope,
        audiences = readOptionalStringOrArray(propAud, fields),
        scope = readOptionalString(propScope, fields),
      )
    case _ =>
      deserializationError(
        s"Could not read ${value.prettyPrint} as AuthServiceJWTPayload: value is not an object"
      )
  }

  def readFromString(value: String): Try[AuthServiceJWTPayload] =
    for {
      json <- Try(value.parseJson)
      parsed <- Try(readPayload(json))
    } yield parsed

  private def readPayload(value: JsValue): AuthServiceJWTPayload = value match {
    case JsObject(fields) =>
      val scope = fields.get(propScope)
      // Support scope that spells 'daml_ledger_api'
      val scopes = scope.toList
        .collect({ case JsString(scope) => scope.split(" ") })
        .flatten
      // We're using this rather restrictive test to ensure we continue parsing all legacy sandbox tokens that
      // are in use before the 2.0 release; and thereby maintain full backwards compatibility.
      val audienceValue = readOptionalStringOrArray(propAud, fields)
      // Tokens with audience which starts with `https://daml.com/participant/jwt/aud/participant/${participantId}`
      // where `${participantId}` is non-empty string are supported.
      // As required for JWTs, additional fields can be in a token but will be ignored (including scope)
      val participantAudiences = audienceValue.filter(_.startsWith(audPrefix))
      if (participantAudiences.nonEmpty) {
        participantAudiences
          .map(_.substring(audPrefix.length))
          .filter(_.nonEmpty) match {
          case participantId :: Nil =>
            StandardJWTPayload(
              issuer = readOptionalString(propIss, fields),
              participantId = Some(participantId),
              userId = readString(propSub, fields), // guarded by if-clause above
              exp = readInstant(propExp, fields),
              format = StandardJWTTokenFormat.Audience,
              audiences =
                List.empty, // we do not read or extract audience claims for ParticipantId-based tokens
              scope = readOptionalString(propScope, fields),
            )
          case Nil =>
            deserializationError(
              s"Could not read ${value.prettyPrint} as AuthServiceJWTPayload: " +
                s"`aud` must include participantId value prefixed by $audPrefix"
            )
          case _ =>
            deserializationError(
              s"Could not read ${value.prettyPrint} as AuthServiceJWTPayload: " +
                s"`aud` must include a single participantId value prefixed by $audPrefix"
            )
        }
      } else if (scopes.contains(scopeLedgerApiFull)) {
        // We support the tokens with scope containing `daml_ledger_api`, there is no restriction of `aud` field.
        val participantId = audienceValue match {
          case id :: Nil => Some(id)
          case Nil => None
          case _ =>
            deserializationError(
              s"Could not read ${value.prettyPrint} as AuthServiceJWTPayload: " +
                s"`aud` must be empty or a single participantId."
            )
        }
        StandardJWTPayload(
          issuer = readOptionalString(propIss, fields),
          participantId = participantId,
          userId = readString(propSub, fields),
          exp = readInstant(propExp, fields),
          format = StandardJWTTokenFormat.Scope,
          audiences =
            List.empty, // we do not read or extract audience claims for Scope-based tokens
          scope = Some(scopeLedgerApiFull),
        )
      } else
        deserializationError(
          msg =
            s"Access token with unknown scope \"${scopes.mkString}\". Issue tokens with adjusted or no scope to get rid of this warning."
        )

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

  private[this] def readString(name: String, fields: Map[String, JsValue]): String =
    fields.get(name) match {
      case Some(JsString(value)) => value
      case Some(value) =>
        deserializationError(s"Could not read ${value.prettyPrint} as string for $name")
      case _ =>
        deserializationError(s"Could not read value for $name")
    }

  private[this] def readOptionalStringOrArray(
      name: String,
      fields: Map[String, JsValue],
  ): List[String] =
    fields.get(name) match {
      case None => List.empty
      case Some(JsNull) => List.empty
      case Some(JsString(value)) => List(value)
      case Some(array: JsArray) =>
        readStringList(name, array.elements)
      case Some(value) =>
        deserializationError(s"Could not read ${value.prettyPrint} as string for $name")
    }

  private def readStringList(name: String, values: Vector[JsValue]) = {
    values.toList.map {
      case JsString(value) => value
      case value =>
        deserializationError(s"Could not read ${value.prettyPrint} as string element for $name")
    }
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
  object AudienceBasedTokenJsonImplicits extends DefaultJsonProtocol {
    implicit object AuthServiceJWTPayloadFormat extends RootJsonFormat[AuthServiceJWTPayload] {
      override def write(v: AuthServiceJWTPayload): JsValue = writeAudienceBasedPayload(v)

      override def read(json: JsValue): AuthServiceJWTPayload = readAudienceBasedToken(json)
    }
  }

  object ScopeBasedTokenJsonImplicits extends DefaultJsonProtocol {
    implicit object AuthServiceJWTPayloadFormat extends RootJsonFormat[AuthServiceJWTPayload] {
      override def write(v: AuthServiceJWTPayload): JsValue = writeScopeBasedPayload(v)

      override def read(json: JsValue): AuthServiceJWTPayload = readScopeBasedToken(json)
    }
  }
}
