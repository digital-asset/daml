// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant

import org.slf4j.{Logger, LoggerFactory}
import spray.json._

import scala.util.{Success, Try}

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
  private[this] final val propSub: String = "sub"
  private[this] final val propScope: String = "scope"
  private[this] final val propParty: String = "party" // Legacy JSON API payload
  private[this] final val legacyProperties: List[String] =
    List(propLedgerId, propParticipantId, propApplicationId, propAdmin, propActAs, propReadAs)

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
        propSub -> JsString(v.userId),
        propExp -> writeOptionalInstant(v.exp),
        propScope -> writeOptionalString(v.scope),
      )
    case v: StandardJWTPayload =>
      JsObject(
        propIss -> writeOptionalString(v.issuer),
        propAud -> JsString(audPrefix + v.participantId.getOrElse("")),
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
    case _: CustomDamlJWTPayload =>
      serializationError(s"Could not write CustomDamlJWTPayload as audience-based payload")
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
    case _: CustomDamlJWTPayload =>
      serializationError(s"Could not write CustomDamlJWTPayload as scope payload")
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
    JsArray(value.map(JsString(_)): _*)

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

  def readScopeBasedTokenFromString(
      targetScope: String
  )(value: String): Try[AuthServiceJWTPayload] =
    for {
      json <- Try(value.parseJson)
      parsed <- Try(readScopeBasedToken(json))
      validated <- parsed match {
        case p: StandardJWTPayload if p.scope.contains(targetScope) => Success(p)
        case _ => deserializationError(msg = "Unexpected scope in the token")
      }
    } yield validated

  def readFromString(value: String): Try[AuthServiceJWTPayload] =
    for {
      json <- Try(value.parseJson)
      parsed <- Try(readPayload(json))
    } yield parsed

  private def readPayload(value: JsValue): AuthServiceJWTPayload = value match {
    case JsObject(fields) =>
      val scope = fields.get(propScope)
      // Support scope that spells 'daml_ledger_api'
      val scopes = scope.toList.collect({ case JsString(scope) => scope.split(" ") }).flatten
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
        fields.get(oidcNamespace) match {
          case Some(oidcNamespaceField) =>
            // Custom claims format, OIDC compliant
            val customClaims = oidcNamespaceField
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
          case None if legacyProperties.exists(fields.contains) =>
            // Legacy custom format without the nesting underneath the OIDC compliant extension
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
          case None if scopes.nonEmpty =>
            deserializationError(
              msg =
                s"Access token with unknown scope \"${scopes.mkString}\". Issue tokens with adjusted or no scope to get rid of this warning."
            )
          case _ =>
            logger.warn(s"Token ${value.compactPrint} is using an unsupported format")
            CustomDamlJWTPayload(
              ledgerId = None,
              participantId = None,
              applicationId = None,
              exp = readInstant(propExp, fields),
              admin = false,
              actAs = List.empty,
              readAs = List.empty,
            )
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

  private[this] def readOptionalStringList(
      name: String,
      fields: Map[String, JsValue],
  ): List[String] = fields.get(name) match {
    case None => List.empty
    case Some(JsNull) => List.empty
    case Some(JsArray(values)) =>
      readStringList(name, values)
    case Some(value) =>
      deserializationError(s"Could not read ${value.prettyPrint} as string list for $name")
  }

  private def readStringList(name: String, values: Vector[JsValue]) = {
    values.toList.map {
      case JsString(value) => value
      case value =>
        deserializationError(s"Could not read ${value.prettyPrint} as string element for $name")
    }
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
