// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import io.circe.*
import io.circe.parser.*
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant
import scala.util.{Failure, Success, Try}

/** All the JWT payloads that can be used with the JWT auth service. */
sealed abstract class AuthServiceJWTPayload extends Product with Serializable

/** There are two JWT token formats which are currently supported by `StandardJWTPayload`. The
  * format is identified by `aud` claim.
  */
sealed trait StandardJWTTokenFormat
object StandardJWTTokenFormat {

  /** `Scope` format is for the tokens where scope field contains `daml_ledger_api` or if it
    * contains a bespoke string configured through a target-scope parameter.
    */
  final case object Scope extends StandardJWTTokenFormat

  /** `Audience` format is for the tokens where `aud` claim starts with
    * `https://daml.com/jwt/aud/participant/` or if it contains a bespoke string configured through
    * a target-audience parameter.
    */
  final case object Audience extends StandardJWTTokenFormat
}

/** Payload parsed from the standard "sub", "aud", "exp", "iss" claims as specified in
  * https://datatracker.ietf.org/doc/html/rfc7519#section-4.1
  *
  * @param issuer
  *   The issuer of the JWT.
  *
  * @param userId
  *   The user that is authenticated by this payload.
  *
  * @param participantId
  *   If not set, then the user is authenticated for any participant node that accepts the JWT
  *   issuer. We expect this to be used for development only. If set then the user is authenticated
  *   for the given participantId.
  *
  * @param exp
  *   If set, the token is only valid before the given instant.
  * @param audiences
  *   If non-empty and it is an audience-based token, the token is only valid for the intended
  *   recipients.
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
  *   - All custom claims are placed in a namespace field according to the OpenID Connect standard.
  *   - Access tokens use a Daml-specific scope to distinguish them from other access tokens issued
  *     by the same issuer for different systems or APIs.
  *   - All fields are optional in JSON for forward/backward compatibility reasons, where
  *     appropriate.
  *   - Extra JSON fields are ignored when reading.
  *   - Null values and missing JSON fields map to None or a safe default value (if there is one).
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
  private[this] final val propScp: String = "scp"

  // ------------------------------------------------------------------------------------------------------------------
  // Encoding
  // ------------------------------------------------------------------------------------------------------------------
  def writePayload: AuthServiceJWTPayload => Json = {
    case v: StandardJWTPayload if v.format == StandardJWTTokenFormat.Scope =>
      Json.obj(
        propIss -> writeOptionalString(v.issuer),
        propAud -> writeOptionalString(v.participantId),
        propSub -> Json.fromString(v.userId),
        propExp -> writeOptionalInstant(v.exp),
        propScope -> writeOptionalString(v.scope),
      )
    case v: StandardJWTPayload =>
      Json.obj(
        propIss -> writeOptionalString(v.issuer),
        propAud -> (v.audiences.headOption match {
          case None => Json.fromString(audPrefix + v.participantId.getOrElse(""))
          case Some(_) => writeStringList(v.audiences)
        }),
        propSub -> Json.fromString(v.userId),
        propExp -> writeOptionalInstant(v.exp),
        propScope -> writeOptionalString(v.scope),
      )
  }

  def writeAudienceBasedPayload: AuthServiceJWTPayload => Json = {
    case v: StandardJWTPayload if v.format == StandardJWTTokenFormat.Audience =>
      Json.obj(
        propIss -> writeOptionalString(v.issuer),
        propAud -> writeStringList(v.audiences),
        propSub -> Json.fromString(v.userId),
        propExp -> writeOptionalInstant(v.exp),
        propScope -> writeOptionalString(v.scope),
      )
    case _: StandardJWTPayload =>
      throw new RuntimeException(
        s"Could not write StandardJWTPayload of scope format as audience-based payload"
      )
  }

  def writeScopeBasedPayload: AuthServiceJWTPayload => Json = {
    case v: StandardJWTPayload if v.format == StandardJWTTokenFormat.Scope =>
      Json.obj(
        propIss -> writeOptionalString(v.issuer),
        propAud -> writeStringList(v.audiences),
        propSub -> Json.fromString(v.userId),
        propExp -> writeOptionalInstant(v.exp),
        propScope -> writeOptionalString(v.scope),
      )
    case _: StandardJWTPayload =>
      throw new RuntimeException(
        s"Could not write StandardJWTPayload of audience-based format as scope payload"
      )
  }

  /** Writes the given payload to a compact JSON string */
  def compactPrint(
      v: AuthServiceJWTPayload,
      enforceFormat: Option[StandardJWTTokenFormat] = None,
  ): String =
    enforceFormat match {
      case Some(StandardJWTTokenFormat.Audience) =>
        writeAudienceBasedPayload(v).noSpaces
      case Some(StandardJWTTokenFormat.Scope) => writeScopeBasedPayload(v).noSpaces
      case _ => writePayload(v).noSpaces
    }

  private[this] def writeOptionalString(value: Option[String]): Json =
    value.fold(Json.Null)(Json.fromString)

  private[this] def writeStringList(value: List[String]): Json =
    Json.fromValues(value.map(Json.fromString).toVector)

  private[this] def writeOptionalInstant(value: Option[Instant]): Json =
    value.fold(Json.Null)(i => Json.fromLong(i.getEpochSecond))

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding
  // ------------------------------------------------------------------------------------------------------------------
  def readAudienceBasedToken(value: Json): Either[String, AuthServiceJWTPayload] =
    value.asObject
      .toRight(s"Could not read ${value.spaces2} as AuthServiceJWTPayload: value is not an object")
      .map { obj =>
        val fields = obj.toMap
        StandardJWTPayload(
          issuer = readOptionalString(propIss, fields),
          participantId = None,
          userId = readString(propSub, fields),
          exp = readInstant(propExp, fields),
          format = StandardJWTTokenFormat.Audience,
          audiences = readOptionalStringOrArray(propAud, fields),
          scope = readAndCombineScopes(fields),
        )
      }

  def readScopeBasedToken(value: Json): Either[String, AuthServiceJWTPayload] =
    value.asObject
      .toRight(s"Could not read ${value.noSpaces} as AuthServiceJWTPayload: value is not an object")
      .map { obj =>
        val fields = obj.toMap
        StandardJWTPayload(
          issuer = readOptionalString(propIss, fields),
          participantId = None,
          userId = readString(propSub, fields),
          exp = readInstant(propExp, fields),
          format = StandardJWTTokenFormat.Scope,
          audiences = readOptionalStringOrArray(propAud, fields),
          scope = readAndCombineScopes(fields),
        )
      }

  def readFromString(value: String): Either[RuntimeException, AuthServiceJWTPayload] =
    parse(value).left
      .map(_.getMessage)
      .flatMap(readPayload)
      .left
      .map(message => new RuntimeException(message))

  private def readPayload(value: Json): Either[String, AuthServiceJWTPayload] =
    value.asObject match {
      case Some(obj) =>
        val fields = obj.toMap
        // Support scope that spells 'daml_ledger_api'
        val scopes = readScopes(fields)
        // We're using this rather restrictive test to ensure we continue parsing all legacy sandbox tokens that
        // are in use before the 2.0 release; and thereby maintain full backwards compatibility.
        val audienceValue = readOptionalStringOrArray(propAud, fields)
        // Tokens with audience which starts with `https://daml.com/jwt/aud/participant/${participantId}`
        // where `${participantId}` is non-empty string are supported.
        // As required for JWTs, additional fields can be in a token but will be ignored (including scope)
        val participantAudiences = audienceValue.filter(_.startsWith(audPrefix))
        if (participantAudiences.nonEmpty) {
          participantAudiences
            .map(_.substring(audPrefix.length))
            .filter(_.nonEmpty) match {
            case participantId :: Nil =>
              Right(
                StandardJWTPayload(
                  issuer = readOptionalString(propIss, fields),
                  participantId = Some(participantId),
                  userId = readString(propSub, fields), // guarded by if-clause above
                  exp = readInstant(propExp, fields),
                  format = StandardJWTTokenFormat.Audience,
                  audiences =
                    List.empty, // we do not read or extract audience claims for ParticipantId-based tokens
                  scope = readAndCombineScopes(fields),
                )
              )
            case Nil =>
              Left(
                s"Could not read ${value.noSpaces} as AuthServiceJWTPayload: `aud` must include participantId value prefixed by $audPrefix"
              )
            case _ =>
              Left(
                s"Could not read ${value.noSpaces} as AuthServiceJWTPayload: `aud` must include a single participantId value prefixed by $audPrefix"
              )
          }
        } else if (scopes.contains(scopeLedgerApiFull)) {
          // We support the tokens with scope containing `daml_ledger_api`.
          // `aud` field is interpreted as the participantId and may be validated by the apis authorizer for
          // conformance with actual participantId.
          val participantIdE = audienceValue match {
            case id :: Nil => Right(Some(id))
            case Nil => Right(None)
            case _ =>
              Left(
                s"Could not read ${value.noSpaces} as AuthServiceJWTPayload: `aud` must be empty or a single participantId."
              )
          }
          participantIdE
            .map(participantId =>
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
            )

        } else {
          Left(
            s"Access token with unknown scope \"${scopes.mkString}\". Issue tokens with adjusted or no scope to get rid of this warning."
          )
        }

      case None =>
        Left(
          s"Could not read ${value.noSpaces} as AuthServiceJWTPayload: value is not an object"
        )
    }

  private[this] def readOptionalString(name: String, fields: Map[String, Json]): Option[String] =
    fields.get(name) match {
      case None => None
      case Some(j) if j.isNull => None
      case Some(j) =>
        j.asString.orElse(
          sys.error(s"Could not read ${j.spaces2} as string for $name")
        )
    }

  private[this] def readString(name: String, fields: Map[String, Json]): String =
    fields.get(name) match {
      case Some(j) =>
        j.asString.getOrElse(
          throw new RuntimeException(s"Could not read ${j.noSpaces} as string for $name")
        )
      case _ =>
        throw new RuntimeException(s"Could not read value for $name")
    }

  private[this] def readOptionalStringOrArray(
      name: String,
      fields: Map[String, Json],
  ): List[String] =
    fields.get(name) match {
      case None => List.empty
      case Some(j) if j.isNull => List.empty
      case Some(j) =>
        j.asString
          .map(List(_))
          .orElse(j.asArray.map(readStringList(name, _)))
          .getOrElse(sys.error(s"Could not read ${j.spaces2} as string for $name"))
    }

  private[this] def readScopes(fields: Map[String, Json]): List[String] = {
    // Read the scopes from the "scope" field which contains a string with space separated entries,
    // see https://datatracker.ietf.org/doc/html/rfc8693#name-scope-scopes-claim
    // Otherwise, read from the "scp" field which contains a vector of entries,
    // see https://ldapwiki.com/wiki/Wiki.jsp?page=Scp%20%28Scopes%29%20Claim
    val scopes = fields.get(propScope).toList.flatMap(_.asString.map(_.split(" ")).toList).flatten
    if (scopes.nonEmpty) scopes else readOptionalStringOrArray(propScp, fields)
  }

  private[this] def readAndCombineScopes(fields: Map[String, Json]): Option[String] = {
    val scopes = readScopes(fields)
    scopes.headOption.fold[Option[String]](None)(_ => Some(scopes.mkString(" ")))
  }

  private def readStringList(name: String, values: Vector[Json]) =
    values.toList.map { j =>
      j.asString.getOrElse(
        throw new RuntimeException(s"Could not read ${j.noSpaces} as string element for $name")
      )
    }

  private[this] def readInstant(name: String, fields: Map[String, Json]): Option[Instant] =
    fields.get(name) match {
      case None => None
      case Some(j) if j.isNull => None
      case Some(j) =>
        j.asNumber
          .flatMap(_.toLong)
          .map(Instant.ofEpochSecond)
          .orElse(
            throw new RuntimeException(s"Could not read ${j.spaces2} as epoch seconds for $name")
          )
    }

  // ------------------------------------------------------------------------------------------------------------------
  // Implicits that can be imported to write JSON
  // ------------------------------------------------------------------------------------------------------------------
  object JsonImplicits extends AuthServiceJWTPayloadCodec(writePayload, readPayload)

  object AudienceBasedTokenJsonImplicits
      extends AuthServiceJWTPayloadCodec(writeAudienceBasedPayload, readAudienceBasedToken)

  object ScopeBasedTokenJsonImplicits
      extends AuthServiceJWTPayloadCodec(writeScopeBasedPayload, readScopeBasedToken)

  abstract class AuthServiceJWTPayloadCodec(
      writeToken: AuthServiceJWTPayload => Json,
      readToken: Json => Either[String, AuthServiceJWTPayload],
  ) {
    implicit val authServiceJWTPayloadEncoder: Encoder[AuthServiceJWTPayload] =
      Encoder.instance(writeToken)

    implicit val authServiceJWTPayloadDecoder: Decoder[AuthServiceJWTPayload] =
      Decoder.instance { c =>
        Try(readToken(c.value)) match {
          case Failure(exception) => Left(DecodingFailure(exception.getMessage, Nil))
          case Success(Left(parsingError)) => Left(DecodingFailure(parsingError, Nil))
          case Success(Right(parsedBody)) => Right(parsedBody)
        }
      }
  }
}
