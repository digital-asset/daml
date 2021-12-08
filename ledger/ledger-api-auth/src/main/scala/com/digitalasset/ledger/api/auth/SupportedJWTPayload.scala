// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import com.daml.ledger.api.auth.AuthServiceJWTCodec.{
  readPayload,
  readStandardTokenPayload,
  writePayload,
  writeStandardTokenPayload,
}
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonFormat}

/** A wrapper class to add support for standard JWT tokens alongside the existing custom tokens in a minimally invasive way.
  *
  * The main problem is that the [[AuthServiceJWTPayload]] class is used by other applications
  * like the JSON-API to parse tokens; and we don't want to meddle with that code as part of the PoC.
  */
// TODO (i12049): clarify naming and inline case classes where possible.
sealed trait SupportedJWTPayload {}
final case class CustomDamlJWTPayload(payload: AuthServiceJWTPayload) extends SupportedJWTPayload
final case class StandardJWTPayload(payload: AuthServiceJWTPayload) extends SupportedJWTPayload

/** JSON codecs for all the formats of JWTs supported by the [[AuthServiceJWT]]. */
object SupportedJWTCodec {

  /** Writes the given payload to a compact JSON string */
  def compactPrint(v: SupportedJWTPayload): String =
    JsonImplicits.SupportedJWTPayloadFormat.write(v).compactPrint

  // ------------------------------------------------------------------------------------------------------------------
  // Implicits that can be imported to write JSON
  // ------------------------------------------------------------------------------------------------------------------
  object JsonImplicits extends DefaultJsonProtocol {
    implicit object SupportedJWTPayloadFormat extends RootJsonFormat[SupportedJWTPayload] {
      override def write(v: SupportedJWTPayload): JsValue = v match {
        case CustomDamlJWTPayload(payload) => writePayload(payload)
        case StandardJWTPayload(payload) => writeStandardTokenPayload(payload)
      }
      override def read(json: JsValue): SupportedJWTPayload =
        readStandardTokenPayload(json) match {
          case Some(payload) => StandardJWTPayload(payload)
          case None => CustomDamlJWTPayload(readPayload(json))
        }
    }
  }
}
