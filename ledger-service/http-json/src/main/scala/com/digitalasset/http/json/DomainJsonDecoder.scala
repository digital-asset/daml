// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.daml.lf
import com.digitalasset.http.domain.HasTemplateId
import com.digitalasset.http.util.IdentifierConverters
import com.digitalasset.http.{Services, domain}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{Traverse, \/}
import spray.json.{JsObject, JsValue, JsonReader}

import scala.language.higherKinds

class DomainJsonDecoder(
    resolveTemplateId: Services.ResolveTemplateId,
    jsObjectToApiRecord: (lf.data.Ref.Identifier, JsObject) => JsonError \/ lav1.value.Record,
    jsValueToApiValue: (lf.data.Ref.Identifier, JsValue) => JsonError \/ lav1.value.Value) {

  def decodeR[F[_]](a: String)(
      implicit ev1: JsonReader[F[JsObject]],
      ev2: Traverse[F],
      ev3: domain.HasTemplateId[F]): JsonError \/ F[lav1.value.Record] =
    for {
      b <- SprayJson.parse(a).leftMap(e => JsonError(e.shows))
      c <- SprayJson.mustBeJsObject(b)
      d <- decodeR(c)
    } yield d

  def decodeR[F[_]](a: JsObject)(
      implicit ev1: JsonReader[F[JsObject]],
      ev2: Traverse[F],
      ev3: domain.HasTemplateId[F]): JsonError \/ F[lav1.value.Record] =
    for {
      b <- SprayJson.decode[F[JsObject]](a)(ev1).leftMap(e => JsonError(e.shows))
      c <- decodeUnderlyingRecords(b)
    } yield c

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decodeUnderlyingRecords[F[_]: Traverse: domain.HasTemplateId](
      fa: F[JsObject]): JsonError \/ F[lav1.value.Record] = {
    for {
      templateId <- lookupTemplateId(fa)
      damlLfId = IdentifierConverters.damlLfIdentifier(templateId)
      apiValue <- fa.traverse(jsObject => jsObjectToApiRecord(damlLfId, jsObject))
    } yield apiValue
  }

  def decodeV[F[_]](a: JsValue)(
      implicit ev1: JsonReader[F[JsValue]],
      ev2: Traverse[F],
      ev3: domain.HasTemplateId[F]): JsonError \/ F[lav1.value.Value] =
    for {
      b <- SprayJson.decode[F[JsValue]](a)(ev1).leftMap(e => JsonError(e.shows))
      c <- decodeUnderlyingValues(b)
    } yield c

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decodeUnderlyingValues[F[_]: Traverse: domain.HasTemplateId](
      fa: F[JsValue]): JsonError \/ F[lav1.value.Value] = {
    for {
      templateId <- lookupTemplateId(fa)
      damlLfId = IdentifierConverters.damlLfIdentifier(templateId)
      apiValue <- fa.traverse(jsValue => jsValueToApiValue(damlLfId, jsValue))
    } yield apiValue
  }

  private def lookupTemplateId[F[_]: domain.HasTemplateId](
      fa: F[_]): JsonError \/ lar.TemplateId = {
    val H: HasTemplateId[F] = implicitly
    val templateId: domain.TemplateId.OptionalPkg = H.templateId(fa)
    resolveTemplateId(templateId).leftMap(e => JsonError(e.shows))
  }
}
