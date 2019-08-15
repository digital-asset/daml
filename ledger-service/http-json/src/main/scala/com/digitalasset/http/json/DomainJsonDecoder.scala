// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.daml.lf
import com.digitalasset.http.domain.HasTemplateId
import com.digitalasset.http.{PackageService, domain}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{Traverse, \/}
import spray.json.{JsObject, JsValue, JsonReader}

import scala.language.higherKinds

class DomainJsonDecoder(
    resolveTemplateId: PackageService.ResolveTemplateId,
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
      damlLfId <- lookupLfIdentifier(fa)
      apiValue <- fa.traverse(jsObject => jsObjectToApiRecord(damlLfId, jsObject))
    } yield apiValue
  }

  def decodeV[F[_]](a: String)(
      implicit ev1: JsonReader[F[JsValue]],
      ev2: Traverse[F],
      ev3: domain.HasTemplateId[F]): JsonError \/ F[lav1.value.Value] =
    for {
      b <- SprayJson.parse(a).leftMap(e => JsonError(e.shows))
      d <- decodeV(b)
    } yield d

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
      damlLfId <- lookupLfIdentifier(fa)
      apiValue <- fa.traverse(jsValue => jsValueToApiValue(damlLfId, jsValue))
    } yield apiValue
  }

  private def lookupLfIdentifier[F[_]: domain.HasTemplateId](
      fa: F[_]): JsonError \/ lf.data.Ref.Identifier = {
    val H: HasTemplateId[F] = implicitly
    val templateId: domain.TemplateId.OptionalPkg = H.templateId(fa)
    resolveTemplateId(templateId)
      .map(x => H.lfIdentifier(fa, x))
      .leftMap(e => JsonError("lookupLfIdentifier: " + e.shows))
  }
}
