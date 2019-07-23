// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import akka.util.ByteString
import com.digitalasset.daml.lf
import com.digitalasset.http.domain.HasTemplateId
import com.digitalasset.http.util.DamlLfIdentifiers
import com.digitalasset.http.{Services, domain}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{Traverse, \/}
import spray.json.{JsValue, JsonReader}

import scala.language.higherKinds

class DomainJsonDecoder(
    resolveTemplateId: Services.ResolveTemplateId,
    jsValueToApiValue: (lf.data.Ref.Identifier, JsValue) => JsonError \/ lav1.value.Value) {

  def decode[F[_]](a: ByteString)(
      implicit ev1: JsonReader[F[JsValue]],
      ev2: Traverse[F],
      ev3: domain.HasTemplateId[F]): JsonError \/ F[lav1.value.Value] =
    for {
      b <- SprayJson.parse[F[JsValue]](a)(ev1).leftMap(e => JsonError(e.shows))
      c <- decodeValues(b)
    } yield c

  // decode underlying values that require resolved templateID
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decodeValues[F[_]: Traverse: domain.HasTemplateId](
      fa: F[JsValue]): JsonError \/ F[lav1.value.Value] =
    for {
      templateId <- lookupTemplateId(fa)
      damlLfId = DamlLfIdentifiers.damlLfIdentifier(templateId)
      apiValue <- fa.traverse(jsValue => jsValueToApiValue(damlLfId, jsValue))
    } yield apiValue

  private def lookupTemplateId[F[_]: domain.HasTemplateId](
      fa: F[_]): JsonError \/ lar.TemplateId = {
    val H: HasTemplateId[F] = implicitly
    val templateId: domain.TemplateId.OptionalPkg = H.templateId(fa)
    resolveTemplateId(templateId).leftMap(e => JsonError(e.shows))
  }
}
