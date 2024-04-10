// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.lf
import com.daml.lf.typesig
import JsValueToApiValueConverter.LfTypeLookup
import JsonProtocol.LfValueCodec
import com.daml.ledger.api.{v2 => lav2}
import com.digitalasset.canton.http.domain
import com.digitalasset.canton.ledger.api.util.LfEngineToApi
import scalaz.std.string._
import scalaz.{-\/, \/, \/-}
import spray.json.JsValue

class JsValueToApiValueConverter(lfTypeLookup: LfTypeLookup) {
  import com.digitalasset.canton.http.util.ErrorOps._

  def jsValueToLfValue(
      lfId: lf.data.Ref.Identifier,
      jsValue: JsValue,
  ): JsonError \/ lf.value.Value =
    \/.attempt(
      LfValueCodec.jsValueToApiValue(jsValue, lfId, lfTypeLookup)
    )(identity).liftErr(JsonError)

  def jsValueToLfValue(
      lfType: typesig.Type,
      jsValue: JsValue,
  ): JsonError \/ lf.value.Value =
    \/.attempt(
      LfValueCodec.jsValueToApiValue(jsValue, lfType, lfTypeLookup)
    )(identity).liftErr(JsonError)

  def jsValueToApiValue(lfType: domain.LfType, jsValue: JsValue): JsonError \/ lav2.value.Value =
    for {
      lfValue <- jsValueToLfValue(lfType, jsValue)
      apiValue <- JsValueToApiValueConverter.lfValueToApiValue(lfValue)
    } yield apiValue
}

object JsValueToApiValueConverter {
  import com.digitalasset.canton.http.util.ErrorOps._

  type LfTypeLookup = lf.data.Ref.Identifier => Option[lf.typesig.DefDataType.FWT]

  def lfValueToApiValue(lfValue: domain.LfValue): JsonError \/ lav2.value.Value =
    \/.fromEither(LfEngineToApi.lfValueToApiValue(verbose = true, lfValue)).liftErr(JsonError)

  def mustBeApiRecord(a: lav2.value.Value): JsonError \/ lav2.value.Record = a.sum match {
    case lav2.value.Value.Sum.Record(b) => \/-(b)
    case _ => -\/(JsonError(s"Expected ${classOf[lav2.value.Value.Sum.Record]}, got: $a"))
  }
}
