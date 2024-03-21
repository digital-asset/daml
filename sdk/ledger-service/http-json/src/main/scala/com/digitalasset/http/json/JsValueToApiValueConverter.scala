// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.json

import com.daml.lf
import com.daml.lf.iface
import com.daml.http.domain
import com.daml.http.json.JsValueToApiValueConverter.LfTypeLookup
import com.daml.http.json.JsonProtocol.LfValueCodec
import com.daml.ledger.api.{v1 => lav1}
import com.daml.platform.participant.util.LfEngineToApi
import scalaz.std.string._
import scalaz.{-\/, \/, \/-}
import spray.json.JsValue

class JsValueToApiValueConverter(lfTypeLookup: LfTypeLookup) {
  import com.daml.http.util.ErrorOps._

  def jsValueToLfValue(
      lfId: lf.data.Ref.Identifier,
      jsValue: JsValue,
  ): JsonError \/ lf.value.Value =
    \/.attempt(
      LfValueCodec.jsValueToApiValue(jsValue, lfId, lfTypeLookup)
    )(identity).liftErr(JsonError)

  def jsValueToLfValue(
      lfType: iface.Type,
      jsValue: JsValue,
  ): JsonError \/ lf.value.Value =
    \/.attempt(
      LfValueCodec.jsValueToApiValue(jsValue, lfType, lfTypeLookup)
    )(identity).liftErr(JsonError)

  def jsValueToApiValue(lfType: domain.LfType, jsValue: JsValue): JsonError \/ lav1.value.Value =
    for {
      lfValue <- jsValueToLfValue(lfType, jsValue)
      apiValue <- JsValueToApiValueConverter.lfValueToApiValue(lfValue)
    } yield apiValue
}

object JsValueToApiValueConverter {
  import com.daml.http.util.ErrorOps._

  type LfTypeLookup = lf.data.Ref.Identifier => Option[lf.iface.DefDataType.FWT]

  def lfValueToApiValue(lfValue: domain.LfValue): JsonError \/ lav1.value.Value =
    \/.fromEither(LfEngineToApi.lfValueToApiValue(verbose = true, lfValue)).liftErr(JsonError)

  def mustBeApiRecord(a: lav1.value.Value): JsonError \/ lav1.value.Record = a.sum match {
    case lav1.value.Value.Sum.Record(b) => \/-(b)
    case _ => -\/(JsonError(s"Expected ${classOf[lav1.value.Value.Sum.Record]}, got: $a"))
  }
}
