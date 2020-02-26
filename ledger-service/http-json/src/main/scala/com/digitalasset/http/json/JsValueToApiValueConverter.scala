// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.iface
import com.digitalasset.http.domain
import com.digitalasset.http.json.JsValueToApiValueConverter.LfTypeLookup
import com.digitalasset.http.json.JsonProtocol.LfValueCodec
import com.digitalasset.ledger.api.{v1 => lav1}
import com.digitalasset.platform.participant.util.LfEngineToApi
import scalaz.std.string._
import scalaz.{-\/, \/, \/-}
import spray.json.{JsObject, JsValue}

class JsValueToApiValueConverter(lfTypeLookup: LfTypeLookup) {
  import com.digitalasset.http.util.ErrorOps._

  def jsValueToLfValue(
      lfId: lf.data.Ref.Identifier,
      jsValue: JsValue): JsonError \/ lf.value.Value[lf.value.Value.AbsoluteContractId] =
    \/.fromTryCatchNonFatal(
      LfValueCodec.jsValueToApiValue(jsValue, lfId, lfTypeLookup)
    ).liftErr(JsonError)

  def jsValueToLfValue(
      lfType: iface.Type,
      jsValue: JsValue): JsonError \/ lf.value.Value[lf.value.Value.AbsoluteContractId] =
    \/.fromTryCatchNonFatal(
      LfValueCodec.jsValueToApiValue(jsValue, lfType, lfTypeLookup)
    ).liftErr(JsonError)

  def jsValueToApiValue(lfType: domain.LfType, jsValue: JsValue): JsonError \/ lav1.value.Value =
    for {
      lfValue <- jsValueToLfValue(lfType, jsValue)
      apiValue <- JsValueToApiValueConverter.lfValueToApiValue(lfValue)
    } yield apiValue

  def jsObjectToApiRecord(
      lfType: domain.LfType,
      jsObject: JsObject): JsonError \/ lav1.value.Record =
    for {
      a <- jsValueToApiValue(lfType, jsObject)
      b <- mustBeApiRecord(a)
    } yield b

  private def mustBeApiRecord(a: lav1.value.Value): JsonError \/ lav1.value.Record = a.sum match {
    case lav1.value.Value.Sum.Record(b) => \/-(b)
    case _ => -\/(JsonError(s"Expected ${classOf[lav1.value.Value.Sum.Record]}, got: $a"))
  }
}

object JsValueToApiValueConverter {
  import com.digitalasset.http.util.ErrorOps._

  type LfTypeLookup = lf.data.Ref.Identifier => Option[lf.iface.DefDataType.FWT]

  def lfValueToApiValue(lfValue: domain.LfValue): JsonError \/ lav1.value.Value =
    \/.fromEither(LfEngineToApi.lfValueToApiValue(verbose = true, lfValue)).liftErr(JsonError)
}
