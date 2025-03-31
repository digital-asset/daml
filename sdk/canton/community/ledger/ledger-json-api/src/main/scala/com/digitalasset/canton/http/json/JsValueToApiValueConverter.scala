// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.ledger.api.v2 as lav2
import com.digitalasset.canton.http
import com.digitalasset.canton.ledger.api.util.LfEngineToApi
import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.typesig
import scalaz.std.string.*
import scalaz.{-\/, \/, \/-}
import spray.json.JsValue

import JsValueToApiValueConverter.LfTypeLookup
import JsonProtocol.LfValueCodec

class JsValueToApiValueConverter(
    lfTypeLookup: LfTypeLookup,
    resolvePackageName: Ref.PackageId => Ref.PackageName,
) {
  import com.digitalasset.canton.http.util.ErrorOps.*

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

  def jsValueToApiValue(lfType: http.LfType, jsValue: JsValue): JsonError \/ lav2.value.Value =
    for {
      lfValue <- jsValueToLfValue(lfType, jsValue)
      apiValue <- JsValueToApiValueConverter.lfValueToApiValue(lfValue, resolvePackageName)
    } yield apiValue
}

object JsValueToApiValueConverter {
  import com.digitalasset.canton.http.util.ErrorOps.*

  type LfTypeLookup = lf.data.Ref.Identifier => Option[lf.typesig.DefDataType.FWT]

  def lfValueToApiValue(
      lfValue: http.LfValue,
      resolvePackageName: Ref.PackageId => Ref.PackageName,
  ): JsonError \/ lav2.value.Value =
    \/.fromEither(LfEngineToApi.lfValueToApiValue(verbose = true, lfValue, resolvePackageName))
      .liftErr(JsonError)

  def mustBeApiRecord(a: lav2.value.Value): JsonError \/ lav2.value.Record = a.sum match {
    case lav2.value.Value.Sum.Record(b) => \/-(b)
    case _ => -\/(JsonError(s"Expected ${classOf[lav2.value.Value.Sum.Record]}, got: $a"))
  }
}
