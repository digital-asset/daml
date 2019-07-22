// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.value.json.ApiCodecCompressed
import com.digitalasset.ledger.api.{v1 => lav1}
import com.digitalasset.platform.participant.util.LfEngineToApi
import scalaz.\/
import spray.json.JsValue

object JsValueToApiValueConverter {

  type LfTypeLookup = lf.data.Ref.Identifier => Option[lf.iface.DefDataType.FWT]

  def jsValueToApiValue(lfTypeLookup: LfTypeLookup)(
      lfId: lf.data.Ref.Identifier,
      jsValue: JsValue): JsonError \/ lav1.value.Value = {

    for {
      lfValue <- \/.fromTryCatchNonFatal(
        ApiCodecCompressed.jsValueToApiValue(jsValue, lfId, lfTypeLookup))
        .leftMap(JsonError.toJsonError)
        .map(toAbsoluteContractId)

      apiValue <- \/.fromEither(LfEngineToApi.lfValueToApiValue(verbose = true, lfValue))
        .leftMap(JsonError.toJsonError)
    } yield apiValue
  }

  private def toAbsoluteContractId(
      a: lf.value.Value[String]): lf.value.Value[lf.value.Value.AbsoluteContractId] =
    a.mapContractId(cid =>
      lf.value.Value.AbsoluteContractId(lf.data.Ref.ContractIdString.assertFromString(cid)))
}
