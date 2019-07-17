// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.value.json.ApiCodecCompressed
import com.digitalasset.http.util.ApiValueToLfValueConverter
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.\/
import scalaz.syntax.show._
import spray.json.JsValue

object ApiValueToJsValueConverter {

  def apiValueToJsValue(apiToLf: ApiValueToLfValueConverter.ApiValueToLfValue)(
      a: lav1.value.Value): JsonError \/ JsValue =
    apiToLf(a)
      .map { b: lf.value.Value[lf.value.Value.AbsoluteContractId] =>
        ApiCodecCompressed.apiValueToJsValue(lfValueOfString(b))
      }
      .leftMap(x => JsonError(x.shows))

  private def lfValueOfString(
      lfValue: lf.value.Value[lf.value.Value.AbsoluteContractId]): lf.value.Value[String] =
    lfValue.mapContractId(x => x.coid)
}
