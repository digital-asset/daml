// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import slick.jdbc.{GetResult, SetParameter}

final case class LocalOffset(requestCounter: RequestCounter)
    extends PrettyPrinting
    with Ordered[LocalOffset] {

  override def pretty: Pretty[LocalOffset.this.type] =
    prettyOfClass(param("request counter", _.requestCounter))

  def toLong: Long = requestCounter.unwrap
  override def compare(that: LocalOffset): Int = requestCounter.compare(that.requestCounter)
}

object LocalOffset {
  val MaxValue: LocalOffset = LocalOffset(RequestCounter.MaxValue)

  implicit val setParameterLocalOffset: SetParameter[LocalOffset] = (v, p) => {
    p >> v.requestCounter
  }

  implicit val getResultLocalOffset: GetResult[LocalOffset] = GetResult { r =>
    val requestCounter = r.<<[RequestCounter]

    LocalOffset(requestCounter)
  }
}
