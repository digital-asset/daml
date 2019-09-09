// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http
package query

import scalaz.\&/
import spray.json.{JsObject, JsValue}

sealed abstract class ValuePredicate[+LfV] extends Product with Serializable

object ValuePredicate {
  final case class Literal[+LfV](literal: LfV) extends ValuePredicate[LfV]
  final case class RecordSubset[+LfV](fields: Map[String, ValuePredicate[LfV]])
      extends ValuePredicate[LfV]
  // boolean is whether inclusive (lte vs lt)
  final case class Range[+LfV](ltgt: (Boolean, LfV) \&/ (Boolean, LfV)) extends ValuePredicate[LfV]

  def fromJsObject(it: Map[String, JsValue]): ValuePredicate[JsValue] =
    Literal(JsObject())
}
