// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http
package query

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.ScalazEqual._
import com.digitalasset.daml.lf.iface

import scalaz.\&/
import spray.json._

sealed abstract class ValuePredicate[+Ty, +LfV] extends Product with Serializable

object ValuePredicate {
  type TypeLookup = Ref.Identifier => Option[iface.DefDataType.FWT]

  final case class Literal[+Ty, +LfV](literal: LfV, typ: Ty) extends ValuePredicate[Ty, LfV]
  final case class RecordSubset[+Ty, +LfV](fields: Map[String, ValuePredicate[Ty, LfV]])
      extends ValuePredicate[Ty, LfV]
  // boolean is whether inclusive (lte vs lt)
  final case class Range[+Ty, +LfV](ltgt: (Boolean, LfV) \&/ (Boolean, LfV), typ: Ty) extends ValuePredicate[Ty, LfV]

  def fromJsObject(it: Map[String, JsValue], typ: iface.Type, defs: TypeLookup): ValuePredicate[iface.Type, JsValue] = {
    RecordSubset(Map())
  }

  private[this] def fromPrim(it: JsValue, typ: iface.TypePrim): ValuePredicate[iface.Type, JsValue] = {
    import iface.PrimType._
    def lit = Literal(it, typ)
    (typ.typ, it).match2 {
      case Bool => {case JsBoolean(_) => lit}
      case Int64 => {case JsNumber(_) | JsString(_) => lit}
      case Text => {case JsString(_) => lit}
      case Date => {case JsString(_) => lit}
      case Timestamp => {case JsString(_) => lit}
      case Party => {case JsString(_) => lit}
      case ContractId => {case JsString(_) => lit}
      case List => {case JsArray(_) => lit}
      case Unit => {case JsObject(_) => lit}
      case Optional => {case todo => lit}
      case Map => {case JsObject(_) => lit}
    }(fallback = sys.error("TODO fallback"))
  }
}
