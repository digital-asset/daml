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
  final case class Range[+Ty, +LfV](ltgt: (Boolean, LfV) \&/ (Boolean, LfV), typ: Ty)
      extends ValuePredicate[Ty, LfV]

  def fromJsObject(
      it: Map[String, JsValue],
      typ: iface.Type,
      defs: TypeLookup): ValuePredicate[iface.Type, JsValue] = {
    type Result = ValuePredicate[iface.Type, JsValue]

    def fromValue(it: JsValue, typ: iface.Type): Result =
      (typ, it).match2 {
        case p @ iface.TypePrim(_, _) => { case _ => fromPrim(it, p) }
        case tc @ iface.TypeCon(iface.TypeConName(id), _) => {
          case _ =>
            val ddt = defs(id).getOrElse(sys.error(s"Type $id not found"))
            fromCon(it, id, tc instantiate ddt)
        }
        case iface.TypeNumeric(_) => { case JsString(_) | JsNumber(_) => Literal(it, typ) }
        case iface.TypeVar(_) => sys.error("no vars allowed!")
      }(fallback = ???)

    def fromCon(it: JsValue, id: Ref.Identifier, typ: iface.DataType.FWT): Result =
      (typ, it).match2 {
        case iface.Record(fieldTyps) => {
          case JsObject(fields) =>
            val lookup = fieldTyps.toMap[String, iface.Type]
            RecordSubset(fields transform { (fName, fSpec) =>
              fromValue(
                fSpec,
                lookup.getOrElse(
                  fName,
                  sys.error(s"no field $fName in ${id.qualifiedName.qualifiedName}")))
            })
        }
        case iface.Variant(fieldTyps) => ???
        case e @ iface.Enum(_) => {
          case JsString(s) => fromEnum(s, e)
        }
      }(fallback = ???)

    def fromEnum(it: String, typ: iface.Enum): Result =
      if (typ.constructors contains it) ??? else sys.error("not a member of the enum")

    def fromPrim(it: JsValue, typ: iface.TypePrim): Result = {
      import iface.PrimType._
      def lit = Literal(it, typ)
      (typ.typ, it).match2 {
        case Bool => { case JsBoolean(_) => lit }
        case Int64 => { case JsNumber(_) | JsString(_) => lit }
        case Text => { case JsString(_) => lit }
        case Date => { case JsString(_) => lit }
        case Timestamp => { case JsString(_) => lit }
        case Party => { case JsString(_) => lit }
        case ContractId => { case JsString(_) => lit }
        case List => { case JsArray(_) => lit }
        case Unit => { case JsObject(_) => lit }
        case Optional => { case todo => lit }
        case Map => { case JsObject(_) => lit }
      }(fallback = sys.error("TODO fallback"))
    }

    RecordSubset(Map()) // TODO
  }
}
