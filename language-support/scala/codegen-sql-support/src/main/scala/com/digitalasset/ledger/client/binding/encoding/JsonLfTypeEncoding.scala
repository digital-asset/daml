// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding
package encoding

import scalaz.{OneAnd, Plus}
import scalaz.std.vector._
import scalaz.syntax.semigroup._
import spray.{json => sj}
import sj.{JsObject, JsString, JsValue, JsonFormat}
import com.digitalasset.ledger.api.v1.{value => rpcvalue}
import com.digitalasset.ledger.client.binding.{Primitive => P}
import scalaz.Leibniz.===

import scala.collection.breakOut

object JsonLfTypeEncoding extends LfTypeEncoding {
  sealed trait Out[A] extends JsonFormat[A] {
    val format: JsonFormat[A]
    override def read(json: JsValue): A = format.read(json)
    override def write(obj: A): JsValue = format.write(obj)
  }
  object Out {
    def apply[A](fmt: JsonFormat[A]): Out[A] = OutJson(fmt)
  }
  final case class OutJson[A](format: JsonFormat[A]) extends Out[A]
  final case class OutUnit[A]()(implicit val isUnit: A === P.Unit) extends Out[A] {
    val format: JsonFormat[A] = isUnit.flip.subst(CustomJsonFormats.unitJsonFormat)
  }

  case class Field[A](name: String, value: Out[A])

  case class RecordFields[A](
      jsonRead: (String PartialFunction JsValue) => A, // JSON record in
      jsonWrite: A => Map[String, JsValue]) // JSON record out

  // Note that DAML-LF allows empty variants, but forbids them in serializable types,
  // which is all we're concerned here. Hence the OneAnd
  type VariantCases[A] = OneAnd[Vector, VariantCase[A]]

  override def record[A](recordId: rpcvalue.Identifier, fi: RecordFields[A]): Out[A] =
    Out(new JsonFormat[A] {
      override def write(obj: A): JsValue = JsObject(fi.jsonWrite(obj))
      override def read(json: JsValue): A = json match {
        case JsObject(map) => fi.jsonRead(map)
        case _ => sj.deserializationError("not a record")
      }
    })

  override def emptyRecord[A](recordId: rpcvalue.Identifier, element: () => A): Out[A] =
    Out(new JsonFormat[A] {
      override def write(obj: A): JsValue = JsObject()
      override def read(json: JsValue): A = json match {
        case JsObject(_) => element()
        case _ => sj.deserializationError("not a record")
      }
    })

  override def field[A](fieldName: String, o: Out[A]): Field[A] = Field(fieldName, o)

  override def fields[A](fi: Field[A]): RecordFields[A] = {
    import spray.json._
    implicit val aev: JsonFormat[A] = fi.value
    RecordFields(
      jsonRead = jso =>
        jso
          .applyOrElse(
            fi.name,
            (_: Any) => sj.deserializationError("no such field", fieldNames = List(fi.name)))
          .convertTo[A],
      jsonWrite = a => Map((fi.name, a.toJson))
    )
  }

  override def variant[A](variantId: rpcvalue.Identifier, cases: VariantCases[A]): Out[A] = {
    val casesVec = cases.head +: cases.tail
    type VariantCaseAux[A0, Inner0] = VariantCase[A0] { type Inner = Inner0 }
    type VariantCaseUnit[B] = VariantCaseAux[B, P.Unit]
    // check if all constructors have unit argument -- if that's the case, we'll encode the
    // variant as an enum.
    val casesUnit: Vector[VariantCaseUnit[A]] = casesVec.collect(Function.unlift { vc =>
      vc.out match {
        case ou: OutUnit[vc.Inner] =>
          Some(ou.isUnit.subst[VariantCaseAux[A, ?]](vc))
        case _ => None
      }
    })
    if (casesUnit.length == casesVec.length) {
      val readCases: Map[String, VariantCaseUnit[A]] =
        casesUnit.map(vc => (vc.name, vc))(breakOut)
      Out(new JsonFormat[A] {
        override def write(obj: A): JsValue =
          casesVec collectFirst {
            Function unlift { vc =>
              vc.select.lift(obj) map (_ => JsString(vc.name))
            }
          } getOrElse sj.serializationError("no case matched obj; that is impossible")

        override def read(json: JsValue): A = json match {
          case JsString(variantCaseName) =>
            val vc: VariantCaseUnit[A] =
              readCases.getOrElse(
                variantCaseName,
                sj.deserializationError(
                  "not a case of this variant",
                  fieldNames = List(variantCaseName)))
            vc.inject(())
          case _ => sj.deserializationError("not an enum string")
        }
      })
    } else {
      val readCases: Map[String, VariantCase[A]] =
        casesVec.map(vc => (vc.name, vc))(breakOut)
      Out(new JsonFormat[A] {
        override def write(obj: A): JsValue =
          casesVec collectFirst {
            Function unlift { vc =>
              vc.select.lift(obj) map (jsv => VariantJson(vc.name, vc.out.write(jsv)))
            }
          } getOrElse sj.serializationError("no case matched obj; that is impossible")

        override def read(json: JsValue): A = json match {
          case VariantJson(variantCaseName, data) =>
            val vc: VariantCase[A] =
              readCases.getOrElse(
                variantCaseName,
                sj.deserializationError(
                  "not a case of this variant",
                  fieldNames = List(variantCaseName)))
            vc.inject(vc.out.read(data))
          case _ => sj.deserializationError("not a variant map")
        }
      })
    }
  }

  override def variantCase[B, A](caseName: String, o: Out[B])(inject: B => A)(
      select: A PartialFunction B): VariantCases[A] = {
    val vc: VariantCase[A] = MkVariantCase[A, B](caseName, inject, select, o)
    OneAnd(vc, Vector.empty)
  }

  object RecordFields extends InvariantApply[RecordFields] {
    override def xmap[A, B](fa: RecordFields[A], f: A => B, g: B => A): RecordFields[B] =
      RecordFields(jsonRead = f compose fa.jsonRead, jsonWrite = fa.jsonWrite compose g)

    override def xmapN[A, B, Z](fa: RecordFields[A], fb: RecordFields[B])(f: (A, B) => Z)(
        g: Z => (A, B)): RecordFields[Z] =
      RecordFields(
        jsonRead = jso => f(fa.jsonRead(jso), fb.jsonRead(jso)),
        jsonWrite = { z =>
          val (a, b) = g(z)
          val ma = fa.jsonWrite(a)
          val mb = fb.jsonWrite(b)
          if ((ma.keySet intersect mb.keySet).nonEmpty)
            sj.serializationError(
              "Something is really wrong, probably a codegen issue. We got duplicate field names in the same record")
          ma ++ mb
        }
      )
  }

  object VariantCases extends Plus[VariantCases] {
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    override def plus[A](a: VariantCases[A], b: => VariantCases[A]): VariantCases[A] = a |+| b
  }

  override val primitive: ValuePrimitiveEncoding[Out] = JsonTypeCodecs

  sealed trait VariantCase[A] {
    type Inner
    val name: String
    val inject: Inner => A
    val select: A PartialFunction Inner
    val out: Out[Inner]
  }
  final case class MkVariantCase[A, B](
      name: String,
      inject: B => A,
      select: A PartialFunction B,
      out: Out[B])
      extends VariantCase[A] { type Inner = B }

  private object VariantJson {
    def apply(caseName: String, value: JsValue): JsObject = JsObject(Map(caseName -> value))

    def unapply(jsObject: JsObject): Option[(String, JsValue)] = jsObject.fields.toSeq match {
      case Seq(a) => Some(a)
      case _ => None
    }
  }

}
