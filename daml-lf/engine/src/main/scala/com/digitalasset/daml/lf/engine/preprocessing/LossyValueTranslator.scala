// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data.Ref.IdString
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.{ArrayList, SValue}
import com.daml.lf.value.Value
import com.daml.lf.value.Value._

/** This translator is only intended for use where a SValue is needed for error reporting but
  * only a Value is available. This can be the case for [[com.daml.lf.transaction.SharedGlobalKey]]
  * where the package-id may not be available. Once there are other ways to resolve missing package-ids,
  * for example issue #17646, this will longer be necessary.
  */
object LossyValueTranslator {

  private[preprocessing] val lossyConversionIdentifier: Ref.Identifier =
    Ref.Identifier.assertFromString("Lossy:Conversion:Id")
  private[preprocessing] val lossyConversionVariantRank: Int = -1
  private[preprocessing] val lossyConversionFieldPrefix: String = "Lossy_Field_"

  private final case class ValueTy(value: SValue, ty: Type)

  @throws[Error.Preprocessing.Error]
  def unsafeTranslate(lf: Value): (SValue, Type) = {
    val ValueTy(value, ty) = translate(lf)
    (value, ty)
  }

  private def translate(value: Value): ValueTy = {

    def vty(value: SValue, ty: Type): ValueTy = ValueTy(value, ty)
    def bty(value: SValue, bt: BuiltinType): ValueTy = ValueTy(value, TBuiltin(bt))

    def go(value0: Value, nesting: Int = 0): ValueTy = {
      if (nesting > Value.MAXIMUM_NESTING) {
        throw Error.Preprocessing.ValueNesting(value)
      } else {
        val newNesting = nesting + 1

        def typeError(msg: String = s"Unable to translate value: $value") =
          throw Error.Preprocessing.Internal("LossyTranslate", msg, None)

        value0 match {
          case ValueUnit => bty(SValue.SUnit, BTUnit)
          case ValueBool(b) => bty(if (b) SValue.SValue.True else SValue.SValue.False, BTBool)
          case ValueInt64(i) => bty(SValue.SInt64(i), BTInt64)
          case ValueTimestamp(t) => bty(SValue.STimestamp(t), BTTimestamp)
          case ValueDate(t) => bty(SValue.SDate(t), BTDate)
          case ValueText(t) => bty(SValue.SText(t), BTText)
          case ValueParty(p) => bty(SValue.SParty(p), BTParty)

          case ValueContractId(c) =>
            vty(
              SValue.SContractId(c),
              TApp(TBuiltin(BTContractId), TTyCon(lossyConversionIdentifier)),
            )

          case ValueNumeric(d) =>
            Numeric.fromBigDecimal(TNat.Decimal.n, d) match {
              case Right(value) =>
                vty(SValue.SNumeric(value), TApp(TBuiltin(BTNumeric), TNat.Decimal))
              case Left(message) => typeError(message)
            }

          case ValueOptional(mbValue) =>
            mbValue match {
              case None =>
                vty(SValue.SValue.None, TBuiltin(BTOptional))
              case Some(x) =>
                val vt = go(x, newNesting)
                vty(SValue.SOptional(Some(vt.value)), TApp(TBuiltin(BTOptional), vt.ty))
            }

          case ValueList(ls) =>
            val list = ls.map(go(_, newNesting))
            list.iterator.nextOption() match {
              case None => vty(SValue.SValue.EmptyList, TBuiltin(BTList))
              case Some(ref) => vty(SValue.SList(list.map(_.value)), TApp(TBuiltin(BTList), ref.ty))
            }

          case ValueTextMap(entries) =>
            entries.iterator.nextOption() match {
              case None =>
                vty(SValue.SValue.EmptyTextMap, TBuiltin(BTTextMap))
              case Some((_, refVal)) =>
                val refTy = go(refVal, newNesting).ty
                vty(
                  SValue.SMap(
                    isTextMap = true,
                    entries = entries.iterator.map { case (k, v) =>
                      SValue.SText(k) -> go(v, newNesting).value
                    },
                  ),
                  TApp(TBuiltin(BTTextMap), refTy),
                )
            }

          case ValueGenMap(entries) =>
            entries.iterator.nextOption() match {
              case None =>
                vty(
                  SValue.SValue.EmptyGenMap,
                  TBuiltin(BTGenMap),
                )
              case Some((kr, vr)) =>
                val refK = go(kr, newNesting).ty
                val refV = go(vr, newNesting).ty
                vty(
                  SValue.SMap(
                    isTextMap = false,
                    entries = entries.iterator.map { case (k, v) =>
                      go(k, newNesting).value -> go(v, newNesting).value
                    },
                  ),
                  TApp(TApp(TBuiltin(BTGenMap), refK), refV),
                )
            }

          case ValueVariant(mbId, constructorName, val0) =>
            val vt = go(val0, newNesting)
            // This cannot work in the general case as there is no way to derive all the types that
            // are involved in the variant as we only observer one instance
            vty(
              SValue.SVariant(
                mbId.getOrElse(lossyConversionIdentifier),
                constructorName,
                lossyConversionVariantRank,
                vt.value,
              ),
              TApp(TTyCon(lossyConversionIdentifier), vt.ty),
            )

          case ValueRecord(mTyCon, sourceElements) =>
            val flatNames = sourceElements.iterator.flatMap(_._1).toList
            val names = flatNames.length match {
              case 0 =>
                (1 to sourceElements.length).map(i =>
                  IdString.Name.assertFromString(s"$lossyConversionFieldPrefix$i")
                )
              case sourceElements.length => flatNames
              case _ => typeError()
            }
            val vts = sourceElements.iterator.map(_._2).map(go(_, newNesting)).toList
            val types = vts.map(_.ty).foldLeft[Type](TTyCon(lossyConversionIdentifier)) {
              case (it, t) => TApp(it, t)
            }
            vty(
              SValue.SRecord(
                mTyCon.getOrElse(lossyConversionIdentifier),
                ImmArray.from(names),
                vts.map(_.value).to(ArrayList),
              ),
              types,
            )

          case ValueEnum(mbId, constructor) =>
            vty(
              SValue.SEnum(
                mbId.getOrElse(lossyConversionIdentifier),
                constructor,
                lossyConversionVariantRank,
              ),
              TTyCon(lossyConversionIdentifier),
            )

          case _ => typeError()
        }

      }
    }

    go(value)

  }

}
