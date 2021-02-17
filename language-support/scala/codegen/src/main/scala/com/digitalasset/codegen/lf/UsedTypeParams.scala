// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.lf

import com.daml.codegen.lf.DamlDataTypeGen.{DataType, VariantField}
import com.daml.lf.data.Ref
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.iface
import scalaz.Monoid
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.syntax.bifoldable._
import scalaz.syntax.foldable._
import scalaz.syntax.monoid._

object UsedTypeParams {

  /** Returns only type parameters that specified in fields, not relying on `DataType.typeVars`.
    */
  def collectTypeParamsInUse(typeDecl: DataType): Set[String] =
    foldMapGenTypes(typeDecl)(collectTypeParams)

  private def foldMapGenTypes[Z: Monoid](typeDecl: DataType)(f: iface.Type => Z): Z = {
    val notAGT = (_: String) => mzero[Z]
    (typeDecl: ScopedDataType[iface.DataType[iface.Type, VariantField]])
      .foldMap(_.bifoldMap(f)(_.bifoldMap(_ foldMap (_.bifoldMap(notAGT)(f)))(f)))
  }

  private[this] def collectTypeParams[S >: Ref.Name](field: iface.Type): Set[S] = field match {
    case iface.TypeVar(x) => Set(x)
    case iface.TypePrim(_, xs) => xs.toSet.flatMap(collectTypeParams)
    case iface.TypeCon(_, xs) => xs.toSet.flatMap(collectTypeParams)
    case iface.TypeNumeric(_) => Set.empty
  }

  final case class LookupType[+RF, +VF](
      run: iface.TypeConName => Option[(ScopedDataType.DT[RF, VF], LookupType[RF, VF])]
  )

  /** Variance of `sdt.typeVars` in order. */
  def covariantVars[RF <: iface.Type, VF <: iface.Type](
      sdt: ScopedDataType.DT[RF, VF],
      lookupType: LookupType[RF, VF],
  ): ImmArraySeq[Variance] = {
    import iface._, Variance._
    def goType(typ: iface.Type): Map[Ref.Name, Variance] = typ match {
      case TypeVar(name) => Map(name -> Covariant)

      case TypePrim(pt, typArgs) =>
        import PrimType._
        pt match {
          case GenMap =>
            val Seq(kt, vt) = typArgs
            // we don't need to inspect `kt` any further than enumerating it;
            // every occurrence therein is invariant
            goType(vt) |+| collectTypeParams(kt).view.map((_, Invariant)).toMap
          case Bool | Int64 | Text | Date | Timestamp | Party | ContractId | List | Unit |
              Optional | TextMap =>
            typArgs foldMap goType
        }

      case TypeCon(tcName, typArgs) =>
        val (refSdt, refLookupType @ _) =
          lookupType.run(tcName) getOrElse sys.error(s"$tcName not found")
        // TODO must use refLookupType in goSdt's dynamic scope
        goSdt(refSdt).zip(typArgs).foldMap { case (pVariance, aContents) =>
          val aPositions = goType(aContents)
          pVariance match {
            case Covariant => aPositions
            case Invariant => aPositions.transform((_, _) => Invariant)
          }
        }

      case TypeNumeric(_) => Map.empty
    }

    def goSdt(sdt: ScopedDataType.DT[RF, VF]): ImmArraySeq[Variance] = {
      val vLookup: Map[Ref.Name, Variance] = sdt.dataType match {
        case Record(fields) => fields foldMap { case (_, typ) => goType(typ) }
        case Variant(fields) => fields foldMap { case (_, typ) => goType(typ) }
        case Enum(_) => Map.empty
      }
      sdt.typeVars map (vLookup.getOrElse(_, Covariant))
    }

    goSdt(sdt)
  }

  sealed abstract class Variance extends Product with Serializable
  object Variance {
    case object Covariant extends Variance
    case object Invariant extends Variance

    implicit val `variance conjunction monoid`: Monoid[Variance] = Monoid.instance(
      {
        case (Covariant, Covariant) => Covariant
        case _ => Invariant
      },
      Covariant,
    )
  }
}
