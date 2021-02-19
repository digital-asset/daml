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
import scalaz.syntax.std.map._

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

  import VarianceConstraint.{BaseResolution, DelayedResolution}

  /** Variance of `sdt.typeVars` in order. */
  def covariantVars[RF <: iface.Type, VF <: iface.Type](
      sdt: ScopedDataType.DT[RF, VF],
      lookupType: LookupType[RF, VF],
  ): ImmArraySeq[Variance] = {
    import iface._, Variance._
    def goType(typ: iface.Type): VarianceConstraint = typ match {
      case TypeVar(name) =>
        // while we default to Covariant at a later step,
        // absence *does not* mean set-to-Covariant at this step
        VarianceConstraint.base(Map(sdt.name -> Map(name -> Covariant)))

      case TypePrim(pt, typArgs) =>
        import PrimType.{Map => _, _}
        pt match {
          case GenMap =>
            val Seq(kt, vt) = typArgs
            // we don't need to inspect `kt` any further than enumerating it;
            // every occurrence therein is invariant
            goType(vt) |+| VarianceConstraint.base(
              Map(sdt.name -> collectTypeParams(kt).view.map((_, Invariant)).toMap)
            )
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
            case Invariant =>
              aPositions.copy(resolutions =
                aPositions.resolutions.transform((_, m) => m.transform((_, _) => Invariant))
              )
          }
        }

      case TypeNumeric(_) => VarianceConstraint.base(Map.empty)
    }

    def goSdt(sdt: ScopedDataType.DT[RF, VF]): DelayedResolution = {
      val vLookup = sdt.dataType match {
        case Record(fields) => fields foldMap { case (_, typ) => goType(typ) }
        case Variant(fields) => fields foldMap { case (_, typ) => goType(typ) }
        case Enum(_) => mzero[VarianceConstraint]
      }
      Map(sdt.name -> (sdt.typeVars, vLookup))
    }

    goSdt(sdt)
  }

  // an implementation tool for covariantVars
  private[this] final case class VarianceConstraint(
      resolutions: BaseResolution,
      delayedConstraints: DelayedResolution,
  ) {
    def solve: BaseResolution = ???
  }

  private[this] object VarianceConstraint {
    type BaseResolution = Map[ScopedDataType.Name, Map[Ref.Name, Variance]]
    type DelayedResolution = Map[ScopedDataType.Name, (ImmArraySeq[Ref.Name], VarianceConstraint)]

    def base(base: BaseResolution) = VarianceConstraint(base, Map.empty)

    implicit val `constraint unifier monoid`: Monoid[VarianceConstraint] = Monoid.instance(
      { case (VarianceConstraint(r0, d0), VarianceConstraint(r1, d1)) =>
        VarianceConstraint(
          r0 |+| r1,
          d0.unionWithKey(d1) { (name, sr0, sr1) =>
            assert(
              sr0.length == sr1.length,
              s"type $name yielded different arities; this should never happen",
            )
            (sr0 zip sr1) map { case ((v0, c0), (v1, c1)) =>
              assert(
                v0 == v1,
                s"type $name had different parameter names $v0, $v1; this should never happen",
              )
              (v0, c0 |+| c1)
            }
          },
        )
      },
      VarianceConstraint(Map.empty, Map.empty),
    )
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
