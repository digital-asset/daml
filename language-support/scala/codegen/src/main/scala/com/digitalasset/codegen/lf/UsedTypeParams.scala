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
      run: iface.TypeConName => Option[(iface.DefDataType[RF, VF], LookupType[RF, VF])]
  )

  import VarianceConstraint.{BaseResolution, DelayedResolution}
  import ScopedDataType.{Name => I}

  final class ResolvedVariance private (prior: Map[I, ImmArraySeq[Variance]]) {
    def allCovariantVars(dt: I, ei: iface.EnvironmentInterface): ResolvedVariance =
      covariantVars(dt, (i: I) => ei.typeDecls get i map (_.`type`))

    /** Variance of `sdt.typeVars` in order. */
    private[this] def covariantVars[RF <: iface.Type, VF <: iface.Type](
        dt: I,
        lookupType: I => Option[iface.DefDataType[RF, VF]],
    ): ResolvedVariance = {
      import iface._, Variance._
      def goType(dt: I, seen: Set[I])(typ: iface.Type): VarianceConstraint = typ match {
        case TypeVar(name) =>
          // while we default to Covariant at a later step,
          // absence *does not* mean set-to-Covariant at this step
          VarianceConstraint.base(Map(dt -> Map(name -> Covariant)))

        case TypePrim(pt, typArgs) =>
          import PrimType.{Map => _, _}
          pt match {
            case GenMap =>
              val Seq(kt, vt) = typArgs
              // we don't need to inspect `kt` any further than enumerating it;
              // every occurrence therein is invariant
              goType(dt, seen)(vt) |+| VarianceConstraint.base(
                Map(dt -> collectTypeParams(kt).view.map((_, Invariant)).toMap)
              )
            case Bool | Int64 | Text | Date | Timestamp | Party | ContractId | List | Unit |
                Optional | TextMap =>
              typArgs foldMap goType(dt, seen)
          }

        case TypeCon(TypeConName(tcName), typArgs) =>
          val refDdt = lookupType(tcName) getOrElse sys.error(s"$tcName not found")
          if (seen(tcName)) {
            VarianceConstraint(
              resolutions = Map.empty,
              delayedConstraints = (refDdt.typeVars, typArgs foldMap goType(dt, seen)),
            )
          } else {
            val tcVc = goSdt(tcName, seen + tcName)(refDdt)
            refDdt.typeVars.zip(typArgs).foldMap { case (paramName, aContents) =>
              val pVariance = tcVc
              val aPositions = goType(dt, seen)(aContents)
              pVariance match {
                case Covariant => aPositions
                case Invariant =>
                  aPositions mapResolutions (_.alter(dt)(
                    _ map (_ transform ((_, _) => Invariant))
                  ))
              }
            }
          }

        case TypeNumeric(_) => VarianceConstraint.base(Map.empty)
      }

      def goSdt(dt: I, seen: Set[I])(sdt: DefDataType[RF, VF]): VarianceConstraint = {
        val vLookup = sdt.dataType match {
          case Record(fields) => fields foldMap { case (_, typ) => goType(dt, seen)(typ) }
          case Variant(fields) => fields foldMap { case (_, typ) => goType(dt, seen)(typ) }
          case Enum(_) => mzero[VarianceConstraint]
        }
        vLookup
      }

      goSdt(sdt)
    }
  }

  object ResolvedVariance {
    val Empty: ResolvedVariance = new ResolvedVariance(Map.empty)
  }

  // an implementation tool for covariantVars
  private[this] final case class VarianceConstraint(
      resolutions: BaseResolution,
      delayedConstraints: DelayedResolution,
  ) {
    def mapResolutions(f: BaseResolution => BaseResolution) =
      copy(resolutions = f(resolutions))
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
            val (sb0, c0) = sr0
            val (sb1, c1) = sr1
            assert(
              sb0 == sb1,
              s"type $name yielded different param lists; this should never happen",
            )
            (sb0, c0 |+| c1)
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
