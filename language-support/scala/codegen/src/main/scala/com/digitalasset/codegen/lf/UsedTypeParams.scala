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
import scalaz.syntax.traverse._
import scalaz.syntax.monoid._
import scalaz.syntax.std.map._

import scala.annotation.tailrec

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

  import VarianceConstraint.BaseResolution
  import ScopedDataType.{Name => I}
  import Variance._

  final class ResolvedVariance private (prior: Map[I, ImmArraySeq[Variance]]) {
    def allCovariantVars(dt: I, ei: iface.EnvironmentInterface): ResolvedVariance =
      covariantVars(dt, (i: I) => ei.typeDecls get i map (_.`type`))

    /** Variance of `sdt.typeVars` in order. */
    private[this] def covariantVars[RF <: iface.Type, VF <: iface.Type](
        dt: I,
        lookupType: I => Option[iface.DefDataType[RF, VF]],
    ): ResolvedVariance = {
      import iface._

      def lookupOrFail(i: I) = lookupType(i) getOrElse sys.error(s"$i not found")

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
              // this is only safe for all-params-covariant cases
              typArgs foldMap goType(dt, seen)
          }

        case TypeCon(TypeConName(tcName), typArgs) =>
          val refDdt = lookupOrFail(tcName)
          val (innerDelays, argBases) = typArgs traverse { typArg =>
            val avc = goType(dt, seen)(typArg)
            (avc.delayedConstraints, avc.resolutions)
          }
          val argConstraints = VarianceConstraint(
            resolutions = Map.empty,
            delayedConstraints =
              DelayedResolution(Map(dt -> refDdt.typeVars.zip(argBases))) |+| innerDelays,
          )
          val refConstraints =
            if (seen(tcName)) mzero[VarianceConstraint] else goSdt(tcName, seen + tcName)(refDdt)
          argConstraints |+| refConstraints

        case TypeNumeric(_) => VarianceConstraint.base(Map.empty)
      }

      def goSdt(dt: I, seen: Set[I])(sdt: DefDataType[RF, VF]): VarianceConstraint =
        prior
          .get(dt)
          .map { resolved =>
            VarianceConstraint.base(Map(dt -> sdt.typeVars.view.zip(resolved).toMap))
          }
          .getOrElse {
            val vLookup = sdt.dataType match {
              case Record(fields) => fields foldMap { case (_, typ) => goType(dt, seen)(typ) }
              case Variant(fields) => fields foldMap { case (_, typ) => goType(dt, seen)(typ) }
              case Enum(_) => mzero[VarianceConstraint]
            }
            vLookup
          }

      val solved = goSdt(dt, Set(dt))(lookupOrFail(dt)).solve
      new ResolvedVariance(prior ++ solved.view.map { case (tcName, m) =>
        val paramsOrder = lookupOrFail(tcName).typeVars
        (tcName, paramsOrder map m)
      })
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
    def solve: BaseResolution =
      fixedPoint(resolutions) {
        delayedConstraints.constraints.foldLeft(_) { case (resolutions, (dtName, paramArgs)) =>
          val resAtDtName = resolutions.getOrElse(dtName, Map.empty)
          resolutions |+| paramArgs.foldMap { case (paramName, paramArg) =>
            resAtDtName.getOrElse(paramName, Covariant) match {
              case Covariant => Map.empty
              case Invariant => paramArg.transform((_, m) => setAllValues(m)(Invariant))
            }
          }
        }
      }
  }

  private[this] def setAllValues[K, V](m: Map[K, V])(v: V) =
    m transform ((_, _) => v)

  @tailrec private[this] def fixedPoint[A](init: A)(f: A => A): A = {
    val next = f(init)
    if (init == next) init else fixedPoint(next)(f)
  }

  private[this] object VarianceConstraint {
    type BaseResolution = Map[ScopedDataType.Name, Map[Ref.Name, Variance]]

    def base(base: BaseResolution) = VarianceConstraint(base, mzero[DelayedResolution])

    implicit val `constraint unifier monoid`: Monoid[VarianceConstraint] = Monoid.instance(
      { case (VarianceConstraint(r0, d0), VarianceConstraint(r1, d1)) =>
        VarianceConstraint(r0 |+| r1, d0 |+| d1)
      },
      VarianceConstraint(Map.empty, mzero[DelayedResolution]),
    )
  }

  private[this] final case class DelayedResolution(
      constraints: Map[ScopedDataType.Name, ImmArraySeq[(Ref.Name, BaseResolution)]]
  )

  private[this] object DelayedResolution {
    implicit val `ref apps monoid`: Monoid[DelayedResolution] = Monoid.instance(
      { case (DelayedResolution(d0), DelayedResolution(d1)) =>
        DelayedResolution(d0.unionWithKey(d1) { (name, sr0, sr1) =>
          assert(
            sr0.length == sr1.length,
            s"type $name yielded different param lists; this should never happen",
          )
          sr0 zip sr1 map { case ((p0, a0), (_, a1)) =>
            (p0, a0 |+| a1)
          }
        })
      },
      DelayedResolution(Map.empty),
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
