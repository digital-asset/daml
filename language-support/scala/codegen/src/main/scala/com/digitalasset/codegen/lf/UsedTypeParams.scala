// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.lf

import com.daml.codegen.lf.DamlDataTypeGen.{DataType, VariantField}
import com.daml.lf.data.Ref, Ref.Identifier
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
  import Variance._
  private[this] type TVar = Ref.Name

  final class ResolvedVariance private (private val prior: Map[Identifier, ImmArraySeq[Variance]]) {

    /** The variance of each type parameter of `dt` in `ei`, in order,
      * and an updated copy of the receiver that may contain more cached
      * resolved variances from `ei`.
      */
    def allCovariantVars(
        dt: Identifier,
        ei: iface.EnvironmentInterface,
    ): (ResolvedVariance, ImmArraySeq[Variance]) = {
      val resolved = covariantVars(dt, (i: Identifier) => ei.typeDecls get i map (_.`type`))
      (
        resolved,
        resolved.prior.getOrElse(
          dt,
          ei.typeDecls.getOrElse(dt, sys.error(s"$dt not found")).`type`.typeVars map (_ =>
            Covariant
          ),
        ),
      )
    }

    private[this] def covariantVars(
        dt: Identifier,
        lookupType: Identifier => Option[iface.DefDataType.FWT],
    ): ResolvedVariance = {
      import iface._

      def lookupOrFail(i: Identifier) = lookupType(i) getOrElse sys.error(s"$i not found")

      def goTypeDefn(dt: Identifier, seen: Set[Identifier])(
          typ: iface.Type
      ): VarianceConstraint = {
        def goType(contexts: Map[Identifier, Set[TVar]])(typ: iface.Type): VarianceConstraint =
          typ match {
            case TypeVar(name) =>
              // while we default to Covariant at a later step,
              // absence in this map *does not* mean set-to-Covariant at this step
              VarianceConstraint(
                Map(dt -> Map(name -> Covariant)),
                DelayedResolution(
                  if (contexts.nonEmpty) Map(dt -> Map(name -> contexts)) else Map.empty
                ),
              )

            case TypePrim(pt, typArgs) =>
              import PrimType.{Map => _, _}
              pt match {
                case GenMap =>
                  val Seq(kt, vt) = typArgs
                  // we don't need to inspect `kt` any further than enumerating it;
                  // every occurrence therein is invariant
                  goType(contexts)(vt) |+| VarianceConstraint.base(
                    Map(dt -> collectTypeParams(kt).view.map((_, Invariant)).toMap)
                  )
                case Bool | Int64 | Text | Date | Timestamp | Party | ContractId | List | Unit |
                    Optional | TextMap =>
                  // this is only safe for all-params-covariant cases
                  typArgs foldMap goType(contexts)
              }

            case TypeCon(TypeConName(tcName), typArgs) =>
              val refDdt = lookupOrFail(tcName)
              val argConstraints = refDdt.typeVars zip typArgs foldMap { case (tv, ta) =>
                goType(contexts |+| Map(tcName -> Set(tv)))(ta)
              }
              val refConstraints =
                if (seen(tcName)) mzero[VarianceConstraint]
                else goSdt(tcName, seen + tcName)(refDdt)
              argConstraints |+| refConstraints

            case TypeNumeric(_) => VarianceConstraint.base(Map.empty)
          }
        goType(Map.empty)(typ)
      }

      def goSdt(dt: Identifier, seen: Set[Identifier])(
          sdt: DefDataType.FWT
      ): VarianceConstraint =
        prior
          .get(dt)
          .map(VarianceConstraint.reresolve(dt, sdt, _))
          .getOrElse {
            val fields = sdt.dataType match {
              case Record(fields) => fields
              case Variant(fields) => fields
              case Enum(_) => ImmArraySeq.empty
            }
            fields foldMap { case (_, typ) => goTypeDefn(dt, seen)(typ) }
          }

      val solved = goSdt(dt, Set(dt))(lookupOrFail(dt)).solve
      new ResolvedVariance(prior ++ solved.view.map { case (tcName, m) =>
        val paramsOrder = lookupOrFail(tcName).typeVars
        (tcName, paramsOrder map (m.getOrElse(_, Covariant)))
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
      fixedPoint(resolutions) { resolutions =>
        resolutions |+| delayedConstraints.contexts.transform { (_, argVars) =>
          argVars.transform { (_, argContexts) =>
            import scalaz.std.iterable._
            // What we're really doing here is *multiplying* the variances.  So
            // whereas the monoid would yield (contravariant, contravariant) => contravariant,
            // we would want (contravariant, contravariant) => covariant.  However,
            // the multiplication monoid happens to equal the addition monoid for now,
            // so there is no need to complicate things by defining both.
            (argContexts: Iterable[(Identifier, Set[TVar])]) foldMap { case (ctxI, ctxVs) =>
              val resAtCtxI = resolutions.getOrElse(ctxI, Map.empty)
              (ctxVs: Iterable[TVar]) foldMap (resAtCtxI.getOrElse(_, Covariant))
            }
          }
        }
      }
  }

  @tailrec private[this] def fixedPoint[A](init: A)(f: A => A): A = {
    val next = f(init)
    if (init == next) init else fixedPoint(next)(f)
  }

  private[this] object VarianceConstraint {
    type BaseResolution = Map[ScopedDataType.Name, Map[TVar, Variance]]

    def base(base: BaseResolution) = VarianceConstraint(base, mzero[DelayedResolution])

    /** Put a resolved variance back in constraint format, so it doesn't have to
      * be figured again.
      */
    def reresolve(dtName: Identifier, dt: iface.DefDataType[_, _], resolved: Seq[Variance]) =
      VarianceConstraint.base(Map(dtName -> dt.typeVars.view.zip(resolved).toMap))

    implicit val `constraint unifier monoid`: Monoid[VarianceConstraint] = Monoid.instance(
      { case (VarianceConstraint(r0, d0), VarianceConstraint(r1, d1)) =>
        VarianceConstraint(r0 |+| r1, d0 |+| d1)
      },
      VarianceConstraint(Map.empty, mzero[DelayedResolution]),
    )
  }

  // the outer (I.TVar) occurs in the argument position of the inner (I.TVar)s
  private[this] final case class DelayedResolution(
      contexts: Map[Identifier, Map[TVar, Map[Identifier, Set[TVar]]]]
  )

  private[this] object DelayedResolution {
    implicit val `ref apps monoid`: Monoid[DelayedResolution] = Monoid.instance(
      { case (DelayedResolution(d0), DelayedResolution(d1)) =>
        DelayedResolution(d0 |+| d1)
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
