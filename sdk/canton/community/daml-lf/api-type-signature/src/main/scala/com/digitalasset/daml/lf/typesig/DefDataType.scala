// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.typesig

import scalaz.std.map._
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.syntax.traverse._
import scalaz.syntax.std.map._
import scalaz.syntax.std.option._
import scalaz.Semigroup
import scalaz.Tags.FirstVal
import java.{util => j}

import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.Ref
import com.daml.nonempty.NonEmpty

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

case class DefDataType[+RF, +VF](typeVars: ImmArraySeq[Ref.Name], dataType: DataType[RF, VF]) {
  def getTypeVars: j.List[_ <: String] = typeVars.asJava
}

object DefDataType {

  /** Alias for application to [[Type]]. Note that FWT stands for "Field with
    * type", because before we parametrized over both the field and the type,
    * while now we only parametrize over the type.
    */
  type FWT = DefDataType[Type, Type]
}

sealed trait DataType[+RT, +VT] extends Product with Serializable {
  def bimap[C, D](f: RT => C, g: VT => D): DataType[C, D] =
    this match {
      case r: Record[RT] => r.map(f)
      case v: Variant[VT] => v.map(g)
      case e: Enum => e
    }
}

object DataType {

  /** Alias for application to [[Type]]. Note that FWT stands for "Field with
    * type", because before we parametrized over both the field and the type,
    * while now we only parametrize over the type.
    */
  type FWT = DataType[Type, Type]

  sealed trait GetFields[+A] {
    def fields: ImmArraySeq[(Ref.Name, A)]
    final def getFields: j.List[_ <: (String, A)] = fields.asJava
  }
}

// Record TypeDecl`s have an object generated for them in their own file
final case class Record[+RT](fields: ImmArraySeq[(Ref.Name, RT)])
    extends DataType[RT, Nothing]
    with DataType.GetFields[RT] {

  def map[B](f: RT => B): Record[B] = Record(fields.map { case (n, t) => (n, f(t)) })

  /** Widen to DataType, in Java. */
  def asDataType[PRT >: RT, VT]: DataType[PRT, VT] = this
}

object Record extends FWTLike[Record]

// Variant TypeDecl`s have an object generated for them in their own file
final case class Variant[+VT](fields: ImmArraySeq[(Ref.Name, VT)])
    extends DataType[Nothing, VT]
    with DataType.GetFields[VT] {

  /** Widen to DataType, in Java. */
  def asDataType[RT, PVT >: VT]: DataType[RT, PVT] = this

  def map[B](f: VT => B): Variant[B] = Variant(fields.map { case (n, t) => (n, f(t)) })
}

object Variant extends FWTLike[Variant]
final case class Enum(constructors: ImmArraySeq[Ref.Name]) extends DataType[Nothing, Nothing] {

  /** Widen to DataType, in Java. */
  def asDataType[RT, PVT]: DataType[RT, PVT] = this
}

final case class DefTemplate[+Ty](
    tChoices: TemplateChoices[Ty],
    key: Option[Ty],
    implementedInterfaces: Seq[Ref.TypeConId],
) {
  def map[B](f: Ty => B): DefTemplate[B] =
    DefTemplate(tChoices.map(f), key.map(f), implementedInterfaces)

  @deprecated("use tChoices.directChoices or tChoices.resolvedChoices instead", since = "2.3.0")
  private[daml] def choices = tChoices.directChoices

  /** Remove choices from `unresolvedInheritedChoices` and add to `choices`
    * given the `astInterfaces` from an [[EnvironmentSignature]].  If the result
    * has any `unresolvedInheritedChoices` left, these choices were not found.
    */
  def resolveChoices[O >: Ty](
      astInterfaces: PartialFunction[Ref.TypeConId, DefInterface[O]]
  ): Either[TemplateChoices.ResolveError[DefTemplate[O]], DefTemplate[O]] = {
    import scalaz.std.either._
    import scalaz.syntax.bifunctor._
    tChoices resolveChoices astInterfaces bimap (_.map(r => copy(tChoices = r)), r =>
      copy(tChoices = r))
  }

  def getKey: j.Optional[_ <: Ty] = key.toJava

  private[typesig] def extendWithInterface[OTy >: Ty](
      ifaceName: Ref.TypeConId,
      ifc: DefInterface[OTy],
  ): DefTemplate[OTy] = {
    import TemplateChoices.{Resolved, Unresolved}
    copy(
      implementedInterfaces = implementedInterfaces :+ ifaceName,
      tChoices = tChoices match {
        case unr @ Unresolved(_, sources) =>
          unr.copy(unresolvedChoiceSources = sources incl ifaceName)
        // If unresolved, we need only add ifc as a future interface to resolve;
        // otherwise, we must self-resolve and add to preexisting resolutions
        case r @ Resolved(rc) =>
          type K[C] = Semigroup[Resolved.Choices[C]]
          r.copy(resolvedChoices =
            FirstVal
              .unsubst[K, TemplateChoice[OTy]](Semigroup.apply)
              .append(rc, ifc choicesAsResolved ifaceName)
          )
      },
    )
  }
}

object DefTemplate {
  type FWT = DefTemplate[Type]

  private[daml] val Empty: DefTemplate[Nothing] =
    DefTemplate(TemplateChoices.Resolved(Map.empty), None, Seq.empty)
}

/** Choices in a [[DefTemplate]]. */
sealed abstract class TemplateChoices[+Ty] extends Product with Serializable {
  import TemplateChoices.{Resolved, Unresolved, ResolveError, directAsResolved, logger}

  /** Choices defined directly on the template */
  def directChoices: Map[Ref.ChoiceName, TemplateChoice[Ty]]

  /** Choices defined on the template, or on resolved implemented interfaces if
    * resolved
    */
  def resolvedChoices: Map[Ref.ChoiceName, NonEmpty[Map[Option[Ref.TypeConId], TemplateChoice[Ty]]]]

  def map[B](f: Ty => B): TemplateChoices[B]

  /** A shim function to delay porting a component to overloaded choices.
    * Discards essential data, so not a substitute for a proper port.
    * TODO (#13974) delete when there are no more callers
    */
  private[daml] def assumeNoOverloadedChoices(
      githubIssue: Int
  ): Map[Ref.ChoiceName, TemplateChoice[Ty]] = this match {
    case Unresolved(directChoices, _) => directChoices
    case Resolved(resolvedChoices) =>
      resolvedChoices.transform { (choiceName, overloads) =>
        if (overloads.sizeIs == 1) overloads.head1._2
        else
          overloads
            .get(None)
            .cata(
              { directChoice =>
                logger.warn(s"discarded inherited choices for $choiceName, see #$githubIssue")
                directChoice
              }, {
                val (Some(randomKey), randomChoice) = overloads.head1
                logger.warn(
                  s"selected $randomKey-inherited choice but discarded others for $choiceName, see #$githubIssue"
                )
                randomChoice
              },
            )
      }
  }

  final def getDirectChoices: j.Map[Ref.ChoiceName, _ <: TemplateChoice[Ty]] =
    directChoices.asJava

  final def getResolvedChoices
      : j.Map[Ref.ChoiceName, _ <: j.Map[j.Optional[Ref.TypeConId], _ <: TemplateChoice[Ty]]] =
    resolvedChoices.transform((_, m) => m.forgetNE.mapKeys(_.toJava).asJava).asJava

  /** Coerce to [[Resolved]] based on the environment `astInterfaces`, or fail
    * with the choices that could not be resolved.
    */
  private[typesig] def resolveChoices[O >: Ty](
      astInterfaces: PartialFunction[Ref.TypeConId, DefInterface[O]]
  ): Either[ResolveError[Resolved[O]], Resolved[O]] = this match {
    case Unresolved(direct, unresolved) =>
      val getAstInterface = astInterfaces.lift
      type ResolutionResult[C] = (Set[Ref.TypeConId], Resolved.Choices[C])
      val (missing, resolved): ResolutionResult[TemplateChoice[O]] =
        FirstVal.unsubst[ResolutionResult, TemplateChoice[O]](
          unresolved.forgetNE
            .foldMap { tcn =>
              getAstInterface(tcn).cata(
                { astIf =>
                  val tcnResolved = astIf choicesAsResolved tcn
                  FirstVal.subst[ResolutionResult, TemplateChoice[O]]((
                    Set.empty[Ref.TypeConId],
                    tcnResolved,
                  ))
                },
                (Set(tcn), Map.empty): ResolutionResult[Nothing],
              )
            }
        )
      val rChoices = Resolved(resolved.unionWith(directAsResolved(direct))(_ ++ _))
      missing match {
        case NonEmpty(missing) => Left(ResolveError(missing, rChoices))
        case _ => Right(rChoices)
      }
    case r @ Resolved(_) => Right(r)
  }
}

object TemplateChoices {
  private val logger = com.typesafe.scalalogging.Logger(getClass)

  final case class ResolveError[+Partial](
      missingInterfaces: NonEmpty[Set[Ref.TypeConId]],
      partialResolution: Partial,
  ) {
    private[typesig] def describeError: String =
      missingInterfaces.mkString(", ")

    private[typesig] def map[B](f: Partial => B): ResolveError[B] =
      copy(partialResolution = f(partialResolution))
  }

  private[typesig] final case class Unresolved[+Ty](
      directChoices: Map[Ref.ChoiceName, TemplateChoice[Ty]],
      unresolvedChoiceSources: NonEmpty[Set[Ref.TypeConId]],
  ) extends TemplateChoices[Ty] {
    override def resolvedChoices = directAsResolved(directChoices)

    override def map[B](f: Ty => B): Unresolved[B] =
      Unresolved(
        directChoices.map { case (n, choice) => (n, choice.map(f)) },
        unresolvedChoiceSources,
      )
  }

  private[TemplateChoices] def directAsResolved[Ty](
      directChoices: Map[Ref.ChoiceName, TemplateChoice[Ty]]
  ) =
    directChoices transform ((_, c) => NonEmpty(Map, (Option.empty[Ref.TypeConId], c)))

  private[typesig] final case class Resolved[+Ty](
      resolvedChoices: Map[Ref.ChoiceName, NonEmpty[
        Map[Option[Ref.TypeConId], TemplateChoice[Ty]]
      ]]
  ) extends TemplateChoices[Ty] {
    override def directChoices = resolvedChoices collect (Function unlift { case (cn, m) =>
      m get None map ((cn, _))
    })

    override def map[B](f: Ty => B): Resolved[B] =
      Resolved(resolvedChoices.view.mapValues(_.toNEF.map(_.map(f))).toMap)
  }

  object Resolved {
    private[daml] def fromDirect[Ty](directChoices: Map[Ref.ChoiceName, TemplateChoice[Ty]]) =
      Resolved(directAsResolved(directChoices))

    // choice type abstracted over the TemplateChoice, for specifying
    // aggregation of choices (typically with tags, foldMap, semigroup)
    private[typesig] type Choices[C] =
      Map[Ref.ChoiceName, NonEmpty[Map[Option[Ref.TypeConId], C]]]
  }
}

final case class TemplateChoice[+Ty](param: Ty, consuming: Boolean, returnType: Ty) {
  def map[C](f: Ty => C): TemplateChoice[C] = TemplateChoice(f(param), consuming, f(returnType))
}

object TemplateChoice {
  type FWT = TemplateChoice[Type]
}

/** @param choices Choices of this interface, indexed by name
  * @param retroImplements IDs of templates that implement this interface, upon
  *                        introduction of this interface into the environment
  */
final case class DefInterface[+Ty](
    choices: Map[Ref.ChoiceName, TemplateChoice[Ty]],
    viewType: Option[Ref.TypeConId],
    // retroImplements are used only by LF 1.x
    retroImplements: Set[Ref.TypeConId] = Set.empty,
) {
  def getChoices: j.Map[Ref.ChoiceName, _ <: TemplateChoice[Ty]] =
    choices.asJava

  // Restructure `choices` in the resolved-choices data structure format,
  // for aggregation with [[TemplateChoices.Resolved]].
  private[typesig] def choicesAsResolved[Name](
      selfName: Name
  ): Map[Ref.ChoiceName, NonEmpty[Map[Option[Name], TemplateChoice[Ty]]]] =
    choices transform ((_, tc) => NonEmpty(Map, Option[Name](selfName) -> tc))

  private[typesig] def resolveRetroImplements[S, OTy >: Ty](selfName: Ref.TypeConId, s: S)(
      setTemplate: SetterAt[Ref.TypeConId, S, DefTemplate[OTy]]
  ): (S, DefInterface[OTy]) = {
    def addMySelf(dt: DefTemplate[OTy]) =
      dt.extendWithInterface(selfName, this)

    retroImplements
      .foldLeft((s, retroImplements)) { (sr, tplName) =>
        val (s, remaining) = sr
        setTemplate(s, tplName).cata(setter => (setter(addMySelf), remaining - tplName), sr)
      }
      .map(remaining => copy(retroImplements = remaining))
  }
}

object DefInterface extends FWTLike[DefInterface] {

  // documentation-only type synonyms for valid interface view types
  type ViewType[+Ty] = Record[Ty]
  type ViewTypeFWT = ViewType[Type]

}

/** Add aliases to companions. */
sealed abstract class FWTLike[F[+_]] {

  /** Alias for application to [[Type]]. Note that FWT stands for "Field with
    * type", because before we parametrized over both the field and the type,
    * while now we only parametrize over the type.
    */
  type FWT = F[Type]
}
