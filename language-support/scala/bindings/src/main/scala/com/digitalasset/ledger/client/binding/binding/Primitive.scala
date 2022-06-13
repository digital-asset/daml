// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.TimeZone

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.{commands => rpccmd, value => rpcvalue}
import com.daml.ledger.client.binding.encoding.ExerciseOn
import scalaz.syntax.std.boolean._
import scalaz.syntax.tag._

import scala.annotation.nowarn
import scala.collection.{Factory, mutable, immutable => imm}

sealed abstract class Primitive extends PrimitiveInstances {
  type Int64 = Long
  type Numeric = BigDecimal
  type Party = ApiTypes.Party
  val Party: ApiTypes.Party.type = ApiTypes.Party
  type Text = String

  /** A [[LocalDate]] in the range [0001-01-01, 9999-12-31] (`[Date.MIN,
    * Date.MAX]`).  This is the range that can be stored as primitive
    * `Date`s on a ledger, and matches the set of representable dates in
    * the [RFC-3339](https://www.ietf.org/rfc/rfc3339.txt) date-time
    * format.  Any [[LocalDate]] in that range can be converted to a
    * [[Date]] by calling `Date.fromLocalDate`.
    */
  type Date <: LocalDate
  val Date: DateApi

  /** An [[Instant]] with only microsecond resolution in the range
    * [0001-01-01T00:00:00Z, 9999-12-31T23:59:59.999999Z] (`[Timestamp.MIN,
    * Timestamp.MAX]`).  Only such times can be stored as primitive `Time`s
    * on the ledger.  Any [[Instant]] in that range can be converted to
    * a [[Timestamp]] by calling `Time.discardNanos`.
    */
  type Timestamp <: Instant
  val Timestamp: TimeApi
  type Unit = scala.Unit
  type Bool = scala.Boolean
  type List[+A] = imm.Seq[A]
  val List: imm.Seq.type = imm.Seq

  type Optional[+A] = scala.Option[A]
  val Optional: scala.Option.type = scala.Option

  type TextMap[+V] <: imm.Map[String, V] with Compat.MapLike[String, V, imm.Map, TextMap[V]]
  val TextMap: TextMapApi

  @deprecated("Use TextMap", since = "0.13.40")
  type Map[+V] = TextMap[V]
  @deprecated("Use TextMap", since = "0.13.40")
  val Map: TextMap.type

  type GenMap[K, +V] <: imm.Map[K, V] with Compat.MapLike[K, V, GenMap, GenMap[K, V]]
  val GenMap: Compat.MapFactory[GenMap]
  private[binding] def substGenMap[F[_[_, _]]](tc: F[imm.Map]): F[GenMap]

  type ChoiceId = ApiTypes.Choice
  val ChoiceId: ApiTypes.Choice.type = ApiTypes.Choice

  // abstract primitives
  type ContractId[+Tpl] <: ApiTypes.ContractId
  val ContractId: ContractIdApi
  type TemplateId[+Tpl] <: ApiTypes.TemplateId
  val TemplateId: TemplateIdApi
  type Update[+A] <: DomainCommand

  sealed abstract class TextMapApi {
    def empty[V]: TextMap[V]
    def apply[V](elems: (String, V)*): TextMap[V]
    def newBuilder[V]: mutable.Builder[(String, V), TextMap[V]]
    implicit def factory[V]: Factory[(String, V), TextMap[V]]
    final def fromMap[V](map: imm.Map[String, V]): TextMap[V] = leibniz[V](map)
    def subst[F[_[_]]](fa: F[imm.Map[String, *]]): F[TextMap]
    final def leibniz[V]: imm.Map[String, V] =:= TextMap[V] =
      subst[Lambda[g[_] => imm.Map[String, V] =:= g[V]]](
        implicitly[imm.Map[String, V] =:= imm.Map[String, V]]
      )
  }

  sealed abstract class DateApi {
    val MIN: Date
    val MAX: Date

    /** Narrow `ld` if it's in the `[MIN, MAX]` range, `None` otherwise. */
    def fromLocalDate(ld: LocalDate): Option[Date]

    // bypass the value test
    private[binding] def subst[F[_]](tc: F[LocalDate]): F[Date]
  }

  sealed abstract class TimeApi {
    val MIN: Timestamp
    val MAX: Timestamp

    /** Reduce `t`'s resolution to exclude nanoseconds; return `None` if outside
      * the `[MIN, MAX]` range, the reduced-resolution value
      * otherwise.
      */
    def discardNanos(t: Instant): Option[Timestamp]

    // bypass the value test
    private[binding] def subst[F[_]](tc: F[Instant]): F[Timestamp]
  }

  sealed abstract class ContractIdApi {
    def apply[Tpl](contractId: String): ContractId[Tpl]
    def subst[F[_], Tpl](tc: F[ApiTypes.ContractId]): F[ContractId[Tpl]]
  }

  sealed abstract class TemplateIdApi {
    // the sole source of valid Template-associated template IDs is
    // their codegenned companions
    def apply[Tpl <: Template[Tpl]](
        packageId: String,
        moduleName: String,
        entityName: String,
    ): TemplateId[Tpl]

    private[binding] def substEx[F[_]](fa: F[rpcvalue.Identifier]): F[TemplateId[_]]

    /** Package ID, module name, and entity name.
      */
    def unapply[Tpl](t: TemplateId[Tpl]): Option[(String, String, String)]
  }

  private[binding] def substContractId[F[_], Tpl](tc: F[ApiTypes.ContractId]): F[ContractId[Tpl]]

  private[binding] def createFromArgs[Tpl](
      companion: TemplateCompanion[_ <: Tpl],
      na: rpcvalue.Record,
  ): Update[ContractId[Tpl]]

  private[binding] def exercise[ExOn, Tpl, Out](
      templateCompanion: ContractTypeCompanion[Tpl],
      receiver: ExOn,
      choiceId: String,
      argument: rpcvalue.Value,
  )(implicit ev: ExerciseOn[ExOn, Tpl]): Update[Out]

  private[binding] def arguments(
      recordId: rpcvalue.Identifier,
      args: Seq[(String, rpcvalue.Value)],
  ): rpcvalue.Record
}

private[client] object OnlyPrimitive extends Primitive {
  type Date = LocalDate
  type Timestamp = Instant
  type ContractId[+Tpl] = ApiTypes.ContractId
  type TemplateId[+Tpl] = ApiTypes.TemplateId
  type Update[+A] = DomainCommand

  type TextMap[+V] = imm.Map[String, V]

  object TextMap extends TextMapApi {
    override def empty[V]: TextMap[V] = imm.Map.empty
    override def apply[V](elems: (String, V)*): TextMap[V] = imm.Map(elems: _*)
    override def newBuilder[V]: mutable.Builder[(String, V), TextMap[V]] = imm.Map.newBuilder
    override def factory[V]: Factory[(String, V), TextMap[V]] =
      implicitly[Factory[(String, V), imm.Map[String, V]]]
    override def subst[F[_[_]]](fa: F[TextMap]): F[TextMap] = fa
  }

  override val Map = TextMap

  type GenMap[K, +V] = imm.Map[K, V]
  override val GenMap = imm.Map
  private[binding] override def substGenMap[F[_[_, _]]](tc: F[GenMap]) = tc

  object Date extends DateApi {
    import com.daml.api.util.TimestampConversion
    private val UTC = TimeZone.getTimeZone("UTC")
    override val MIN = LocalDateTime.ofInstant(TimestampConversion.MIN, UTC.toZoneId).toLocalDate
    override val MAX = LocalDateTime.ofInstant(TimestampConversion.MAX, UTC.toZoneId).toLocalDate

    override def fromLocalDate(ld: LocalDate) = {
      import scala.math.Ordering.Implicits._
      val ldc: java.time.chrono.ChronoLocalDate = ld
      (ldc >= MIN && ldc <= MAX) option ld
    }

    private[binding] override def subst[F[_]](tc: F[LocalDate]) = tc
  }

  object Timestamp extends TimeApi {
    import com.daml.api.util.TimestampConversion
    override val MIN = TimestampConversion.MIN
    override val MAX = TimestampConversion.MAX

    override def discardNanos(t: Instant) = {
      import scala.math.Ordering.Implicits._
      (t >= MIN && t <= MAX) option (t truncatedTo java.time.temporal.ChronoUnit.MICROS)
    }

    private[binding] override def subst[F[_]](tc: F[Instant]) = tc
  }

  object TemplateId extends TemplateIdApi {
    // the ledger api still uses names with only dots in them, while QualifiedName.toString
    // separates the module and the name in the module with colon.
    override def apply[Tpl <: Template[Tpl]](
        packageId: String,
        moduleName: String,
        entityName: String,
    ): TemplateId[Tpl] =
      ApiTypes.TemplateId(
        rpcvalue
          .Identifier(packageId = packageId, moduleName = moduleName, entityName = entityName)
      )

    private[binding] override def substEx[F[_]](fa: F[rpcvalue.Identifier]) =
      ApiTypes.TemplateId subst fa

    override def unapply[Tpl](t: TemplateId[Tpl]): Some[(String, String, String)] = {
      val rpcvalue.Identifier(packageId, moduleName, entityName) = t.unwrap
      Some((packageId, moduleName, entityName))
    }
  }

  object ContractId extends ContractIdApi {
    override def apply[Tpl](contractId: String) =
      ApiTypes.ContractId(contractId)

    override def subst[F[_], Tpl](tc: F[ApiTypes.ContractId]): F[ContractId[Tpl]] = tc
  }

  private[binding] override def substContractId[F[_], Tpl](
      tc: F[ApiTypes.ContractId]
  ): F[ContractId[Tpl]] = tc

  private[binding] override def createFromArgs[Tpl](
      companion: TemplateCompanion[_ <: Tpl],
      na: rpcvalue.Record,
  ): Update[ContractId[Tpl]] =
    DomainCommand(
      rpccmd.Command(
        rpccmd.Command.Command
          .Create(rpccmd.CreateCommand(templateId = Some(companion.id.unwrap), Some(na)))
      ),
      companion,
    )

  private[binding] override def exercise[ExOn, Tpl, Out](
      exerciseTarget: ContractTypeCompanion[Tpl],
      receiver: ExOn,
      choiceId: String,
      argument: rpcvalue.Value,
  )(implicit ev: ExerciseOn[ExOn, Tpl]): Update[Out] =
    DomainCommand(
      rpccmd.Command {
        ev match {
          case _: ExerciseOn.OnId[Tpl] =>
            rpccmd.Command.Command.Exercise(
              rpccmd.ExerciseCommand(
                templateId = Some(exerciseTarget.id.unwrap),
                contractId = (receiver: ContractId[Tpl]).unwrap,
                choice = choiceId,
                choiceArgument = Some(argument),
              )
            )
          case _: ExerciseOn.CreateAndOnTemplate[Tpl] =>
            val cfe: Template.CreateForExercise[Tpl] = receiver
            rpccmd.Command.Command.CreateAndExercise(
              // TODO #13993 pass exerciseTarget.id.unwrap as interface ID
              rpccmd.CreateAndExerciseCommand(
                templateId = Some(cfe.value.templateId.unwrap),
                createArguments = Some(cfe.value.arguments),
                choice = choiceId,
                choiceArgument = Some(argument),
              )
            )
          case _: ExerciseOn.OnKey[Tpl] =>
            val k: Template.Key[Tpl] = receiver
            // TODO #13993 pass exerciseTarget.id.unwrap as interface ID
            rpccmd.Command.Command.ExerciseByKey(
              rpccmd.ExerciseByKeyCommand(
                templateId = Some(k.origin.id.unwrap),
                contractKey = Some(k.encodedKey),
                choice = choiceId,
                choiceArgument = Some(argument),
              )
            )
        }
      },
      exerciseTarget,
    )

  private[binding] override def arguments(
      recordId: rpcvalue.Identifier,
      args: Seq[(String, rpcvalue.Value)],
  ): rpcvalue.Record =
    rpcvalue.Record(
      recordId = Some(recordId),
      args.map { case (k, v) =>
        rpcvalue.RecordField(k, Some(v))
      },
    )
}

sealed abstract class PrimitiveInstances

// do not import this._, use -Xsource:2.13 scalac option instead
object PrimitiveInstances {
  import Primitive.{GenMap, TextMap}

  import language.implicitConversions

  implicit def textMapFactory[V]: Factory[(String, V), TextMap[V]] =
    TextMap.factory

  implicit def genMapFactory[K, V]: Factory[(K, V), GenMap[K, V]] = {
    type CBF[M[_, _]] = Factory[(K, V), M[K, V]]
    @nowarn("msg=local val genMapFactory in method genMapFactory is never used")
    val genMapFactory = () // prevent recursion
    GenMap.subst[CBF](implicitly[CBF[imm.Map]])
  }

  /** Applied in contexts that ''expect'' a `TextMap`, iff -Xsource:2.13. */
  implicit def textMapFromMap[V](m: imm.Map[String, V]): TextMap[V] = TextMap fromMap m

  /** Applied in contexts that ''expect'' a `GenMap`, iff -Xsource:2.13. */
  implicit def genMapFromMap[K, V](m: imm.Map[K, V]): GenMap[K, V] = {
    type Id2[M[_, _]] = M[K, V]
    GenMap.subst[Id2](m)
  }

  implicit final class GenMapCompanionMethods(private val self: GenMap.type) extends AnyVal {
    private[binding] def subst[F[_[_, _]]](tc: F[imm.Map]): F[GenMap] =
      Primitive substGenMap tc
  }
}
