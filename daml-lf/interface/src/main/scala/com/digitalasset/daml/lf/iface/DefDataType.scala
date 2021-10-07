// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.iface

import scalaz.std.map._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.applicative.^
import scalaz.syntax.semigroup._
import scalaz.syntax.traverse._
import scalaz.{Applicative, Bifunctor, Bitraverse, Bifoldable, Foldable, Functor, Monoid, Traverse}
import java.{util => j}

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref

import scala.jdk.CollectionConverters._

case class DefDataType[+RF, +VF](typeVars: ImmArraySeq[Ref.Name], dataType: DataType[RF, VF]) {
  def bimap[C, D](f: RF => C, g: VF => D): DefDataType[C, D] =
    Bifunctor[DefDataType].bimap(this)(f, g)

  def getTypeVars: j.List[_ <: String] = typeVars.asJava
}

object DefDataType {

  /** Alias for application to [[Type]]. Note that FWT stands for "Field with
    * type", because before we parametrized over both the field and the type,
    * while now we only parametrize over the type.
    */
  type FWT = DefDataType[Type, Type]

  implicit val `DDT bitraverse`: Bitraverse[DefDataType] =
    new Bitraverse[DefDataType] with Bifoldable.FromBifoldMap[DefDataType] {

      override def bimap[A, B, C, D](
          fab: DefDataType[A, B]
      )(f: A => C, g: B => D): DefDataType[C, D] = {
        DefDataType(fab.typeVars, Bifunctor[DataType].bimap(fab.dataType)(f, g))
      }

      override def bifoldMap[A, B, M: Monoid](fab: DefDataType[A, B])(f: A => M)(g: B => M): M = {
        Bifoldable[DataType].bifoldMap(fab.dataType)(f)(g)
      }

      override def bitraverseImpl[G[_]: Applicative, A, B, C, D](
          fab: DefDataType[A, B]
      )(f: A => G[C], g: B => G[D]): G[DefDataType[C, D]] = {
        Applicative[G].map(Bitraverse[DataType].bitraverse(fab.dataType)(f)(g))(dataTyp =>
          DefDataType(fab.typeVars, dataTyp)
        )
      }
    }
}

sealed trait DataType[+RT, +VT] extends Product with Serializable {
  def bimap[C, D](f: RT => C, g: VT => D): DataType[C, D] =
    Bifunctor[DataType].bimap(this)(f, g)
}

object DataType {

  /** Alias for application to [[Type]]. Note that FWT stands for "Field with
    * type", because before we parametrized over both the field and the type,
    * while now we only parametrize over the type.
    */
  type FWT = DataType[Type, Type]

  // While this instance appears to overlap the subclasses' traversals,
  // naturality holds with respect to those instances and this one, so there is
  // no risk of confusion.
  implicit val `DT bitraverse`: Bitraverse[DataType] =
    new Bitraverse[DataType] with Bifoldable.FromBifoldMap[DataType] {

      override def bimap[A, B, C, D](fab: DataType[A, B])(f: A => C, g: B => D): DataType[C, D] =
        fab match {
          case r @ Record(_) =>
            Functor[Record].map(r)(f).widen
          case v @ Variant(_) =>
            Functor[Variant].map(v)(g).widen
          case e @ Enum(_) =>
            e
          case e @ Iface() => e
        }

      override def bifoldMap[A, B, M: Monoid](fab: DataType[A, B])(f: A => M)(g: B => M): M =
        fab match {
          case r @ Record(_) =>
            Foldable[Record].foldMap(r)(f)
          case v @ Variant(_) =>
            Foldable[Variant].foldMap(v)(g)
          case Enum(_) => {
            val m = implicitly[Monoid[M]]
            m.zero
          }
          case Iface() => {
            val m = implicitly[Monoid[M]]
            m.zero
          }
        }

      override def bitraverseImpl[G[_]: Applicative, A, B, C, D](
          fab: DataType[A, B]
      )(f: A => G[C], g: B => G[D]): G[DataType[C, D]] =
        fab match {
          case r @ Record(_) =>
            Traverse[Record].traverse(r)(f).widen
          case v @ Variant(_) =>
            Traverse[Variant].traverse(v)(g).widen
          case e @ Enum(_) =>
            Applicative[G].pure(e)
          case e @ Iface() =>
            Applicative[G].pure(e)
        }
    }

  sealed trait GetFields[+A] {
    def fields: ImmArraySeq[(Ref.Name, A)]
    final def getFields: j.List[_ <: (String, A)] = fields.asJava
  }
}

// Record TypeDecl`s have an object generated for them in their own file
final case class Record[+RT](fields: ImmArraySeq[(Ref.Name, RT)])
    extends DataType[RT, Nothing]
    with DataType.GetFields[RT] {

  /** Widen to DataType, in Java. */
  def asDataType[PRT >: RT, VT]: DataType[PRT, VT] = this
}

object Record extends FWTLike[Record] {
  implicit val `R traverse`: Traverse[Record] =
    new Traverse[Record] with Foldable.FromFoldMap[Record] {

      override def map[A, B](fa: Record[A])(f: A => B): Record[B] =
        Record(fa.fields map (_ map f))

      override def foldMap[A, B: Monoid](fa: Record[A])(f: A => B): B =
        fa.fields foldMap { case (_, a) => f(a) }

      override def traverseImpl[G[_]: Applicative, A, B](fa: Record[A])(
          f: A => G[B]
      ): G[Record[B]] =
        Applicative[G].map(fa.fields traverse (_ traverse f))(bs => fa.copy(fields = bs))
    }
}

// Variant TypeDecl`s have an object generated for them in their own file
final case class Variant[+VT](fields: ImmArraySeq[(Ref.Name, VT)])
    extends DataType[Nothing, VT]
    with DataType.GetFields[VT] {

  /** Widen to DataType, in Java. */
  def asDataType[RT, PVT >: VT]: DataType[RT, PVT] = this
}

object Variant extends FWTLike[Variant] {
  implicit val `V traverse`: Traverse[Variant] =
    new Traverse[Variant] with Foldable.FromFoldMap[Variant] {

      override def map[A, B](fa: Variant[A])(f: A => B): Variant[B] =
        Variant(fa.fields map (_ map f))

      override def foldMap[A, B: Monoid](fa: Variant[A])(f: A => B): B =
        fa.fields foldMap { case (_, a) => f(a) }

      override def traverseImpl[G[_]: Applicative, A, B](fa: Variant[A])(
          f: A => G[B]
      ): G[Variant[B]] =
        Applicative[G].map(fa.fields traverse (_ traverse f))(bs => fa.copy(fields = bs))
    }
}

final case class Enum(constructors: ImmArraySeq[Ref.Name]) extends DataType[Nothing, Nothing] {

  /** Widen to DataType, in Java. */
  def asDataType[RT, PVT]: DataType[RT, PVT] = this
}

final case class Iface() extends DataType[Nothing, Nothing] {

  /** Widen to DataType, in Java. */
  def asDataType[RT, PVT]: DataType[RT, PVT] = this
}

final case class DefTemplate[+Ty](choices: Map[Ref.Name, TemplateChoice[Ty]], key: Option[Ty]) {
  def map[B](f: Ty => B): DefTemplate[B] = Functor[DefTemplate].map(this)(f)

  def getChoices: j.Map[Ref.ChoiceName, _ <: TemplateChoice[Ty]] =
    choices.asJava

  def getKey: j.Optional[_ <: Ty] =
    key.fold(j.Optional.empty[Ty])(k => j.Optional.of(k))
}

object DefTemplate {
  type FWT = DefTemplate[Type]

  implicit val `TemplateDecl traverse`: Traverse[DefTemplate] =
    new Traverse[DefTemplate] with Foldable.FromFoldMap[DefTemplate] {
      override def foldMap[A, B: Monoid](fa: DefTemplate[A])(f: A => B): B =
        fa.choices.foldMap(_ foldMap f) |+| (fa.key foldMap f)

      override def traverseImpl[G[_]: Applicative, A, B](
          fab: DefTemplate[A]
      )(f: A => G[B]): G[DefTemplate[B]] =
        ^(fab.choices traverse (_ traverse f), fab.key traverse f) { (choices, key) =>
          fab.copy(choices = choices, key = key)
        }
    }

}

final case class TemplateChoice[+Ty](param: Ty, consuming: Boolean, returnType: Ty) {
  def map[C](f: Ty => C): TemplateChoice[C] =
    Functor[TemplateChoice].map(this)(f)
}

object TemplateChoice {
  type FWT = TemplateChoice[Type]

  implicit val `Choice traverse`: Traverse[TemplateChoice] = new Traverse[TemplateChoice] {
    override def traverseImpl[G[_]: Applicative, A, B](
        fa: TemplateChoice[A]
    )(f: A => G[B]): G[TemplateChoice[B]] =
      ^(f(fa.param), f(fa.returnType)) { (param, returnType) =>
        fa.copy(param = param, returnType = returnType)
      }
  }
}

/** Add aliases to companions. */
sealed abstract class FWTLike[F[+_]] {

  /** Alias for application to [[Type]]. Note that FWT stands for "Field with
    * type", because before we parametrized over both the field and the type,
    * while now we only parametrize over the type.
    */
  type FWT = F[Type]
}
