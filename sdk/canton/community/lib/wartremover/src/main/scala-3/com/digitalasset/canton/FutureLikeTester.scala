// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import cats.data.{EitherT, OptionT}
import org.wartremover.WartUniverse

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.quoted.{Quotes, Type}
import scala.reflect.{ClassTag, classTag}

object FutureLikeTester {

  /** The returned predicate tests whether the given type is future-like. A type `T` is considered
    * future-like if one of the following holds:
    *   - The type constructor of `T` is [[scala.concurrent.Future]]
    *   - The type constructor of `T` is [[cats.data.EitherT]] or [[cats.data.OptionT]] and its
    *     first type argument is future-like.
    *   - The type constructor of `T` is annotated with the annotation `FutureLike`
    *   - The type constructor of `T` is annotated with the annotation `FutureTransformer(i)` and
    *     the argument at position i is considered future-like
    */
  def tester[FutureLike: Type](u: WartUniverse): u.quotes.reflect.TypeRepr => Boolean = {
    given Quotes = u.quotes
    import u.quotes.reflect.*

    def typeConstructorOf[T: ClassTag] = TypeRepr.typeConstructorOf(classTag[T].runtimeClass)

    val futureTypeConstructor: TypeRepr = typeConstructorOf[Future[?]]
    val eitherTTypeConstructor: TypeRepr = typeConstructorOf[EitherT[?, ?, ?]]
    val optionTTypeConstructor: TypeRepr = typeConstructorOf[OptionT[?, ?]]

    @tailrec
    def go(typ: TypeRepr): Boolean =
      typ.widen match
        case _: NamedType =>
          if typ <:< futureTypeConstructor then true
          else
            val annotations = typ.baseClasses.flatMap(_.annotations)
            annotations.exists(term => term.tpe <:< TypeRepr.of[FutureLike])
        case AppliedType(tycon, args) =>
          if tycon <:< eitherTTypeConstructor || tycon <:< optionTTypeConstructor then go(args(0))
          else
            val annotations = tycon.baseClasses.flatMap(_.annotations)
            val position = annotations.map(_.asExpr).collectFirst {
              case '{ FutureTransformer($position: Int) } => position.valueOrAbort
              case '{ new FutureTransformer($position: Int) } => position.valueOrAbort
            }
            position match
              case Some(position) => go(args(position))
              case None => go(tycon)
        case TypeLambda(_, _, result) => go(result)
        case _ => false

    go(_)
  }
}
