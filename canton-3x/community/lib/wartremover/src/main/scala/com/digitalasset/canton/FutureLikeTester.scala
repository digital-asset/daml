// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import cats.data.{EitherT, OptionT}
import org.wartremover.WartUniverse

import scala.annotation.{StaticAnnotation, tailrec}
import scala.concurrent.Future

object FutureLikeTester {

  /** The returned predicate tests whether the given type is future-like.
    * A type `T` is considered future-like if one of the following holds:
    * - The type constructor of `T` is [[scala.concurrent.Future]]
    * - The type constructor of `T` is [[cats.data.EitherT]] or [[cats.data.OptionT]]
    *   and its first type argument is future-like.
    * - The type constructor of `T` is annotated with the annotation `futureLikeType`
    */
  def tester(
      u: WartUniverse
  )(futureLikeType: u.universe.Type): u.universe.Type => Boolean = {
    import u.universe.*

    val futureTypeConstructor: Type = typeOf[Future[Unit]].typeConstructor
    val eitherTTypeConstructor: Type =
      typeOf[EitherT[Future, Unit, Unit]].typeConstructor
    val optionTTypeConstructor: Type = typeOf[OptionT[Future, Unit]].typeConstructor
    val futureTransformerType: Type = typeOf[FutureTransformer]

    @tailrec
    def go(typ: Type): Boolean = {
      if (typ.typeConstructor =:= futureTypeConstructor) true
      else if (typ.typeConstructor =:= eitherTTypeConstructor) {
        val args = typ.typeArgs
        args.nonEmpty && go(args(0))
      } else if (typ.typeConstructor =:= optionTTypeConstructor) {
        val args = typ.typeArgs
        args.nonEmpty && go(args(0))
      } else
        typ match {
          case PolyType(_binds, body) => go(body)
          case _ =>
            val annotations = typ.typeConstructor.typeSymbol.annotations
            if (annotations.exists(_.tree.tpe =:= futureLikeType)) true
            else
              annotations.find(_.tree.tpe =:= futureTransformerType) match {
                case Some(transformerAnnotation)
                    if transformerAnnotation.tree.children.sizeCompare(2) >= 0 =>
                  val transformedTypeArgument = transformerAnnotation.tree.children(1)
                  transformedTypeArgument match {
                    case Literal(Constant(i: Int)) if i >= 0 && typ.typeArgs.sizeCompare(i) > 0 =>
                      go(typ.typeArgs(i))
                    case _ => false
                  }
                case _ => false
              }
        }
    }

    go
  }
}

/** Annotation for computation transformer type constructors (e.g., a monad transformer)
  * so that if it will be treated future-like when applied to a future-like computation type.
  *
  * @param transformedTypeArgumentPosition The type argument position for the computation type that is transformed
  */
final case class FutureTransformer(transformedTypeArgumentPosition: Int) extends StaticAnnotation
