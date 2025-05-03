// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Id
import cats.data.{EitherT, Nested, OptionT}
import cats.syntax.either.*
import com.digitalasset.canton.Uninhabited
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.util.TryUtil.*

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Typeclass for computations with an operation that can run a side effect after the computation
  * has finished.
  *
  * The typeclass abstracts the following patterns so that it can be used for types other than
  * [[scala.concurrent.Future]].
  * {{{
  * future.transform { result => val () = body(result); result } // synchronous body
  * future.transformWith { result => body(result).transform(_ => result) } // asynchronous body
  * }}}
  *
  * Usage:
  * {{{
  * import com.digitalasset.canton.util.Thereafter.syntax.*
  *
  * myAsyncComputation.thereafter(result => ...) // synchronous body
  * myAsyncComputation.thereafterF(result => ...) // asynchronous body
  * }}}
  *
  * It is preferred to similar functions such as [[scala.concurrent.Future.andThen]] because it
  * properly chains exceptions from the side-effecting computation back into the original
  * computation.
  *
  * @tparam F
  *   The computation's type functor.
  */
trait Thereafter[F[_]] {

  /** The container type for computation result.
    *
    * Must contain at most one element of the given type parameter.
    */
  type Content[_]

  /** Evidences that [[Content]] is actually covariant in its type argument.
    *
    * We do not declare [[Content]] itself as covariant in the type definition because this would
    * leak into the `Thereafter.Aux` pattern and thereby break the compiler's type inference.
    */
  def covariantContent[A, B](implicit ev: A <:< B): Content[A] <:< Content[B]

  /** Defines the shapes under which a [[Content]] does contain a single element of the type
    * argument. With [[withShape]]`(shape, x)`, we can uniquely reconstruct the [[Content]] from a
    * value `x` and its `shape`, which is needed for implementing [[thereafter]] in terms of
    * [[transformChaining]].
    *
    * For example, if [[Content]] is `Either[String, *]`, then only the form `Right(x)` contains
    * such an element `x`. So they are all of the same shape `Right` and so the shape can be any
    * singleton type such as `Right.type` or `Unit`. Conversely, if [[Content]] is `Tuple2(Int, *)`,
    * then an element `x` can appear in many values, e.g., `(0, x)` and `(1, x)` of different
    * shapes. In this case, the shape type would be `Int` so that we can reconstruct `(0, x)` or
    * `(1, x)` from the value `x` and the corresponding shape `0` or `1`.
    *
    * Formally, `Shape` is the type of one-hole contexts for [[Content]], also known as zippers,
    * which is the derivative of [[Content]]/ As such, `Shape` should depend on its type argument.
    * However, as [[Content]] contains at most element of the given argument type, its derivative is
    * always constant. So we can omit the type argument altogether.
    *
    * @see
    *   https://en.wikipedia.org/wiki/Zipper_(data_structure)
    */
  type Shape

  /** Plugs the value `x` into the hole of the shape interpreted as a one-hole context.
    */
  def withShape[A](shape: Shape, x: A): Content[A]

  /** Runs either `empty` or `single` after the computation `f` has run.
    *   - If `f` does not produce a value of type `A`, `empty` is run. If `empty` completes
    *     normally, `f` is returned. If `empty` throws, the thrown exception is incorporated into
    *     `f` as follows:
    *     - If `f` did not throw an exception, the exception overrides the result of `f`.
    *     - If `f` did throw an exception, the exception from `empty` is added as a suppressed
    *       exception.
    *   - Otherwise, if `f` produces a single value `a` of type `A`, then the behavior is equivalent
    *     to `f.map(single)`.
    *
    * This method is more general than [[thereafter]] because `single` can transform the value. This
    * is necessary for instances such as [[cats.data.Nested]] where the nested computation must be
    * transformed, which is not possible with [[thereafter]] for the outer computation. Note that a
    * plain functor on the outer computation does not suffice because the transformation of the
    * inner computation could throw and it is not guaranteed that the functor properly chains the
    * exceptions. Nested computations are useful for keeping the several computations
    * distinguishable, such as the synchronous and asynchronous parts of message processing.
    *
    * The signature of `single` separates the `Shape` from the value to ensure that `single` only
    * transforms the value, but does not change the shape. This is crucial for [[cats.data.Nested]]
    * as we would otherwise have to be able to replace the inner computation with an empty content
    * of the outer, which doesn't make sense.
    */
  def transformChaining[A](
      f: F[A]
  )(empty: Content[Uninhabited] => Unit, single: (Shape, A) => A): F[A]

  /** Runs `body` after the computation `f` has completed.
    *
    * @return
    *   The computation that results from chaining `f` before `body`. Completes only after `body`
    *   has run. If `body` completes normally, the result of the computation is the same as `f`'s
    *   result. If `body` throws, the result includes the thrown exception.
    */
  def thereafter[A](f: F[A])(body: Content[A] => Unit): F[A] =
    transformChaining(f)(
      empty = content => body(covariantContent[Uninhabited, A].apply(content)),
      single = { (shape, a) =>
        body(withShape(shape, a))
        a
      },
    )

  def thereafterP[A](f: F[A])(body: PartialFunction[Content[A], Unit]): F[A] =
    thereafter(f)(body.lift(_).discard[Option[Unit]])

  def thereafterSuccessOrFailure[A](f: F[A])(success: A => Unit, failure: => Unit): F[A] =
    transformChaining(f)(
      empty = _ => failure,
      single = { (_, a) =>
        success(a)
        a
      },
    )
}

/** Extension of [[Thereafter]] that adds the possibility to run an asynchronous piece of code
  * afterwards with proper synchronization and exception propagation.
  */
trait ThereafterAsync[F[_]] extends Thereafter[F] {

  def executionContext: ExecutionContext

  def transformChainingF[A](
      f: F[A]
  )(empty: Content[Uninhabited] => Future[Unit], single: (Shape, A) => Future[A]): F[A]

  /** runs `body` after the computation `f` has completed
    *
    * @return
    *   The computation that results from chaining `f` before `body`. Completes only after `body`
    *   has run. If `body` completes normally, the result of the computation is the same as `f`'s
    *   result. If `body` throws, the result includes the thrown exception. If `body` produces a
    *   failed computation, the result includes the thrown exception.
    */
  def thereafterF[A](f: F[A])(body: Content[A] => Future[Unit]): F[A] =
    transformChainingF(f)(
      empty = content => body(covariantContent[Uninhabited, A].apply(content)),
      single = { (shape, a) =>
        body(withShape(shape, a)).map(_ => a)(executionContext)
      },
    )

  def thereafterFSuccessOrFailure[A](
      f: F[A]
  )(success: A => Future[Unit], failure: => Future[Unit]): F[A] =
    transformChainingF(f)(
      empty = _ => failure,
      single = { (_, a) =>
        success(a).map(_ => a)(executionContext)
      },
    )
}

object Thereafter {

  /** Make the dependent Content type explicit as a type argument to help with type inference.
    *
    * The construction is the same as in shapeless. [The Type Astronaut's Guide to Shapeless
    * Book](https://underscore.io/books/shapeless-guide/) explains the idea in Chapter 4.2.
    */
  type Aux[F[_], C[_], S] = Thereafter[F] { type Content[A] = C[A]; type Shape = S }
  def apply[F[_]](implicit F: Thereafter[F]): Thereafter.Aux[F, F.Content, F.Shape] = F

  trait Ops[F[_], C[_], S, A] extends Serializable {
    protected def self: F[A]
    protected val typeClassInstance: Thereafter.Aux[F, C, S]
    def thereafter(body: C[A] => Unit): F[A] =
      typeClassInstance.thereafter(self)(body)
    def thereafterP(body: PartialFunction[C[A], Unit]): F[A] =
      typeClassInstance.thereafterP(self)(body)
    def thereafterSuccessOrFailure(success: A => Unit, failure: => Unit): F[A] =
      typeClassInstance.thereafterSuccessOrFailure(self)(success, failure)
    // We do not expose transformChaining here because its a somewhat internal method that's
    // needed only for composition, but not by users of `Thereafter`
  }

  /** Extension method for instances of [[Thereafter]]. */
  object syntax extends ThereafterAsync.syntax {
    import scala.language.implicitConversions
    implicit def ThereafterOps[F[_], A](target: F[A])(implicit
        tc: Thereafter[F]
    ): Ops[F, tc.Content, tc.Shape, A] = new Ops[F, tc.Content, tc.Shape, A] {
      override val self: F[A] = target
      override protected val typeClassInstance: Thereafter.Aux[F, tc.Content, tc.Shape] = tc
    }
  }

  private object IdThereafter extends Thereafter[Id] {
    override type Content[A] = A
    override def covariantContent[A, B](implicit ev: A <:< B): A <:< B = ev

    override type Shape = Unit
    override def withShape[A](shape: Unit, x: A): A = x

    override def transformChaining[A](
        f: Id[A]
    )(empty: Uninhabited => Unit, single: (Shape, A) => A): Id[A] = single((), f)
  }
  implicit def idThereafter: Thereafter.Aux[Id, Id, Unit] = IdThereafter

  private object TryThereafter extends Thereafter[Try] {
    override type Content[A] = Try[A]
    override def covariantContent[A, B](implicit ev: A <:< B): Try[A] <:< Try[B] = ev.liftCo[Try]

    override type Shape = Unit
    override def withShape[A](shape: Unit, x: A): Try[A] = Success(x)

    override def transformChaining[A](
        f: Try[A]
    )(empty: Try[Uninhabited] => Unit, single: (Shape, A) => A): Try[A] =
      f match {
        case Success(x) => Try(single((), x))
        case result @ Failure(resultEx) =>
          val emptyT = Try(empty(Failure(resultEx)))
          // If the body throws an exception, add it as a suppressed exception to the result exception
          emptyT.forFailed { emptyEx =>
            // Avoid an IllegalArgumentException if it's the same exception,
            if (!(resultEx eq emptyEx)) resultEx.addSuppressed(emptyEx)
          }
          result
      }
  }
  implicit def tryThereafter: Thereafter.Aux[Try, Try, Unit] = TryThereafter

  /** [[Thereafter]] instance for [[scala.concurrent.Future]]s */
  class FutureThereafter(implicit ec: ExecutionContext) extends Thereafter[Future] {
    override type Content[A] = Try[A]
    override def covariantContent[A, B](implicit ev: A <:< B): Try[A] <:< Try[B] = ev.liftCo[Try]

    override type Shape = Unit
    override def withShape[A](shape: Unit, x: A): Try[A] = Success(x)

    override def transformChaining[A](
        f: Future[A]
    )(empty: Try[Uninhabited] => Unit, single: (Shape, A) => A): Future[A] =
      f.transform(result => TryThereafter.transformChaining(result)(empty, single))
  }
  implicit def futureThereafter(implicit ec: ExecutionContext): Thereafter.Aux[Future, Try, Unit] =
    new FutureThereafter

  /** Use a type synonym instead of a type lambda so that the Scala compiler does not get confused
    * during implicit resolution, at least for simple cases.
    */
  type EitherTThereafterContent[Content[_], E, A] = Content[Either[E, A]]

  trait EitherTThereafter[F[_], E, FContent[_], FShape] extends Thereafter[EitherT[F, E, *]] {
    def F: Thereafter.Aux[F, FContent, FShape]
    override type Content[A] = EitherTThereafterContent[FContent, E, A]
    override def covariantContent[A, B](implicit ev: A <:< B): Content[A] <:< Content[B] =
      F.covariantContent[Either[E, A], Either[E, B]](ev.liftCo[Either[E, +*]])

    override type Shape = FShape
    override def withShape[A](shape: FShape, x: A): EitherTThereafterContent[FContent, E, A] =
      F.withShape(shape, Right(x))

    override def transformChaining[A](f: EitherT[F, E, A])(
        empty: EitherTThereafterContent[FContent, E, Uninhabited] => Unit,
        single: (Shape, A) => A,
    ): EitherT[F, E, A] =
      EitherT(
        F.transformChaining(f.value)(
          empty = fcontent =>
            empty(F.covariantContent[Uninhabited, Either[E, Uninhabited]].apply(fcontent)),
          single = {
            case (shape, left @ Left(e)) =>
              // Run this only for the effect
              empty(F.withShape(shape, Either.left(e)))
              left
            case (shape, Right(x)) => Right(single(shape, x))
          },
        )
      )
  }

  /** [[Thereafter]] instance lifted through [[cats.data.EitherT]]. */
  implicit def eitherTThereafter[F[_], E](implicit
      FF: Thereafter[F]
  ): Thereafter.Aux[EitherT[F, E, *], EitherTThereafterContent[FF.Content, E, *], FF.Shape] =
    new EitherTThereafter[F, E, FF.Content, FF.Shape] {
      override def F: Thereafter.Aux[F, FF.Content, FF.Shape] = FF
    }

  /** Use a type synonym instead of a type lambda so that the Scala compiler does not get confused
    * during implicit resolution, at least for simple cases.
    */
  type OptionTThereafterContent[Content[_], A] = Content[Option[A]]

  trait OptionTThereafter[F[_], FContent[_], FShape] extends Thereafter[OptionT[F, *]] {
    def F: Thereafter.Aux[F, FContent, FShape]
    override type Content[A] = OptionTThereafterContent[FContent, A]

    override def covariantContent[A, B](implicit ev: A <:< B): Content[A] <:< Content[B] =
      F.covariantContent[Option[A], Option[B]](ev.liftCo[Option])

    override type Shape = FShape
    override def withShape[A](shape: FShape, x: A): OptionTThereafterContent[FContent, A] =
      F.withShape(shape, Some(x))

    override def transformChaining[A](f: OptionT[F, A])(
        empty: OptionTThereafterContent[FContent, Uninhabited] => Unit,
        single: (Shape, A) => A,
    ): OptionT[F, A] = OptionT(
      F.transformChaining(f.value)(
        empty =
          fcontent => empty(F.covariantContent[Uninhabited, Option[Uninhabited]].apply(fcontent)),
        single = {
          case (shape, None) =>
            empty(F.withShape(shape, None))
            None
          case (shape, Some(x)) => Some(single(shape, x))
        },
      )
    )
  }

  /** [[Thereafter]] instance lifted through [[cats.data.OptionT]]. */
  implicit def optionTThereafter[F[_]](implicit
      FF: Thereafter[F]
  ): Thereafter.Aux[OptionT[F, *], OptionTThereafterContent[FF.Content, *], FF.Shape] =
    new OptionTThereafter[F, FF.Content, FF.Shape] {
      override def F: Thereafter.Aux[F, FF.Content, FF.Shape] = FF
    }

  type ComposedThereafterContent[FContent[_], GContent[_], A] = FContent[GContent[A]]
  trait ComposedThereafter[F[_], FContent[_], FShape, G[_], GContent[_], GShape]
      extends Thereafter[Lambda[a => F[G[a]]]] {
    def F: Thereafter.Aux[F, FContent, FShape]
    def G: Thereafter.Aux[G, GContent, GShape]

    override type Content[A] = ComposedThereafterContent[FContent, GContent, A]
    override def covariantContent[A, B](implicit ev: A <:< B): Content[A] <:< Content[B] =
      F.covariantContent[GContent[A], GContent[B]](G.covariantContent)

    override type Shape = (FShape, GShape)
    override def withShape[A](
        shape: (FShape, GShape),
        x: A,
    ): ComposedThereafterContent[FContent, GContent, A] = {
      val (fshape, gshape) = shape
      F.withShape(fshape, G.withShape(gshape, x))
    }

    override def transformChaining[A](f: F[G[A]])(
        empty: Content[Uninhabited] => Unit,
        single: (Shape, A) => A,
    ): F[G[A]] =
      F.transformChaining(f)(
        empty =
          fcontent => empty(F.covariantContent[Uninhabited, GContent[Uninhabited]].apply(fcontent)),
        single = (fshape, ga) =>
          G.transformChaining(ga)(
            empty = gcontent => empty(F.withShape(fshape, gcontent)),
            single = (gshape, a) => single((fshape, gshape), a),
          ),
      )
  }
  def composeThereafter[F[_], G[_]](
      FF: Thereafter[F],
      GG: Thereafter[G],
  ): Thereafter.Aux[
    Lambda[a => F[G[a]]],
    ComposedThereafterContent[FF.Content, GG.Content, *],
    (FF.Shape, GG.Shape),
  ] = new ComposedThereafter[F, FF.Content, FF.Shape, G, GG.Content, GG.Shape] {
    override def F: Thereafter.Aux[F, FF.Content, FF.Shape] = FF
    override def G: Thereafter.Aux[G, GG.Content, GG.Shape] = GG
  }

  trait NestedThereafter[F[_], FContent[_], FShape, G[_], GContent[_], GShape]
      extends Thereafter[Nested[F, G, *]] {
    override type Content[A] = ComposedThereafterContent[FContent, GContent, A]
    override type Shape = (FShape, GShape)

    def composed: Thereafter.Aux[Lambda[a => F[G[a]]], Content, Shape]

    override def covariantContent[A, B](implicit ev: A <:< B): Content[A] <:< Content[B] =
      composed.covariantContent

    override def withShape[A](shape: Shape, x: A): Content[A] =
      composed.withShape(shape, x)

    override def transformChaining[A](
        f: Nested[F, G, A]
    )(empty: Content[Uninhabited] => Unit, single: (Shape, A) => A): Nested[F, G, A] =
      Nested(composed.transformChaining(f.value)(empty, single))
  }

  implicit def nestedThereafter[F[_], G[_]](implicit
      FF: Thereafter[F],
      GG: Thereafter[G],
  ): Thereafter.Aux[
    Nested[F, G, *],
    ComposedThereafterContent[FF.Content, GG.Content, *],
    (FF.Shape, GG.Shape),
  ] = new NestedThereafter[F, FF.Content, FF.Shape, G, GG.Content, GG.Shape] {
    override val composed: Thereafter.Aux[Lambda[a => F[G[a]]], Content, Shape] =
      composeThereafter(FF, GG)
  }
}

object ThereafterAsync {

  trait syntax {
    import scala.language.implicitConversions
    implicit def ThereafterAsyncOps[F[_], A](target: F[A])(implicit
        tc: ThereafterAsync[F]
    ): ThereafterAsync.Ops[F, tc.Content, tc.Shape, A] =
      new ThereafterAsync.Ops[F, tc.Content, tc.Shape, A] {
        override val self: F[A] = target
        protected override val typeClassInstance: ThereafterAsync.Aux[F, tc.Content, tc.Shape] = tc
      }
  }

  /** Make the dependent Content type explicit as a type argument to help with type inference.
    *
    * The construction is the same as in shapeless. [The Type Astronaut's Guide to Shapeless
    * Book](https://underscore.io/books/shapeless-guide/) explains the idea in Chapter 4.2.
    */
  type Aux[F[_], C[_], S] = ThereafterAsync[F] { type Content[A] = C[A]; type Shape = S }
  def apply[F[_]](implicit F: ThereafterAsync[F]): ThereafterAsync.Aux[F, F.Content, F.Shape] = F
  trait Ops[F[_], C[_], S, A] extends Thereafter.Ops[F, C, S, A] {
    protected def self: F[A]
    protected override val typeClassInstance: ThereafterAsync.Aux[F, C, S]
    def thereafterF(body: C[A] => Future[Unit]): F[A] =
      typeClassInstance.thereafterF(self)(body)
    def thereafterFSuccessOrFailure(success: A => Future[Unit], failure: => Future[Unit]): F[A] =
      typeClassInstance.thereafterFSuccessOrFailure(self)(success, failure)
  }

  class FutureThereafterAsync(implicit ec: ExecutionContext)
      extends Thereafter.FutureThereafter
      with ThereafterAsync[Future] {
    override def executionContext: ExecutionContext = ec

    override def transformChainingF[A](
        f: Future[A]
    )(empty: Try[Uninhabited] => Future[Unit], single: (Unit, A) => Future[A]): Future[A] =
      f.transformWith {
        case Success(success) =>
          single((), success)
        case result @ Failure(resultEx) =>
          Future.fromTry(Try(empty(Failure(resultEx)))).flatten.transform {
            case Success(_) => result
            case Failure(bodyEx) =>
              if (!(resultEx eq bodyEx)) resultEx.addSuppressed(bodyEx)
              result
          }
      }
  }
  implicit def futureThereafterAsync(implicit
      ec: ExecutionContext
  ): ThereafterAsync.Aux[Future, Try, Unit] =
    new FutureThereafterAsync

  trait EitherTThereafterAsync[F[_], E, FContent[_], FShape]
      extends Thereafter.EitherTThereafter[F, E, FContent, FShape]
      with ThereafterAsync[EitherT[F, E, *]] {
    override def F: ThereafterAsync.Aux[F, FContent, FShape]
    override def executionContext: ExecutionContext = F.executionContext

    override def transformChainingF[A](f: EitherT[F, E, A])(
        empty: Content[Uninhabited] => Future[Unit],
        single: (Shape, A) => Future[A],
    ): EitherT[F, E, A] =
      EitherT(
        F.transformChainingF(f.value)(
          empty = fcontent =>
            empty(F.covariantContent[Uninhabited, Either[E, Uninhabited]].apply(fcontent)),
          single = {
            case (shape, left @ Left(e)) =>
              // Run this only for the effect
              empty(F.withShape(shape, Either.left(e))).map(_ => left)(executionContext)
            case (shape, Right(x)) => single(shape, x).map(Right(_))(executionContext)
          },
        )
      )
  }
  implicit def eitherTThereafterAsync[F[_], E](implicit
      FF: ThereafterAsync[F]
  ): ThereafterAsync.Aux[
    EitherT[F, E, *],
    Thereafter.EitherTThereafterContent[FF.Content, E, *],
    FF.Shape,
  ] = new EitherTThereafterAsync[F, E, FF.Content, FF.Shape] {
    override def F: ThereafterAsync.Aux[F, FF.Content, FF.Shape] = FF
  }

  trait OptionTThereafterAsync[F[_], FContent[_], FShape]
      extends ThereafterAsync[OptionT[F, *]]
      with Thereafter.OptionTThereafter[F, FContent, FShape] {
    override def F: ThereafterAsync.Aux[F, FContent, FShape]
    override def executionContext: ExecutionContext = F.executionContext

    override def transformChainingF[A](f: OptionT[F, A])(
        empty: Content[Uninhabited] => Future[Unit],
        single: (Shape, A) => Future[A],
    ): OptionT[F, A] =
      OptionT(
        F.transformChainingF(f.value)(
          empty =
            fcontent => empty(F.covariantContent[Uninhabited, Option[Uninhabited]].apply(fcontent)),
          single = {
            case (shape, None) =>
              empty(F.withShape(shape, None)).map(_ => None)(executionContext)
            case (shape, Some(x)) => single(shape, x).map(Some(_))(executionContext)
          },
        )
      )
  }
  implicit def optionTThereafterAsync[F[_]](implicit FF: ThereafterAsync[F]): ThereafterAsync.Aux[
    OptionT[F, *],
    Thereafter.OptionTThereafterContent[FF.Content, *],
    FF.Shape,
  ] = new OptionTThereafterAsync[F, FF.Content, FF.Shape] {
    override def F: ThereafterAsync.Aux[F, FF.Content, FF.Shape] = FF
  }

  trait ComposedThereafterAsync[F[_], FContent[_], FShape, G[_], GContent[_], GShape]
      extends ThereafterAsync[Lambda[a => F[G[a]]]]
      with Thereafter.ComposedThereafter[F, FContent, FShape, G, GContent, GShape] {
    override def F: ThereafterAsync.Aux[F, FContent, FShape]
    override def G: ThereafterAsync.Aux[G, GContent, GShape]
    override def executionContext: ExecutionContext =
      // We arbitrarily pick the execution context from the outer computation
      F.executionContext

    override def transformChainingF[A](f: F[G[A]])(
        empty: Content[Uninhabited] => Future[Unit],
        single: (Shape, A) => Future[A],
    ): F[G[A]] =
      F.transformChainingF(f)(
        empty =
          fcontent => empty(F.covariantContent[Uninhabited, GContent[Uninhabited]].apply(fcontent)),
        single = (fshape, ga) =>
          Future.successful(
            G.transformChainingF(ga)(
              empty = gcontent => empty(F.withShape(fshape, gcontent)),
              single = (gshape, a) => single((fshape, gshape), a),
            )
          ),
      )
  }
  def composeThereafterAsync[F[_], G[_]](
      FF: ThereafterAsync[F],
      GG: ThereafterAsync[G],
  ): ThereafterAsync.Aux[
    Lambda[a => F[G[a]]],
    Thereafter.ComposedThereafterContent[FF.Content, GG.Content, *],
    (FF.Shape, GG.Shape),
  ] = new ComposedThereafterAsync[F, FF.Content, FF.Shape, G, GG.Content, GG.Shape] {
    override def F: ThereafterAsync.Aux[F, FF.Content, FF.Shape] = FF
    override def G: ThereafterAsync.Aux[G, GG.Content, GG.Shape] = GG
  }

  trait NestedThereafterAsync[F[_], FContent[_], FShape, G[_], GContent[_], GShape]
      extends ThereafterAsync[Nested[F, G, *]]
      with Thereafter.NestedThereafter[F, FContent, FShape, G, GContent, GShape] {
    override def composed: ThereafterAsync.Aux[Lambda[a => F[G[a]]], Content, Shape]

    override def executionContext: ExecutionContext = composed.executionContext

    override def transformChainingF[A](f: Nested[F, G, A])(
        empty: Content[Uninhabited] => Future[Unit],
        single: (Shape, A) => Future[A],
    ): Nested[F, G, A] = Nested(composed.transformChainingF(f.value)(empty, single))
  }

  implicit def nestedThereafterAsync[F[_], G[_]](implicit
      FF: ThereafterAsync[F],
      GG: ThereafterAsync[G],
  ): ThereafterAsync.Aux[
    Nested[F, G, *],
    Thereafter.ComposedThereafterContent[FF.Content, GG.Content, *],
    (FF.Shape, GG.Shape),
  ] = new NestedThereafterAsync[F, FF.Content, FF.Shape, G, GG.Content, GG.Shape] {
    override val composed: ThereafterAsync.Aux[Lambda[a => F[G[a]]], Content, Shape] =
      composeThereafterAsync(FF, GG)
  }

}
