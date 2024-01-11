// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.util.TryUtil.*

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Typeclass for computations with an operation that can run a side effect after the computation has finished.
  *
  * The typeclass abstracts the following patterns so that it can be used for types other than [[scala.concurrent.Future]].
  * {{{
  * future.transform { result => val () = body(result); result } // synchronous body
  * future.transform { result => body(result).transform(_ => result) } // asynchronous body
  * }}}
  *
  * Usage:
  * <pre>
  * import com.digitalasset.canton.util.Thereafter.syntax.*
  *
  * myAsyncComputation.thereafter(result => ...)
  * </pre>
  *
  * @tparam F The computation's type functor.
  */
trait Thereafter[F[_]] {

  /** The container type for computation result. */
  type Content[_]

  /** Runs `body` after the computation `f` has completed.
    * @return The computation that results from chaining `f` before `body`. Completes only after `body` has run.
    *         If `body` completes normally, the result of the computation is the same as `f`'s result.
    *         If `body` throws, the result includes the thrown exception.
    */
  def thereafter[A](f: F[A])(body: Content[A] => Unit)(implicit ec: ExecutionContext): F[A]

  /** runs `body` after the computation `f` has completed
    *
    * @return The computation that results from chaining `f` before `body`. Completes only after `body` has run.
    *         If `body` completes normally, the result of the computation is the same as `f`'s result.
    *         If `body` throws, the result includes the thrown exception.
    *         If `body` produces a failed computation, the result includes the thrown exception.
    */
  def thereafterF[A](f: F[A])(body: Content[A] => Future[Unit])(implicit ec: ExecutionContext): F[A]
}

object Thereafter {

  /** Make the dependent Content type explicit as a type argument to help with type inference.
    *
    * The construction is the same as in shapeless.
    * [The Type Astronaut's Guide to Shapeless Book](https://underscore.io/books/shapeless-guide/)
    * explains the idea in Chapter 4.2.
    */
  type Aux[F[_], C[_]] = Thereafter[F] { type Content[A] = C[A] }

  def apply[F[_]](implicit F: Thereafter[F]): Thereafter.Aux[F, F.Content] = F

  /** [[Thereafter]] instance for [[scala.concurrent.Future]]s */
  object FutureThereafter extends Thereafter[Future] {
    override type Content[A] = Try[A]

    override def thereafter[A](
        f: Future[A]
    )(body: Try[A] => Unit)(implicit ec: ExecutionContext): Future[A] =
      f.transform {
        case result: Success[?] =>
          body(result)
          result
        case result @ Failure(resultEx) =>
          val bodyT = Try(body(result))
          // If the body throws an exception, add it as a suppressed exception to the result exception
          bodyT.forFailed { bodyEx =>
            // Avoid an IllegalArgumentException if it's the same exception,
            if (!(resultEx eq bodyEx)) resultEx.addSuppressed(bodyEx)
          }
          result
      }

    override def thereafterF[A](f: Future[A])(body: Try[A] => Future[Unit])(implicit
        ec: ExecutionContext
    ): Future[A] = {
      f.transformWith {
        case result @ Success(success) =>
          body(result).map { (_: Unit) => success }
        case result @ Failure(resultEx) =>
          Future.fromTry(Try(body(result))).flatten.transform {
            case Success(_) => result
            case Failure(bodyEx) =>
              if (!(resultEx eq bodyEx)) resultEx.addSuppressed(bodyEx)
              result
          }
      }
    }
  }
  implicit val futureThereafter: Thereafter.Aux[Future, Try] = FutureThereafter

  /** Use a type synonym instead of a type lambda so that the Scala compiler does not get confused during implicit resolution,
    * at least for simple cases.
    */
  type EitherTThereafterContent[Content[_], E, A] = Content[Either[E, A]]

  /** [[Thereafter]] instance lifted through [[cats.data.EitherT]]. */
  implicit def eitherTThereafter[F[_], E](implicit
      F: Thereafter[F]
  ): Thereafter.Aux[EitherT[F, E, *], EitherTThereafterContent[F.Content, E, *]] =
    new Thereafter[EitherT[F, E, *]] {
      override type Content[A] = EitherTThereafterContent[F.Content, E, A]
      override def thereafter[A](f: EitherT[F, E, A])(body: Content[A] => Unit)(implicit
          ec: ExecutionContext
      ): EitherT[F, E, A] =
        EitherT(F.thereafter(f.value)(body))

      override def thereafterF[A](f: EitherT[F, E, A])(
          body: Content[A] => Future[Unit]
      )(implicit ec: ExecutionContext): EitherT[F, E, A] =
        EitherT(F.thereafterF(f.value)(body))
    }

  /** Use a type synonym instead of a type lambda so that the Scala compiler does not get confused during implicit resolution,
    * at least for simple cases.
    */
  type OptionTThereafterContent[Content[_], A] = Content[Option[A]]

  /** [[Thereafter]] instance lifted through [[cats.data.OptionT]]. */
  implicit def optionTThereafter[F[_]](implicit
      F: Thereafter[F]
  ): Thereafter.Aux[OptionT[F, *], OptionTThereafterContent[F.Content, *]] =
    new Thereafter[OptionT[F, *]] {
      override type Content[A] = OptionTThereafterContent[F.Content, A]

      override def thereafter[A](f: OptionT[F, A])(body: Content[A] => Unit)(implicit
          ec: ExecutionContext
      ): OptionT[F, A] =
        OptionT(F.thereafter(f.value)(body))

      override def thereafterF[A](f: OptionT[F, A])(
          body: Content[A] => Future[Unit]
      )(implicit ec: ExecutionContext): OptionT[F, A] =
        OptionT(F.thereafterF(f.value)(body))
    }

  trait Ops[F[_], C[_], A] extends Serializable {
    protected def self: F[A]
    val typeClassInstance: Thereafter.Aux[F, C]
    def thereafter(body: C[A] => Unit)(implicit ec: ExecutionContext): F[A] =
      typeClassInstance.thereafter(self)(body)
    def thereafterF(body: C[A] => Future[Unit])(implicit ec: ExecutionContext): F[A] =
      typeClassInstance.thereafterF(self)(body)
  }

  /** Extension method for instances of [[Thereafter]]. */
  object syntax {
    import scala.language.implicitConversions
    implicit def ThereafterOps[F[_], A](target: F[A])(implicit
        tc: Thereafter[F]
    ): Ops[F, tc.Content, A] = new Ops[F, tc.Content, A] {
      override val self: F[A] = target
      override val typeClassInstance: Thereafter.Aux[F, tc.Content] = tc
    }
  }
}
