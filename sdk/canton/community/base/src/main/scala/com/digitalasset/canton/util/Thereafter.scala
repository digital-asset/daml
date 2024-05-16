// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.util.Thereafter.EitherTThereafterContent
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
  def thereafter[A](f: F[A])(body: Content[A] => Unit): F[A]

  /** Returns the single `A` in `content` if any. */
  def maybeContent[A](content: Content[A]): Option[A]

  def thereafterSuccessOrFailure[A](f: F[A])(success: A => Unit, failure: => Unit): F[A] =
    thereafter(f)(result => maybeContent(result).fold(failure)(success))

}

/** Extension of [[Thereafter]] that adds the possibility to run an asynchronous piece of code afterwards
  * with proper synchronization and exception propagation.
  */
trait ThereafterAsync[F[_]] extends Thereafter[F] {

  /** runs `body` after the computation `f` has completed
    *
    * @return The computation that results from chaining `f` before `body`. Completes only after `body` has run.
    *         If `body` completes normally, the result of the computation is the same as `f`'s result.
    *         If `body` throws, the result includes the thrown exception.
    *         If `body` produces a failed computation, the result includes the thrown exception.
    */
  def thereafterF[A](f: F[A])(body: Content[A] => Future[Unit]): F[A]

  def thereafterFSuccessOrFailure[A](
      f: F[A]
  )(success: A => Future[Unit], failure: => Future[Unit]): F[A] =
    thereafterF(f)(result => maybeContent(result).fold(failure)(success))
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

  trait Ops[F[_], C[_], A] extends Serializable {
    protected def self: F[A]
    protected val typeClassInstance: Thereafter.Aux[F, C]
    def thereafter(body: C[A] => Unit): F[A] =
      typeClassInstance.thereafter(self)(body)
    def thereafterSuccessOrFailure(success: A => Unit, failure: => Unit): F[A] =
      typeClassInstance.thereafterSuccessOrFailure(self)(success, failure)
  }

  /** Extension method for instances of [[Thereafter]]. */
  object syntax extends ThereafterAsync.syntax {
    import scala.language.implicitConversions
    implicit def ThereafterOps[F[_], A](target: F[A])(implicit
        tc: Thereafter[F]
    ): Ops[F, tc.Content, A] = new Ops[F, tc.Content, A] {
      override val self: F[A] = target
      override protected val typeClassInstance: Thereafter.Aux[F, tc.Content] = tc
    }
  }

  object TryTherafter extends Thereafter[Try] {
    override type Content[A] = Try[A]
    override def thereafter[A](f: Try[A])(body: Try[A] => Unit): Try[A] = f match {
      case result: Success[?] =>
        Try(body(result)).flatMap(_ => result)
      case result @ Failure(resultEx) =>
        val bodyT = Try(body(result))
        // If the body throws an exception, add it as a suppressed exception to the result exception
        bodyT.forFailed { bodyEx =>
          // Avoid an IllegalArgumentException if it's the same exception,
          if (!(resultEx eq bodyEx)) resultEx.addSuppressed(bodyEx)
        }
        result
    }

    override def maybeContent[A](content: Try[A]): Option[A] = content.toOption
  }
  implicit def TryThereafter: Thereafter.Aux[Try, Try] = TryTherafter

  /** [[Thereafter]] instance for [[scala.concurrent.Future]]s */
  class FutureThereafter(implicit ec: ExecutionContext) extends Thereafter[Future] {
    override type Content[A] = Try[A]

    override def thereafter[A](
        f: Future[A]
    )(body: Try[A] => Unit): Future[A] =
      f.transform { result => TryThereafter.thereafter(result)(body) }

    override def maybeContent[A](content: Try[A]): Option[A] = content.toOption
  }
  implicit def futureThereafter(implicit ec: ExecutionContext): Thereafter.Aux[Future, Try] =
    new FutureThereafter

  /** Use a type synonym instead of a type lambda so that the Scala compiler does not get confused during implicit resolution,
    * at least for simple cases.
    */
  type EitherTThereafterContent[Content[_], E, A] = Content[Either[E, A]]

  trait EitherTThereafter[F[_], E, FContent[_]] extends Thereafter[EitherT[F, E, *]] {
    def F: Thereafter.Aux[F, FContent]
    override type Content[A] = EitherTThereafterContent[FContent, E, A]
    override def thereafter[A](f: EitherT[F, E, A])(body: Content[A] => Unit): EitherT[F, E, A] =
      EitherT(F.thereafter(f.value)(body))
    override def maybeContent[A](content: EitherTThereafterContent[FContent, E, A]): Option[A] =
      F.maybeContent(content).flatMap(_.toOption)
  }

  /** [[Thereafter]] instance lifted through [[cats.data.EitherT]]. */
  implicit def eitherTThereafter[F[_], E](implicit
      FF: Thereafter[F]
  ): Thereafter.Aux[EitherT[F, E, *], EitherTThereafterContent[FF.Content, E, *]] =
    new EitherTThereafter[F, E, FF.Content] {
      override def F: Thereafter.Aux[F, FF.Content] = FF
    }

  /** Use a type synonym instead of a type lambda so that the Scala compiler does not get confused during implicit resolution,
    * at least for simple cases.
    */
  type OptionTThereafterContent[Content[_], A] = Content[Option[A]]

  trait OptionTThereafter[F[_], FContent[_]] extends Thereafter[OptionT[F, *]] {
    def F: Thereafter.Aux[F, FContent]
    override type Content[A] = OptionTThereafterContent[FContent, A]
    override def thereafter[A](f: OptionT[F, A])(body: Content[A] => Unit): OptionT[F, A] =
      OptionT(F.thereafter(f.value)(body))
    override def maybeContent[A](content: OptionTThereafterContent[FContent, A]): Option[A] =
      F.maybeContent(content).flatten
  }

  /** [[Thereafter]] instance lifted through [[cats.data.OptionT]]. */
  implicit def optionTThereafter[F[_]](implicit
      FF: Thereafter[F]
  ): Thereafter.Aux[OptionT[F, *], OptionTThereafterContent[FF.Content, *]] =
    new OptionTThereafter[F, FF.Content] {
      override def F: Thereafter.Aux[F, FF.Content] = FF
    }
}

object ThereafterAsync {

  trait syntax {
    import scala.language.implicitConversions
    implicit def ThereafterAsyncOps[F[_], A](target: F[A])(implicit
        tc: ThereafterAsync[F]
    ): ThereafterAsync.Ops[F, tc.Content, A] = new ThereafterAsync.Ops[F, tc.Content, A] {
      override val self: F[A] = target
      protected override val typeClassInstance: ThereafterAsync.Aux[F, tc.Content] = tc
    }
  }

  /** Make the dependent Content type explicit as a type argument to help with type inference.
    *
    * The construction is the same as in shapeless.
    * [The Type Astronaut's Guide to Shapeless Book](https://underscore.io/books/shapeless-guide/)
    * explains the idea in Chapter 4.2.
    */
  type Aux[F[_], C[_]] = ThereafterAsync[F] { type Content[A] = C[A] }
  def apply[F[_]](implicit F: ThereafterAsync[F]): ThereafterAsync.Aux[F, F.Content] = F

  trait Ops[F[_], C[_], A] extends Thereafter.Ops[F, C, A] {
    protected def self: F[A]
    protected override val typeClassInstance: ThereafterAsync.Aux[F, C]
    def thereafterF(body: C[A] => Future[Unit]): F[A] =
      typeClassInstance.thereafterF(self)(body)
    def thereafterFSuccessOrFailure(success: A => Future[Unit], failure: => Future[Unit]): F[A] =
      typeClassInstance.thereafterFSuccessOrFailure(self)(success, failure)
  }

  class FutureThereafterAsync(implicit ec: ExecutionContext)
      extends Thereafter.FutureThereafter
      with ThereafterAsync[Future] {
    override def thereafterF[A](f: Future[A])(body: Try[A] => Future[Unit]): Future[A] = {
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
  implicit def futureThereafterAsync(implicit
      ec: ExecutionContext
  ): ThereafterAsync.Aux[Future, Try] =
    new FutureThereafterAsync

  trait EitherTThereafterAsync[F[_], E, FContent[_]]
      extends Thereafter.EitherTThereafter[F, E, FContent]
      with ThereafterAsync[EitherT[F, E, *]] {
    override def F: ThereafterAsync.Aux[F, FContent]
    override def thereafterF[A](f: EitherT[F, E, A])(
        body: Content[A] => Future[Unit]
    ): EitherT[F, E, A] =
      EitherT(F.thereafterF(f.value)(body))
  }
  implicit def eitherTThereafterAsync[F[_], E](implicit
      FF: ThereafterAsync[F]
  ): ThereafterAsync.Aux[EitherT[F, E, *], EitherTThereafterContent[FF.Content, E, *]] =
    new EitherTThereafterAsync[F, E, FF.Content] {
      override def F: ThereafterAsync.Aux[F, FF.Content] = FF
    }

  trait OptionTThereafterAsync[F[_], FContent[_]]
      extends ThereafterAsync[OptionT[F, *]]
      with Thereafter.OptionTThereafter[F, FContent] {
    override def F: ThereafterAsync.Aux[F, FContent]
    override def thereafterF[A](f: OptionT[F, A])(body: Content[A] => Future[Unit]): OptionT[F, A] =
      OptionT(F.thereafterF(f.value)(body))
  }
  implicit def optionTThereafterAsync[F[_]](implicit FF: ThereafterAsync[F]): ThereafterAsync.Aux[
    OptionT[F, *],
    Thereafter.OptionTThereafterContent[FF.Content, *],
  ] =
    new OptionTThereafterAsync[F, FF.Content] {
      override def F: ThereafterAsync.Aux[F, FF.Content] = FF
    }
}
