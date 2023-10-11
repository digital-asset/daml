// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.{EitherT, OptionT}

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
  * @tparam Content The container type for computation result. Functionally dependent on `F`.
  */
trait Thereafter[F[_], Content[_]] extends Any {

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
  def apply[F[_]]: ThereafterSummon[F] = new ThereafterSummon()
  class ThereafterSummon[F[_]](val dummy: Boolean = false) extends AnyVal {
    def summon[Content[_]](implicit F: Thereafter[F, Content]): Thereafter[F, Content] = F
  }

  /** [[Thereafter]] instance for [[scala.concurrent.Future]]s */
  object FutureThereafter extends Thereafter[Future, Try] {
    override def thereafter[A](
        f: Future[A]
    )(body: Try[A] => Unit)(implicit ec: ExecutionContext): Future[A] =
      f.transform {
        case result: Success[_] =>
          body(result)
          result
        case result @ Failure(resultEx) =>
          val bodyT = Try(body(result))
          // If the body throws an exception, add it as a suppressed exception to the result exception
          bodyT.failed.foreach { bodyEx =>
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
  implicit val futureThereafter: Thereafter[Future, Try] = FutureThereafter

  /** [[Thereafter]] instance lifted through [[cats.data.EitherT]]. */
  class EitherTThereafter[F[_], Content[_], E](implicit val F: Thereafter[F, Content])
      extends Thereafter[EitherT[F, E, *], EitherTThereafterContent[Content, E, *]] {
    override def thereafter[A](f: EitherT[F, E, A])(body: Content[Either[E, A]] => Unit)(implicit
        ec: ExecutionContext
    ): EitherT[F, E, A] =
      EitherT(F.thereafter(f.value)(body))

    override def thereafterF[A](f: EitherT[F, E, A])(
        body: Content[Either[E, A]] => Future[Unit]
    )(implicit ec: ExecutionContext): EitherT[F, E, A] =
      EitherT(F.thereafterF(f.value)(body))
  }

  /** Use a type synonym instead of a type lambda so that the Scala compiler does not get confused during implicit resolution,
    * at least for simple cases.
    */
  type EitherTThereafterContent[Content[_], E, A] = Content[Either[E, A]]
  implicit def eitherTThereafter[F[_], Content[_], E](implicit
      F: Thereafter[F, Content]
  ): EitherTThereafter[F, Content, E] = new EitherTThereafter

  /** [[Thereafter]] instance lifted through [[cats.data.OptionT]]. */
  class OptionTThereafter[F[_], Content[_], E](implicit val F: Thereafter[F, Content])
      extends Thereafter[OptionT[F, *], OptionTThereafterContent[Content, *]] {
    override def thereafter[A](f: OptionT[F, A])(body: Content[Option[A]] => Unit)(implicit
        ec: ExecutionContext
    ): OptionT[F, A] =
      OptionT(F.thereafter(f.value)(body))

    override def thereafterF[A](f: OptionT[F, A])(
        body: Content[Option[A]] => Future[Unit]
    )(implicit ec: ExecutionContext): OptionT[F, A] =
      OptionT(F.thereafterF(f.value)(body))
  }

  /** Use a type synonym instead of a type lambda so that the Scala compiler does not get confused during implicit resolution,
    * at least for simple cases.
    */
  type OptionTThereafterContent[Content[_], A] = Content[Option[A]]
  implicit def optionTThereafter[F[_], Content[_], E](implicit
      F: Thereafter[F, Content]
  ): OptionTThereafter[F, Content, E] = new OptionTThereafter

  trait Ops[F[_], Content[_], A] extends Serializable {
    def self: F[A]
    val typeClassInstance: Thereafter[F, Content]
    def thereafter(body: Content[A] => Unit)(implicit ec: ExecutionContext): F[A] =
      typeClassInstance.thereafter(self)(body)
    def thereafterF(body: Content[A] => Future[Unit])(implicit ec: ExecutionContext): F[A] =
      typeClassInstance.thereafterF(self)(body)
  }

  /** Extension method for instances of [[Thereafter]]. */
  object syntax {
    implicit class ThereafterOps[F[_], Content[_], A](target: F[A])(implicit
        tc: Thereafter[F, Content]
    ) extends Ops[F, Content, A] {
      val self: F[A] = target
      val typeClassInstance: Thereafter[F, Content] = tc
    }
  }
}
