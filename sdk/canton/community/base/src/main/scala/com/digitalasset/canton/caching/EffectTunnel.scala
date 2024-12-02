// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import cats.arrow.FunctionK
import cats.data.EitherT
import cats.syntax.either.*
import cats.~>
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NoStackTrace

/** Type class for disguising the effect of `F` in the effect of `G` temporarily
  * so that `F`'s effect can tunnel through an API that supports only `G`.
  *
  * For example, let `F` be [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]],
  * and `G` be [[scala.concurrent.Future]] (see
  * [[caching.EffectTunnel.effectTunnelFutureUnlessShutdown]]).
  * Then we can enter the tunnel `G` by converting
  * [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]]s
  * into a dedicated exception in a failed [[scala.concurrent.Future]] and exit the tunnel
  * again by converting the exception back into [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]].
  * This obviously assumes that the API that supports only [[scala.concurrent.Future]]
  * does not interact with the dedicated exception.
  */
trait EffectTunnel[F[_], G[_]] {

  /** Converts the effect `F` into the effect `G`.
    * Must be the right-inverse of [[exit]], i.e., `exit(enter(fa)) == fa`.
    */
  def enter[A](fa: F[A], context: String): G[A]

  /** Converts the effect `G` into the effect `F`.
    * Must be the left-inverse of [[enter]], i.e., `exit(enter(fa)) == fa`.
    */
  def exit[A](fa: G[A]): F[A]

  /** [[exit]] as an arrow */
  def exitK: G ~> F = new FunctionK[G, F] {
    override def apply[A](fa: G[A]): F[A] = exit(fa)
  }

  /** Composes this effect tunnel with another effect tunnel. */
  def andThen[H[_]](tunnel: EffectTunnel[G, H]): EffectTunnel[F, H] =
    new EffectTunnel[F, H] {
      override def enter[A](fa: F[A], context: String): H[A] =
        tunnel.enter(EffectTunnel.this.enter(fa, context), context)
      override def exit[A](fa: H[A]): F[A] = EffectTunnel.this.exit(tunnel.exit(fa))
    }
}

object EffectTunnel {

  implicit def id[F[_]]: EffectTunnel[F, F] =
    new EffectTunnel[F, F] {
      override def enter[A](fa: F[A], context: String): F[A] = fa
      override def exit[A](fa: F[A]): F[A] = fa
    }

  implicit def effectTunnelFutureUnlessShutdown(implicit
      ec: ExecutionContext
  ): EffectTunnel[FutureUnlessShutdown, Future] =
    new EffectTunnel[FutureUnlessShutdown, Future] {
      override def enter[A](fa: FutureUnlessShutdown[A], context: String): Future[A] =
        fa.failOnShutdownToAbortException(s"Entering effect tunnel for $context")

      override def exit[A](fa: Future[A]): FutureUnlessShutdown[A] =
        FutureUnlessShutdown.recoverFromAbortException(fa)
    }

  implicit def effectTunnelEitherTFuture[A](implicit
      ec: ExecutionContext
  ): EffectTunnel[EitherT[Future, A, *], Future] = {
    val tag = new Object
    new EffectTunnel[EitherT[Future, A, *], Future] {
      override def enter[B](fa: EitherT[Future, A, B], context: String): Future[B] =
        leftAsExceptionEitherT(fa, tag, context)

      override def exit[B](fa: Future[B]): EitherT[Future, A, B] =
        exceptionAsLeftEitherT(fa, tag)
    }
  }

  /** Lifts an effect tunnel into `EitherT`. */
  def effectTunnelEitherTLift[F[_], G[_], A](
      tunnel: EffectTunnel[F, G]
  ): EffectTunnel[EitherT[F, A, *], EitherT[G, A, *]] =
    new EffectTunnel[EitherT[F, A, *], EitherT[G, A, *]] {
      override def enter[B](fa: EitherT[F, A, B], context: String): EitherT[G, A, B] =
        EitherT(tunnel.enter(fa.value, context))

      override def exit[B](fa: EitherT[G, A, B]): EitherT[F, A, B] =
        EitherT(tunnel.exit(fa.value))
    }

  implicit def effectTunnelEitherTFutureUS[A](implicit ec: ExecutionContext): EffectTunnel[
    EitherT[FutureUnlessShutdown, A, *],
    Future,
  ] = effectTunnelEitherTLift(effectTunnelFutureUnlessShutdown).andThen(effectTunnelEitherTFuture)

  private final case class LeftDisguisedAsAnException[A](left: A, tag: AnyRef)(context: String)
      extends RuntimeException(s"Tunnelling a left value through $context")
      with NoStackTrace

  private def leftAsException[A, B](either: Either[A, B], tag: AnyRef, context: String): Try[B] =
    either.leftMap(LeftDisguisedAsAnException(_, tag)(context)).toTry

  private def leftAsExceptionEitherT[A, B](
      x: EitherT[Future, A, B],
      tag: AnyRef,
      context: String,
  )(implicit ec: ExecutionContext): Future[B] =
    x.value.transform(_.flatMap(leftAsException(_, tag, context)))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def exceptionAsLeft[A, B](x: Try[B], expectedTag: AnyRef): Try[Either[A, B]] =
    x.map(Right(_)).recover {
      case LeftDisguisedAsAnException(left, tag) if tag eq expectedTag =>
        // Since we've checked that the tags are the same,
        // it is safe to cast the value
        Left(left.asInstanceOf[A])
    }

  private def exceptionAsLeftEitherT[A, B](
      x: Future[B],
      expectedTag: AnyRef,
  )(implicit executionContext: ExecutionContext): EitherT[Future, A, B] =
    EitherT(x.transform(exceptionAsLeft(_, expectedTag)))

}
