// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value

import com.daml.lf.data.Bytes

import scala.language.higherKinds
import scala.util.control.NoStackTrace

sealed abstract class CidMapper[-A1, +A2, In, Out] {

  def map(f: In => Out): A1 => A2

  // We cheat using exceptions, to get a cheap implementation of traverse using the `map` function above.
  // In practice, we abort the traversal using an exception as soon as we find an input we cannot map.
  def traverse[L](f: In => Either[L, Out]): A1 => Either[L, A2] = {
    case class Ball(x: L) extends Throwable with NoStackTrace
    a =>
      try {
        Right(map(x => f(x).fold(y => throw Ball(y), identity))(a))
      } catch {
        case Ball(x) => Left(x)
      }
  }

}

object CidMapper {

  type CidChecker[-A1, +A2, AllowCid] = CidMapper[A1, A2, Value.ContractId, AllowCid]

  type NoCidChecker[-A1, +A2] = CidChecker[A1, A2, Nothing]

  type CidSuffixer[-A1, +A2] =
    CidMapper[A1, A2, Value.ContractId, Value.ContractId.V1]

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private val _trivialMapper: CidMapper[Any, Any, Nothing, Any] =
    new CidMapper[Any, Any, Nothing, Any] {
      override def map(f: Nothing => Any): Any => Any = identity
    }

  def trivialMapper[X, In, Out]: CidMapper[X, X, In, Out] =
    _trivialMapper.asInstanceOf[CidMapper[X, X, In, Out]]

  private[value] def basicMapperInstance[Cid1, Cid2]: CidMapper[Cid1, Cid2, Cid1, Cid2] =
    new CidMapper[Cid1, Cid2, Cid1, Cid2] {
      override def map(f: Cid1 => Cid2): Cid1 => Cid2 = f
    }
}

trait CidContainer[+A] {

  import CidMapper._

  protected def self: A

  final def ensureNoCid[B](
      implicit checker: NoCidChecker[A, B]
  ): Either[Value.ContractId, B] =
    checker.traverse[Value.ContractId](Left(_))(self)

  final def assertNoCid[B](message: Value.ContractId => String)(
      implicit checker: NoCidChecker[A, B]
  ): B =
    data.assertRight(ensureNoCid.left.map(message))

  // Sets the suffix of any the V1 ContractId `coid` of the container that are not already suffixed.
  // Uses `f(coid.discriminator)` as suffix.
  final def suffixCid[B](f: crypto.Hash => Bytes)(
      implicit suffixer: CidSuffixer[A, B]
  ): Either[String, B] = {
    suffixer.traverse[String] {
      case Value.ContractId.V1(discriminator, Bytes.Empty) =>
        Value.ContractId.V1.build(discriminator, f(discriminator))
      case acoid @ Value.ContractId.V1(_, _) =>
        Right(acoid)
      case acoid @ Value.ContractId.V0(_) =>
        Left(s"expect a Contract ID V1, found $acoid")
    }(self)
  }

}

trait CidContainer1[F[_]] {

  import CidMapper._

  private[lf] def map1[A, B](f: A => B): F[A] => F[B]

  private[lf] def foreach1[A](f: A => Unit): F[A] => Unit

  protected final def cidMapperInstance[A1, A2, In, Out](
      implicit mapper: CidMapper[A1, A2, In, Out]
  ): CidMapper[F[A1], F[A2], In, Out] =
    new CidMapper[F[A1], F[A2], In, Out] {
      override def map(f: In => Out): F[A1] => F[A2] =
        map1[A1, A2](mapper.map(f))
    }

  final implicit def noCidCheckerInstance[A1, A2](
      implicit checker1: NoCidChecker[A1, A2],
  ): NoCidChecker[F[A1], F[A2]] =
    cidMapperInstance[A1, A2, Value.ContractId, Nothing]

  final implicit def cidSuffixerInstance[A1, A2](
      implicit resolver1: CidSuffixer[A1, A2],
  ): CidSuffixer[F[A1], F[A2]] =
    cidMapperInstance

}

trait CidContainer2[F[_, _]] {

  import CidMapper._

  private[lf] def map2[A1, B1, C1, A2, B2, C2](
      f1: A1 => A2,
      f2: B1 => B2,
  ): F[A1, B1] => F[A2, B2]

  private[lf] def foreach2[A, B](
      f1: A => Unit,
      f2: B => Unit,
  ): F[A, B] => Unit

  protected final def cidMapperInstance[A1, B1, C1, A2, B2, C2, In, Out](
      implicit mapper1: CidMapper[A1, A2, In, Out],
      mapper2: CidMapper[B1, B2, In, Out],
  ): CidMapper[F[A1, B1], F[A2, B2], In, Out] =
    new CidMapper[F[A1, B1], F[A2, B2], In, Out] {
      override def map(f: In => Out): F[A1, B1] => F[A2, B2] = {
        map2[A1, B1, C1, A2, B2, C2](mapper1.map(f), mapper2.map(f))
      }
    }

  final implicit def cidSuffixerInstance[A1, B1, C1, A2, B2, C2](
      implicit suffixer1: CidSuffixer[A1, A2],
      suffixer2: CidSuffixer[B1, B2],
  ): CidSuffixer[F[A1, B1], F[A2, B2]] =
    cidMapperInstance

}

trait CidContainer3[F[_, _, _]] {

  import CidMapper._

  private[lf] def map3[A1, B1, C1, A2, B2, C2](
      f1: A1 => A2,
      f2: B1 => B2,
      f3: C1 => C2,
  ): F[A1, B1, C1] => F[A2, B2, C2]

  private[lf] def foreach3[A, B, C](
      f1: A => Unit,
      f2: B => Unit,
      f3: C => Unit,
  ): F[A, B, C] => Unit

  protected final def cidMapperInstance[A1, B1, C1, A2, B2, C2, In, Out](
      implicit mapper1: CidMapper[A1, A2, In, Out],
      mapper2: CidMapper[B1, B2, In, Out],
      mapper3: CidMapper[C1, C2, In, Out],
  ): CidMapper[F[A1, B1, C1], F[A2, B2, C2], In, Out] =
    new CidMapper[F[A1, B1, C1], F[A2, B2, C2], In, Out] {
      override def map(f: In => Out): F[A1, B1, C1] => F[A2, B2, C2] = {
        map3[A1, B1, C1, A2, B2, C2](mapper1.map(f), mapper2.map(f), mapper3.map(f))
      }
    }

  final implicit def cidSuffixerInstance[A1, B1, C1, A2, B2, C2](
      implicit suffixer1: CidSuffixer[A1, A2],
      suffixer2: CidSuffixer[B1, B2],
      suffixer3: CidSuffixer[C1, C2],
  ): CidSuffixer[F[A1, B1, C1], F[A2, B2, C2]] =
    cidMapperInstance

}
