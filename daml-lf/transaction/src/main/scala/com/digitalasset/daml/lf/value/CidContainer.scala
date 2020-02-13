// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import com.digitalasset.daml.lf.data.Ref

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

  type NoRelCidChecker[-A1, +A2] = CidChecker[A1, A2, Value.AbsoluteContractId]

  type RelCidResolver[-A1, +A2] =
    CidMapper[A1, A2, Value.RelativeContractId, Ref.ContractIdString]

  def trivialMapper[X, In, Out]: CidMapper[X, X, In, Out] =
    new CidMapper[X, X, In, Out] {
      override def map(f: In => Out): X => X = identity
    }

  private[value] def basicMapperInstance[Cid1, Cid2]: CidMapper[Cid1, Cid2, Cid1, Cid2] =
    new CidMapper[Cid1, Cid2, Cid1, Cid2] {
      override def map(f: Cid1 => Cid2): Cid1 => Cid2 = f
    }

  private[value] val basicCidResolverInstance
    : RelCidResolver[Value.ContractId, Value.AbsoluteContractId] =
    new CidMapper[
      Value.ContractId,
      Value.AbsoluteContractId,
      Value.RelativeContractId,
      Ref.ContractIdString] {
      override def map(
          f: Value.RelativeContractId => Ref.ContractIdString,
      ): Value.ContractId => Value.AbsoluteContractId = {
        case acoid: Value.AbsoluteContractId => acoid
        case rcoid: Value.RelativeContractId => Value.AbsoluteContractId(f(rcoid))
      }
    }
}

trait CidContainer[+A] {

  import CidMapper._

  protected val self: A

  final def resolveRelCid[B](f: Value.RelativeContractId => Ref.ContractIdString)(
      implicit resolver: RelCidResolver[A, B]
  ): B =
    resolver.map(f)(self)

  final def ensureNoCid[B](
      implicit checker: NoCidChecker[A, B]
  ): Either[Value.ContractId, B] =
    checker.traverse[Value.ContractId](Left(_))(self)

  final def assertNoCid[B](message: Value.ContractId => String)(
      implicit checker: NoCidChecker[A, B]
  ): B =
    data.assertRight(ensureNoCid.left.map(message))

  final def ensureNoRelCid[B](
      implicit checker: NoRelCidChecker[A, B]
  ): Either[Value.RelativeContractId, B] =
    checker.traverse[Value.RelativeContractId] {
      case acoid: Value.AbsoluteContractId => Right(acoid)
      case rcoid: Value.RelativeContractId => Left(rcoid)
    }(self)

  final def assertNoRelCid[B](message: Value.ContractId => String)(
      implicit checker: NoRelCidChecker[A, B]
  ): B =
    data.assertRight(ensureNoRelCid.left.map(message))

}

trait CidContainer1[F[_]] {

  import CidMapper._

  private[lf] def map1[A, B](f: A => B): F[A] => F[B]

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

  final implicit def noRelCidCheckerInstance[A1, A2](
      implicit checker1: NoRelCidChecker[A1, A2],
  ): NoRelCidChecker[F[A1], F[A2]] =
    cidMapperInstance[A1, A2, Value.ContractId, Value.AbsoluteContractId]

}

trait CidContainer1WithDefaultCidResolver[F[_]] extends CidContainer1[F] {

  import CidMapper._

  final implicit def cidResolverInstance[A1, A2](
      implicit resolver1: RelCidResolver[A1, A2],
  ): RelCidResolver[F[A1], F[A2]] =
    cidMapperInstance(resolver1)

}

trait CidContainer3[F[_, _, _]] {

  import CidMapper._

  private[lf] def map3[A1, B1, C1, A2, B2, C2](
      f1: A1 => A2,
      f2: B1 => B2,
      f3: C1 => C2,
  ): F[A1, B1, C1] => F[A2, B2, C2]

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

  final implicit def noRelCidCheckerInstance[A1, B1, C1, A2, B2, C2](
      implicit checker1: NoRelCidChecker[A1, A2],
      checker2: NoRelCidChecker[B1, B2],
      checker3: NoRelCidChecker[C1, C2],
  ): NoRelCidChecker[F[A1, B1, C1], F[A2, B2, C2]] =
    cidMapperInstance

}

trait CidContainer3WithDefaultCidResolver[F[_, _, _]] extends CidContainer3[F] {

  import CidMapper._

  final implicit def cidResolverInstance[A1, B1, C1, A2, B2, C2](
      implicit resolver1: RelCidResolver[A1, A2],
      resolver2: RelCidResolver[B1, B2],
      resolver3: RelCidResolver[C1, C2],
  ): RelCidResolver[F[A1, B1, C1], F[A2, B2, C2]] =
    cidMapperInstance

}
