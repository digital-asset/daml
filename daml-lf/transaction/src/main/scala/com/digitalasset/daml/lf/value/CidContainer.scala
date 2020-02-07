// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.VersionTimeline

import scala.language.higherKinds
import scala.util.control.NoStackTrace

sealed abstract class CidMapper[-A1, +A2, In, Out] {

  def map(f: In => Out): A1 => A2

  private[value] type UnsafeEither[L, R]

  // here we emulate a traverse like function.
  private[value] def traverse[L](
      f: (L => UnsafeEither[L, Out], Out => UnsafeEither[L, Out]) => In => UnsafeEither[L, Out]
  ): A1 => Either[L, A2]

}

private sealed abstract class CidMapperImpl[A1, A2, In, Out] extends CidMapper[A1, A2, In, Out] {

  private[value] final type UnsafeEither[L, R] = R

  // We cheat using exception
  private[value] final def traverse[L](
      f: (L => UnsafeEither[L, Out], Out => UnsafeEither[L, Out]) => In => UnsafeEither[L, Out]
  ): A1 => Either[L, A2] = {
    case class Ball(x: L) extends Throwable with NoStackTrace
    a =>
      def left(x: L): UnsafeEither[L, Out] = throw Ball(x)
      def right(a: Out): UnsafeEither[L, Out] = a
      try {
        Right(map(f(left, right))(a))
      } catch {
        case Ball(x) => Left(x)
      }
  }
}

object CidMapper {

  type CidChecker[-A1, +A2, AllowCid] = CidMapper[A1, A2, Value.ContractId, AllowCid]

  type NoCidChecker[-A1, +A2] = CidChecker[A1, A2, Nothing]

  type NoRelCidChecker[-A1, +A2] = CidChecker[A1, A2, Value.AbsoluteContractId]

  type RelCidResolver[-A1, +A2, Id] =
    CidMapper[A1, A2, Value.RelativeContractId, Id]

  type RelCidV0Resolver[-A1, +A2] =
    CidMapper[A1, A2, Value.RelativeContractId, Ref.ContractIdStringV0]

  type RelCidV1Resolver[-A1, +A2] =
    CidMapper[A1, A2, Value.RelativeContractId, Ref.ContractIdStringV1]

  def trivialMapper[X, In, Out]: CidMapper[X, X, In, Out] =
    new CidMapperImpl[X, X, In, Out] {
      override def map(f: In => Out): X => X = identity
    }

  private[value] def basicMapperInstance[Cid1, Cid2]: CidMapper[Cid1, Cid2, Cid1, Cid2] =
    new CidMapperImpl[Cid1, Cid2, Cid1, Cid2] {
      override def map(f: Cid1 => Cid2): Cid1 => Cid2 = f
    }

  private[value] def basicCidResolverInstance[Id <: Ref.ContractIdString]
    : RelCidResolver[Value.ContractId, Value.AbsoluteContractId, Id] =
    new CidMapperImpl[Value.ContractId, Value.AbsoluteContractId, Value.RelativeContractId, Id] {
      override def map(
          f: Value.RelativeContractId => Id,
      ): Value.ContractId => Value.AbsoluteContractId = {
        case acoid: Value.AbsoluteContractId => acoid
        case rcoid: Value.RelativeContractId => Value.AbsoluteContractId(f(rcoid))
      }
    }

  private[value] def valueVersionCidV1Resolver[A1, A2](
      implicit resolver: RelCidV1Resolver[A1, A2],
  ): RelCidV1Resolver[Value.VersionedValue[A1], Value.VersionedValue[A2]] =
    new CidMapperImpl[
      Value.VersionedValue[A1],
      Value.VersionedValue[A2],
      Value.RelativeContractId,
      Ref.ContractIdStringV1,
    ] {
      override def map(
          f: Value.RelativeContractId => Ref.ContractIdStringV1,
      ): Value.VersionedValue[A1] => Value.VersionedValue[A2] = {
        case Value.VersionedValue(version, value) =>
          Value.VersionedValue(
            version = VersionTimeline.maxVersion(version, ValueVersions.minContractIdV1),
            value = value.map1(resolver.map(f)),
          )
      }
    }
}

trait CidContainer[+A] {

  import CidMapper._

  protected val self: A

  final def resolveRelCidV0[B](f: Value.RelativeContractId => Ref.ContractIdStringV0)(
      implicit mapper: RelCidV0Resolver[A, B]
  ): B =
    mapper.map(f)(self)

  final def resolveRelCidV1[B](
      f: Value.RelativeContractId => Either[String, Ref.ContractIdStringV1])(
      implicit resolver: RelCidV1Resolver[A, B],
  ): Either[String, B] =
    resolver.traverse[String]((left, right) => f(_).fold(left, right))(self)

  final def ensureNoCid[B](
      implicit mapper: NoCidChecker[A, B]
  ): Either[Value.ContractId, B] =
    mapper.traverse[Value.ContractId]((left, _) => cid => left(cid))(self)

  final def assertNoCid[B](message: Value.ContractId => String)(
      implicit mapper: NoCidChecker[A, B]
  ): B =
    data.assertRight(ensureNoCid.left.map(message))

  final def ensureNoRelCid[B](
      implicit mapper: NoRelCidChecker[A, B]
  ): Either[Value.RelativeContractId, B] =
    mapper.traverse[Value.RelativeContractId]((left, right) => {
      case acoid: Value.AbsoluteContractId => right(acoid)
      case rcoid: Value.RelativeContractId => left(rcoid)
    })(self)

  final def assertNoRelCid[B](message: Value.ContractId => String)(
      implicit mapper: NoRelCidChecker[A, B]
  ): B =
    data.assertRight(ensureNoRelCid.left.map(message))

}

trait CidContainer1[F[_]] {

  import CidMapper._

  private[lf] def map1[A, B](f: A => B): F[A] => F[B]

  protected final def cidMapperInstance[A1, A2, In, Out](
      implicit mapper: CidMapper[A1, A2, In, Out]
  ): CidMapper[F[A1], F[A2], In, Out] =
    new CidMapperImpl[F[A1], F[A2], In, Out] {
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

  final implicit def cidResolverInstance[A1, A2, OutputId](
      implicit mapper1: RelCidResolver[A1, A2, OutputId],
  ): RelCidResolver[F[A1], F[A2], OutputId] =
    cidMapperInstance(mapper1)

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
    new CidMapperImpl[F[A1, B1, C1], F[A2, B2, C2], In, Out] {
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

  final implicit def cidResolverInstance[A1, B1, C1, A2, B2, C2, OutputId](
      implicit mapper1: RelCidResolver[A1, A2, OutputId],
      mapper2: RelCidResolver[B1, B2, OutputId],
      mapper3: RelCidResolver[C1, C2, OutputId],
  ): RelCidResolver[F[A1, B1, C1], F[A2, B2, C2], OutputId] =
    cidMapperInstance

}
