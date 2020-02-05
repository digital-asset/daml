// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.VersionTimeline

import scala.language.higherKinds
import scala.util.control.NoStackTrace

sealed trait CidMapper[-A1, +A2, Fun] {

  def map(f: Fun): A1 => A2

}

object CidMapper {

  type CidChecker[-A1, +A2, AllowCid] = CidMapper[A1, A2, Value.ContractId => AllowCid]

  type NoCidChecker[-A1, +A2] = CidChecker[A1, A2, Nothing]

  type NoRelCidChecker[-A1, +A2] = CidChecker[A1, A2, Value.AbsoluteContractId]

  type RelCidResolver[-A1, +A2, Id] =
    CidMapper[A1, A2, Value.RelativeContractId => Id]

  type RelCidV0Resolver[-A1, +A2] =
    CidMapper[A1, A2, Value.RelativeContractId => Ref.ContractIdStringV0]

  type RelCidV1Resolver[-A1, +A2] =
    CidMapper[A1, A2, Value.RelativeContractId => Ref.ContractIdStringV1]

  def trivialMapper[X, Fun]: CidMapper[X, X, Fun] =
    new CidMapper[X, X, Fun] {
      override def map(f: Fun): X => X = identity
    }

  private[value] def basicMapperInstance[Cid1, Cid2]: CidMapper[Cid1, Cid2, Cid1 => Cid2] =
    new CidMapper[Cid1, Cid2, Cid1 => Cid2] {
      override def map(f: Cid1 => Cid2): Cid1 => Cid2 = f
    }

  private[value] def basicCidResolverInstance[Id <: Ref.ContractIdString]
    : RelCidResolver[Value.ContractId, Value.AbsoluteContractId, Id] =
    new RelCidResolver[Value.ContractId, Value.AbsoluteContractId, Id] {
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
    new RelCidV1Resolver[Value.VersionedValue[A1], Value.VersionedValue[A2]] {
      override def map(
          f: Value.RelativeContractId => Ref.ContractIdStringV1,
      ): Value.VersionedValue[A1] => Value.VersionedValue[A2] = {
        case Value.VersionedValue(version, value) =>
          Value.VersionedValue(
            version = VersionTimeline.maxVersion(version, ValueVersions.minContractIdV1),
            value = value.resolveRelCidV1(f),
          )
      }
    }

}

trait CidContainer[+A] {

  import CidMapper._

  protected val self: A

  final def resolveRelCidV0[B](f: Value.RelativeContractId => Ref.ContractIdStringV0)(
      implicit mapper: RelCidResolver[A, B, Ref.ContractIdStringV0],
  ): B =
    mapper.map(f)(self)

  final def resolveRelCidV1[B](f: Value.RelativeContractId => Ref.ContractIdStringV1)(
      implicit mapper: RelCidResolver[A, B, Ref.ContractIdStringV1],
  ): B =
    mapper.map(f)(self)

  final def ensureNoCid[B](
      implicit mapper: NoCidChecker[A, B]
  ): Either[Value.ContractId, B] = {
    case class Ball(x: Value.ContractId) extends Throwable with NoStackTrace
    try {
      Right(mapper.map(coid => throw Ball(coid))(self))
    } catch {
      case Ball(coid) => Left(coid)
    }
  }

  final def assertNoCid[B](message: Value.ContractId => String)(
      implicit mapper: NoCidChecker[A, B]
  ): B =
    data.assertRight(ensureNoCid.left.map(message))

  final def ensureNoRelCid[B](
      implicit mapper: NoRelCidChecker[A, B]
  ): Either[Value.RelativeContractId, B] = {
    case class Ball(x: Value.RelativeContractId) extends Throwable with NoStackTrace
    try {
      Right(mapper.map({
        case acoid: Value.AbsoluteContractId => acoid
        case rcoid: Value.RelativeContractId => throw Ball(rcoid)
      })(self))
    } catch {
      case Ball(coid) => Left(coid)
    }
  }

  final def assertNoRelCid[B](message: Value.ContractId => String)(
      implicit mapper: NoRelCidChecker[A, B]
  ): B =
    data.assertRight(ensureNoRelCid.left.map(message))

}

trait CidContainer1[F[_]] {

  import CidMapper._

  private[lf] def map1[A, B](f: A => B): F[A] => F[B]

  protected final def cidMapperInstance[A1, A2, Fun](
      implicit mapper: CidMapper[A1, A2, Fun]
  ): CidMapper[F[A1], F[A2], Fun] =
    new CidMapper[F[A1], F[A2], Fun] {
      override def map(f: Fun): F[A1] => F[A2] =
        map1[A1, A2](mapper.map(f))
    }

  final implicit def noCidCheckerInstance[A1, A2](
      implicit checker1: NoCidChecker[A1, A2],
  ): NoCidChecker[F[A1], F[A2]] =
    cidMapperInstance

  final implicit def noRelCidCheckerInstance[A1, A2](
      implicit checker1: NoRelCidChecker[A1, A2],
  ): NoRelCidChecker[F[A1], F[A2]] =
    cidMapperInstance

}

trait CidContainer1WithDefaultCidResolver[F[_]] extends CidContainer1[F] {

  import CidMapper._

  final implicit def cidResolverInstance[A1, A2, OutputId](
      implicit mapper1: RelCidResolver[A1, A2, OutputId],
  ): RelCidResolver[F[A1], F[A2], OutputId] =
    cidMapperInstance

}

trait CidContainer3[F[_, _, _]] {

  import CidMapper._

  private[lf] def map3[A1, B1, C1, A2, B2, C2](
      f1: A1 => A2,
      f2: B1 => B2,
      f3: C1 => C2,
  ): F[A1, B1, C1] => F[A2, B2, C2]

  protected final def cidMapperInstance[A1, B1, C1, A2, B2, C2, Fun](
      implicit mapper1: CidMapper[A1, A2, Fun],
      mapper2: CidMapper[B1, B2, Fun],
      mapper3: CidMapper[C1, C2, Fun],
  ): CidMapper[F[A1, B1, C1], F[A2, B2, C2], Fun] =
    new CidMapper[F[A1, B1, C1], F[A2, B2, C2], Fun] {
      override def map(f: Fun): F[A1, B1, C1] => F[A2, B2, C2] = {
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
