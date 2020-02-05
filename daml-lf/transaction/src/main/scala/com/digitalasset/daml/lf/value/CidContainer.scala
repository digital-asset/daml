// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import com.digitalasset.daml.lf.data.Ref

import scala.language.higherKinds
import scala.util.control.NoStackTrace

sealed trait CidMapper[-A1, +A2, Fun] {

  def map(f: Fun): A1 => A2

}

object CidMapper {

  def trivialMapper[X, Fun]: CidMapper[X, X, Fun] =
    new CidMapper[X, X, Fun] {
      override def map(f: Fun): X => X = identity
    }

  private[value] def basicInstance[Cid1, Cid2]: CidMapper[Cid1, Cid2, Cid1 => Cid2] =
    new CidMapper[Cid1, Cid2, Cid1 => Cid2] {
      override def map(f: Cid1 => Cid2): Cid1 => Cid2 = f
    }

  type RelCidResolverMapper[-A1, +A2] =
    CidMapper[A1, A2, Value.ContractId => Value.AbsoluteContractId]

  type NoCidMapper[-A1, +A2] = CidMapper[A1, A2, Value.ContractId => Nothing]

  type NoRelCidMapper[-A1, +A2] = CidMapper[A1, A2, Value.ContractId => Value.AbsoluteContractId]

}

trait CidContainer[+A] {

  import CidMapper._

  protected val self: A

  def resolveRelCid[B](f: Value.RelativeContractId => Ref.ContractIdString)(
      implicit mapper: RelCidResolverMapper[A, B],
  ): B =
    mapper.map({
      case acoid: Value.AbsoluteContractId => acoid
      case rcoid: Value.RelativeContractId => Value.AbsoluteContractId(f(rcoid))
    })(self)

  def ensureNoCid[B](
      implicit mapper: NoCidMapper[A, B]
  ): Either[Value.ContractId, B] = {
    case class Ball(x: Value.ContractId) extends Throwable with NoStackTrace
    try {
      Right(mapper.map(coid => throw Ball(coid))(self))
    } catch {
      case Ball(coid) => Left(coid)
    }
  }

  def assertNoCid[B](message: Value.ContractId => String)(
      implicit mapper: NoCidMapper[A, B]
  ): B =
    data.assertRight(ensureNoCid.left.map(message))

  def ensureNoRelCid[B](
      implicit mapper: NoRelCidMapper[A, B]
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

  def assertNoRelCid[B](message: Value.ContractId => String)(
      implicit mapper: NoRelCidMapper[A, B]
  ): B =
    data.assertRight(ensureNoRelCid.left.map(message))

}

trait CidContainer1[F[_]] {

  private[lf] def map1[A, B](f: A => B): F[A] => F[B]

  final implicit def cidMapperInstance[A1, A2, Fun](
      implicit mapper: CidMapper[A1, A2, Fun]
  ): CidMapper[F[A1], F[A2], Fun] =
    new CidMapper[F[A1], F[A2], Fun] {
      override def map(f: Fun): F[A1] => F[A2] =
        map1[A1, A2](mapper.map(f))
    }

}

trait CidContainer3[F[_, _, _]] {

  private[lf] def map3[A1, B1, C1, A2, B2, C2](
      f1: A1 => A2,
      f2: B1 => B2,
      f3: C1 => C2,
  ): F[A1, B1, C1] => F[A2, B2, C2]

  final implicit def cidMapperInstance[A1, B1, C1, A2, B2, C2, Fun](
      implicit mapper1: CidMapper[A1, A2, Fun],
      mapper2: CidMapper[B1, B2, Fun],
      mapper3: CidMapper[C1, C2, Fun],
  ): CidMapper[F[A1, B1, C1], F[A2, B2, C2], Fun] =
    new CidMapper[F[A1, B1, C1], F[A2, B2, C2], Fun] {
      override def map(f: Fun): F[A1, B1, C1] => F[A2, B2, C2] = {
        map3[A1, B1, C1, A2, B2, C2](mapper1.map(f), mapper2.map(f), mapper3.map(f))
      }
    }

}
