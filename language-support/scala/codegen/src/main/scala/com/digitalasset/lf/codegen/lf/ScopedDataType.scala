// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen
package lf

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.iface.DataType

import scalaz.{Apply, Comonad, Traverse1}
import scalaz.syntax.functor._

final case class ScopedDataType[+DT](
    name: ScopedDataType.Name,
    typeVars: ImmArraySeq[Ref.Name],
    dataType: DT,
)

object ScopedDataType {
  type Name = Ref.Identifier
  type FWT = ScopedDataType[DataType.FWT]
  type DT[+RF, +VF] = ScopedDataType[DataType[RF, VF]]

  implicit val `SDT covariant`: Traverse1[ScopedDataType] with Comonad[ScopedDataType] =
    new Traverse1[ScopedDataType] with Comonad[ScopedDataType] {
      override def foldMapRight1[A, Z](fa: ScopedDataType[A])(z: A => Z)(f: (A, => Z) => Z): Z =
        z(fa.dataType)

      override def traverse1Impl[G[_]: Apply, A, B](fab: ScopedDataType[A])(
          f: A => G[B]
      ): G[ScopedDataType[B]] =
        f(fab.dataType) map (b => fab copy (dataType = b))

      override def copoint[A](p: ScopedDataType[A]): A = p.dataType

      override def cobind[A, B](fa: ScopedDataType[A])(
          f: ScopedDataType[A] => B
      ): ScopedDataType[B] =
        fa copy (dataType = f(fa))
    }
}
