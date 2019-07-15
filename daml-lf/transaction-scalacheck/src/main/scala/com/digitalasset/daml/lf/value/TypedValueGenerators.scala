// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import scala.language.higherKinds

import data.ImmArray.ImmArraySeq
import iface.{PrimType => PT, Type, TypePrim}

object TypedValueGenerators {
  sealed abstract class ValueAddend {
    type Inj[Cid]
    def t: Type
    def inj[Cid]: Inj[Cid] => Value[Cid]
    def prj[Cid]: Value[Cid] PartialFunction Inj[Cid]
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  object ValueAddend extends PrimInstances[Lambda[a => ValueAddend { type Inj[_] = a }]] {
    type Aux[Inj0[_]] = ValueAddend {
      type Inj[Cid] = Inj0[Cid]
    }
    type NoCid[Inj0] = ValueAddend {
      type Inj[_] = Inj0
    }

    def noCid[Inj0](pt: PT, inj0: Inj0 => Value[Nothing])(
        prj0: Value[Any] PartialFunction Inj0): NoCid[Inj0] = new ValueAddend {
      type Inj[Cid] = Inj0
      override val t = TypePrim(pt, ImmArraySeq.empty)
      override def inj[Cid] = inj0
      override def prj[Cid] = prj0
    }

    import Value._
    val text = noCid(PT.Text, ValueText) { case ValueText(t) => t }
    val int64 = noCid(PT.Int64, ValueInt64) { case ValueInt64(i) => i }
    val decimal = noCid(PT.Decimal, ValueDecimal) { case ValueDecimal(d) => d }
    val unit = noCid(PT.Unit, (_: Unit) => ValueUnit) { case ValueUnit => () }
    val date = noCid(PT.Date, ValueDate) { case ValueDate(d) => d }
    val timestamp = noCid(PT.Timestamp, ValueTimestamp) { case ValueTimestamp(t) => t }
    val bool = noCid(PT.Bool, ValueBool) { case ValueBool(b) => b }
    val party = noCid(PT.Party, ValueParty) { case ValueParty(p) => p }
  }

  trait PrimInstances[F[_]] {
    def text: F[String]
  }
}
