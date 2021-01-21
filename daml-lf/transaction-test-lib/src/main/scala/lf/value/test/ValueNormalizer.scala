// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.value.test

import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.Value
import com.daml.lf.value.Value._

object ValueNormalizer {

  // equivalent to serialization + unserialization.
  def normalize(value0: Value[ContractId], version: TransactionVersion): Value[ContractId] = {

    import scala.Ordering.Implicits._

    val withRank = TransactionVersion.minRank <= version

    def go(value: Value[ContractId]): Value[ContractId] =
      value match {
        case ValueEnum(id, cons, rank) =>
          ValueEnum(id, cons, rank.filter(_ => withRank))
        case ValueRecord(id, fields) =>
          ValueRecord(
            id,
            fields.map { case (field, value) => field -> go(value) },
          )
        case ValueVariant(id, variant, rank, value) =>
          ValueVariant(id, variant, rank.filter(_ => withRank), go(value))
        case _: ValueCidlessLeaf | _: ValueContractId[_] => value
        case ValueList(values) =>
          ValueList(values.map(go))
        case ValueOptional(value) =>
          ValueOptional(value.map(go))
        case ValueTextMap(value) =>
          ValueTextMap(value.mapValue(go))
        case ValueGenMap(entries) =>
          ValueGenMap(entries.map { case (k, v) => go(k) -> go(v) })
      }

    go(value0)

  }

}
