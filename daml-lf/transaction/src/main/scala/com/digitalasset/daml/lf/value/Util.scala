// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value

import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.Value._

object Util {

  // equivalent to serialization + unserialization.
  def normalize(
      value0: Value[ContractId],
      version: TransactionVersion,
  ): Either[String, Value[ContractId]] =
    try {
      Right(assertNormalize(value0, version))
    } catch {
      case e: IllegalArgumentException => Left(e.getMessage)
    }

  @throws[IllegalArgumentException]
  def assertNormalize(value0: Value[ContractId], version: TransactionVersion): Value[ContractId] = {

    val disallowGenMap = version < TransactionVersion.minGenMap

    import scala.Ordering.Implicits._

    def stripTypes(value: Value[ContractId]): Value[ContractId] =
      value match {
        case ValueEnum(_, cons) =>
          ValueEnum(None, cons)
        case ValueRecord(_, fields) =>
          ValueRecord(None, fields.map { case (_, value) => None -> stripTypes(value) })
        case ValueVariant(_, variant, value) =>
          ValueVariant(None, variant, stripTypes(value))
        case _: ValueCidlessLeaf | _: ValueContractId[_] => value
        case ValueList(values) =>
          ValueList(values.map(stripTypes))
        case ValueOptional(value) =>
          ValueOptional(value.map(stripTypes))
        case ValueTextMap(value) =>
          ValueTextMap(value.mapValue(stripTypes))
        case ValueGenMap(entries) =>
          if (allowGenMap)
            throw new IllegalArgumentException(s"GenMap are not allowed in LF $version")
          ValueGenMap(entries.map { case (k, v) => stripTypes(k) -> stripTypes(v) })
      }

    if (version >= TransactionVersion.minTypeErasure)
      stripTypes(value0)
    else
      value0

  }

}
