// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value

import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.Value._
import scala.Ordering.Implicits.infixOrderingOps

object Util {

  // Equivalent to serialization + unserialization.
  // Fails if :
  // - value0 contain GenMap and version < 1.11
  def normalize(
      value0: Value[ContractId],
      version: TransactionVersion,
  ): Either[String, Value[ContractId]] =
    try {
      Right(assertNormalize(value0, version))
    } catch {
      case e: IllegalArgumentException => Left(e.getMessage)
    }

  // unsafe version of `normalize`
  @throws[IllegalArgumentException]
  def assertNormalize(value0: Value[ContractId], version: TransactionVersion): Value[ContractId] = {

    val allowGenMap = version >= TransactionVersion.minGenMap
    val eraseType = version >= TransactionVersion.minTypeErasure

    def handleTypeInfo[X](x: Option[X]) =
      if (eraseType) {
        None
      } else if (x.isEmpty) {
        throw new IllegalArgumentException(
          s"Type information is require for transaction version $version"
        )
      } else {
        x
      }

    def go(value: Value[ContractId]): Value[ContractId] =
      value match {
        case ValueEnum(tyCon, cons) =>
          ValueEnum(handleTypeInfo(tyCon), cons)
        case ValueRecord(tyCon, fields) =>
          ValueRecord(
            handleTypeInfo(tyCon),
            fields.map { case (fieldName, value) => handleTypeInfo(fieldName) -> go(value) },
          )
        case ValueVariant(tyCon, variant, value) =>
          ValueVariant(handleTypeInfo(tyCon), variant, go(value))
        case _: ValueCidlessLeaf | _: ValueContractId[_] => value
        case ValueList(values) =>
          ValueList(values.map(go))
        case ValueOptional(value) =>
          ValueOptional(value.map(go))
        case ValueTextMap(value) =>
          ValueTextMap(value.mapValue(go))
        case ValueGenMap(entries) =>
          if (allowGenMap) {
            ValueGenMap(entries.map { case (k, v) => go(k) -> go(v) })
          } else {
            throw new IllegalArgumentException(
              s"GenMap are not allowed in transaction version $version"
            )
          }
      }

    go(value0)

  }

}
