// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.nameof.NameOf

object Util {

  import value.Value
  import value.Value._

  // Equivalent to serialization + unserialization.
  // Fails if :
  // - `value0` contains GenMap and version < 1.11
  def normalizeValue(
      value0: Value,
      version: TransactionVersion,
  ): Either[String, Value] =
    try {
      Right(assertNormalizeValue(value0, version))
    } catch {
      case e: IllegalArgumentException => Left(e.getMessage)
    }

  // unsafe version of `normalize`
  @throws[IllegalArgumentException]
  def assertNormalizeValue(
      value0: Value,
      version: TransactionVersion,
  ): Value = {

    import Ordering.Implicits.infixOrderingOps

    val allowGenMap = version >= TransactionVersion.minGenMap
    val eraseType = version >= TransactionVersion.minTypeErasure

    def handleTypeInfo[X](x: Option[X]) =
      if (eraseType) {
        None
      } else {
        x
      }

    def go(value: Value): Value =
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
        case _: ValueCidlessLeaf | _: ValueContractId => value
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
            InternalError.illegalArgumentException(
              NameOf.qualifiedNameOfCurrentFunc,
              s"GenMap are not allowed in transaction version $version",
            )
          }
        case _ : ValueAny =>
          value
      }

    go(value0)

  }

  def normalizeVersionedValue(
      value: VersionedValue
  ): Either[String, VersionedValue] =
    normalizeValue(value.unversioned, value.version).map(normalized => value.map(_ => normalized))

  def normalizeContract(
      contract: VersionedContractInstance
  ): Either[String, VersionedContractInstance] =
    normalizeValue(contract.unversioned.arg, contract.version)
      .map(normalized => contract.map(_.copy(arg = normalized)))

  def normalizeKey(
      key: Node.KeyWithMaintainers,
      version: TransactionVersion,
  ): Either[String, Node.KeyWithMaintainers] =
    normalizeValue(key.key, version).map(normalized => key.copy(key = normalized))

  def normalizeOptKey(
      key: Option[Node.KeyWithMaintainers],
      version: TransactionVersion,
  ): Either[String, Option[Node.KeyWithMaintainers]] =
    key match {
      case Some(value) => normalizeKey(value, version).map(Some(_))
      case None => Right(None)
    }

}
