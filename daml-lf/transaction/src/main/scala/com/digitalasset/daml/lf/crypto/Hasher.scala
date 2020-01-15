// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value

object Hasher {

  // tags are used to avoid hash collisions due to equal encoding for different objects

  // tags for atomic data
  private val tagUnit: Byte = 0x01
  private val tagTrue: Byte = 0x02
  private val tagFalse: Byte = 0x03
  private val tagInt64: Byte = 0x04
  private val tagNumeric: Byte = 0x05
  private val tagDate: Byte = 0x06
  private val tagTimeStamp: Byte = 0x07
  private val tagText: Byte = 0x08
  private val tagParty: Byte = 0x09
  private val tagContractId: Byte = 0x0A

  // tag for collection data
  private val tagNone: Byte = 0x20
  private val tagSome: Byte = 0x21
  private val tagList: Byte = 0x22
  private val tagTextMap: Byte = 0x23
  // private val tagGenMap: Byte = 0x24

  // tag for user defined data
  private val tagRecord: Byte = 0x40
  private val tagVariant: Byte = 0x41
  private val tagEnum: Byte = 0x42

  // package private for testing purpose.
  // Do not call this method from outside Hasher object/
  private[crypto] implicit class SHA256HashBuilderOps[X](val builder: SHA256Hash.Builder[X])
      extends AnyVal {

    import builder.{This => ThisBuilder, _}

    def addDottedName(name: Ref.DottedName): ThisBuilder =
      iterateOver(name.segments)(_ add _)

    def addQualifiedName(name: Ref.QualifiedName): ThisBuilder =
      addDottedName(name.module).addDottedName(name.name)

    def addIdentifier(id: Ref.Identifier): ThisBuilder =
      add(id.packageId).addQualifiedName(id.qualifiedName)

    // Should be used together with an other data representing uniquely the type `value`.
    // See for instance hash : Node.GlobalKey => SHA256Hash
    def addTypedValue(value: Value[Value.AbsoluteContractId]): ThisBuilder =
      value match {
        case Value.ValueUnit =>
          add(tagUnit)
        case Value.ValueBool(true) =>
          add(tagTrue)
        case Value.ValueBool(false) =>
          add(tagFalse)
        case Value.ValueInt64(v) =>
          add(tagInt64).add(v)
        case Value.ValueNumeric(v) =>
          add(tagNumeric).add(v.scale).add(v.unscaledValue.toByteArray)
        case Value.ValueTimestamp(v) =>
          add(tagTimeStamp).add(v.micros)
        case Value.ValueDate(v) =>
          add(tagDate).add(v.days)
        case Value.ValueParty(v) =>
          add(tagParty).add(v)
        case Value.ValueText(v) =>
          add(tagText).add(v)
        case Value.ValueContractId(v) =>
          add(tagContractId).add(v.coid)
        case Value.ValueOptional(None) =>
          add(tagNone)
        case Value.ValueOptional(Some(v)) =>
          add(tagSome).addTypedValue(v)
        case Value.ValueList(xs) =>
          add(tagList).iterateOver(xs.toImmArray)(_ addTypedValue _)
        case Value.ValueTextMap(xs) =>
          add(tagTextMap).iterateOver(xs.toImmArray) {
            case (acc, (k, v)) => acc.add(k).addTypedValue(v)
          }
        case Value.ValueRecord(_, fs) =>
          add(tagRecord).iterateOver(fs)(_ addTypedValue _._2)
        case Value.ValueVariant(_, variant, v) =>
          add(tagVariant).add(variant).addTypedValue(v)
        case Value.ValueEnum(_, v) =>
          add(tagEnum).add(v)
        case Value.ValueGenMap(_) =>
          sys.error("Hashing of generic map not implemented")
        // Struct: should never be encountered
        case Value.ValueStruct(_) =>
          sys.error("Hashing of struct values is not supported")
      }
  }

  // Assumes that key is well typed, i.e. :
  // 1 - `key.identifier` is the identifier for a template with a key of type τ
  // 2 - `key.key` is a value of type τ
  def hash(key: Node.GlobalKey): SHA256Hash =
    SHA256Hash
      .builder(HashPurpose.ContractKey)
      .addIdentifier(key.templateId)
      .addTypedValue(key.key.value)
      .build

}
