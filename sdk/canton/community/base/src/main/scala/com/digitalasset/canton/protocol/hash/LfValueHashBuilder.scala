// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.{HashPurpose, PrimitiveHashBuilder}
import com.digitalasset.canton.protocol.hash.LfValueHashBuilder.TypeTag
import com.digitalasset.canton.protocol.hash.LfValueHashBuilder.TypeTag.*
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value

object LfValueHashBuilder {
  private[hash] sealed abstract class TypeTag(val tag: Byte, val name: String)

  /** To avoid hash collision with values of different types which would result in an insecure hash,
    * we prefix each value with a unique tag corresponding to its type.
    *
    * e.g we want to avoid h(Some(42)) == h(4294967338)
    */
  private[hash] object TypeTag {
    case object UnitTag extends TypeTag(0, "Unit")
    case object BoolTag extends TypeTag(1, "Bool")
    case object Int64Tag extends TypeTag(2, "Int64")
    case object NumericTag extends TypeTag(3, "Numeric")
    case object TimestampTag extends TypeTag(4, "Timestamp")
    case object DateTag extends TypeTag(5, "Date")
    case object PartyTag extends TypeTag(6, "Party")
    case object TextTag extends TypeTag(7, "Text")
    case object ContractIdTag extends TypeTag(8, "ContractId")
    case object OptionalTag extends TypeTag(9, "Optional")
    case object ListTag extends TypeTag(10, "List")
    case object TextMapTag extends TypeTag(11, "TextMap")
    case object RecordTag extends TypeTag(12, "Record")
    case object VariantTag extends TypeTag(13, "Variant")
    case object EnumTag extends TypeTag(14, "Enum")
    case object GenMapTag extends TypeTag(15, "GenMap")
  }

  // For testing only
  private[hash] def valueBuilderForV1Node(
      hashTracer: HashTracer = HashTracer.NoOp
  ): LfValueBuilder =
    new NodeBuilderV1(
      HashPurpose.PreparedSubmission,
      hashTracer,
      enforceNodeSeedForCreateNodes = false,
    )
}

/** Hash Builder with additional methods to encode LF Values
  */
private[hash] class LfValueBuilder(purpose: HashPurpose, hashTracer: HashTracer)
    extends PrimitiveHashBuilder(purpose, hashTracer) {
  protected def formatByteToHexString(byte: Byte): String = String.format("%02X", byte)

  final def addDottedName(name: Ref.DottedName): this.type =
    iterateOver(name.segments)(_ add _)

  final def addQualifiedName(name: Ref.QualifiedName): this.type =
    addDottedName(name.module).addDottedName(name.name)

  final def addIdentifier(id: Ref.Identifier): this.type =
    add(id.packageId).addQualifiedName(id.qualifiedName)

  def addTypeTag(typeTag: TypeTag): this.type = addByte(typeTag.tag, s"${typeTag.name} Type Tag")

  def addTypedValue(value: Value): this.type =
    value match {
      case Value.ValueUnit =>
        addTypeTag(UnitTag)
      case Value.ValueBool(b) =>
        addTypeTag(BoolTag).addBool(b)
      case Value.ValueInt64(v) =>
        addTypeTag(Int64Tag).add(v)
      case Value.ValueNumeric(v) =>
        addTypeTag(NumericTag).addNumeric(v)
      case Value.ValueTimestamp(v) =>
        addTypeTag(TimestampTag).add(v.micros)
      case Value.ValueDate(v) =>
        addTypeTag(DateTag).add(v.days)
      case Value.ValueParty(v) =>
        addTypeTag(PartyTag).add(v)
      case Value.ValueText(v) =>
        addTypeTag(TextTag).add(v)
      case Value.ValueContractId(cid) =>
        addTypeTag(ContractIdTag).add(cid.toBytes.toByteString, s"${cid.coid} (contractId)")
      case Value.ValueOptional(opt) =>
        opt match {
          case Some(value) =>
            addTypeTag(OptionalTag).addByte(1.toByte, "Some").addTypedValue(value)
          case None =>
            addTypeTag(OptionalTag).addByte(0.toByte, "None")
        }
      case Value.ValueList(xs) =>
        addTypeTag(ListTag).iterateOver(xs.toImmArray)(_ addTypedValue _)
      case Value.ValueTextMap(xs) =>
        addTypeTag(TextMapTag).iterateOver(xs.toImmArray)((acc, x) =>
          acc.add(x._1).addTypedValue(x._2)
        )
      case Value.ValueRecord(identifier, fs) =>
        addTypeTag(RecordTag)
          .addOptional(identifier, _.addIdentifier)
          .iterateOver(fs)((builder, recordEntry) =>
            builder
              .addOptional(recordEntry._1, builder => (value: String) => builder.add(value))
              .addTypedValue(recordEntry._2)
          )
      case Value.ValueVariant(identifier, variant, v) =>
        addTypeTag(VariantTag)
          .addOptional(identifier, _.addIdentifier)
          .add(variant)
          .addTypedValue(v)
      case Value.ValueEnum(identifier, v) =>
        addTypeTag(EnumTag)
          .addOptional(identifier, _.addIdentifier)
          .add(v)
      case Value.ValueGenMap(entries) =>
        addTypeTag(GenMapTag).iterateOver(entries.iterator, entries.length)((acc, x) =>
          acc.addTypedValue(x._1).addTypedValue(x._2)
        )
    }

  def addCid(cid: Value.ContractId): this.type =
    add(cid.toBytes.toByteString, s"${cid.coid} (contractId)")
}
