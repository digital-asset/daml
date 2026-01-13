// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{HashBuilderFromMessageDigest, HashPurpose}
import com.digitalasset.canton.protocol.hash.LfValueHashBuilder.TypeTag
import com.digitalasset.canton.protocol.hash.LfValueHashBuilder.TypeTag.*
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value

object LfValueHashBuilder {
  private[hash] sealed abstract class TypeTag(val tag: Byte, val name: String)

  /** To avoid hash collision with values of different types which would result in an insecure hash,
    * we prefix each value with a unique tag corresponding to its type.
    *
    * e.g. we want to avoid h(Some(42)) == h(4294967338)
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
  ): LfValueHashBuilder =
    new NodeBuilderV1(
      HashPurpose.PreparedSubmission,
      hashTracer,
      enforceNodeSeedForCreateNodes = false,
    )
}

/** Hash Builder with additional methods to encode LF Values.
  */
private[hash] class LfValueHashBuilder(purpose: HashPurpose, hashTracer: HashTracer)
    extends HashBuilderFromMessageDigest(Sha256, purpose, hashTracer) {
  protected def formatByteToHexString(byte: Byte): String = String.format("%02X", byte)

  private def addDottedName(name: Ref.DottedName): this.type =
    addArray(name.segments)(_ addString _)

  private def addQualifiedName(name: Ref.QualifiedName): this.type =
    addDottedName(name.module).addDottedName(name.name)

  final def addIdentifier(id: Ref.Identifier): this.type =
    addString(id.packageId).addQualifiedName(id.qualifiedName)

  private def addTypeTag(typeTag: TypeTag): this.type =
    addByte(typeTag.tag, _ => s"${typeTag.name} Type Tag")

  def addTypedValue(value: Value): this.type =
    value match {
      case Value.ValueUnit =>
        addTypeTag(UnitTag)
      case Value.ValueBool(b) =>
        addTypeTag(BoolTag).addBool(b)
      case Value.ValueInt64(v) =>
        addTypeTag(Int64Tag).addLong(v)
      case Value.ValueNumeric(v) =>
        addTypeTag(NumericTag).addNumeric(v)
      case Value.ValueTimestamp(v) =>
        addTypeTag(TimestampTag).addLong(v.micros)
      case Value.ValueDate(v) =>
        addTypeTag(DateTag).addInt(v.days)
      case Value.ValueParty(v) =>
        addTypeTag(PartyTag).addString(v)
      case Value.ValueText(v) =>
        addTypeTag(TextTag).addString(v)
      case Value.ValueContractId(cid) =>
        addTypeTag(ContractIdTag).addByteString(
          cid.toBytes.toByteString,
          s"${cid.coid} (contractId)",
        )
      case Value.ValueOptional(opt) =>
        opt match {
          case Some(value) =>
            addTypeTag(OptionalTag).addByte(1.toByte, _ => "Some").addTypedValue(value)
          case None =>
            addTypeTag(OptionalTag).addByte(0.toByte, _ => "None")
        }
      case Value.ValueList(xs) =>
        addTypeTag(ListTag).addArray(xs.toImmArray)(_ addTypedValue _)
      case Value.ValueTextMap(xs) =>
        addTypeTag(TextMapTag).addArray(xs.toImmArray)((acc, x) =>
          acc.addString(x._1).addTypedValue(x._2)
        )
      case Value.ValueRecord(identifier, fs) =>
        addTypeTag(RecordTag)
          .addOptional(identifier, _.addIdentifier)
          .addArray(fs)((builder, recordEntry) =>
            builder
              .addOptional(recordEntry._1, builder => (value: String) => builder.addString(value))
              .addTypedValue(recordEntry._2)
          )
      case Value.ValueVariant(identifier, variant, v) =>
        addTypeTag(VariantTag)
          .addOptional(identifier, _.addIdentifier)
          .addString(variant)
          .addTypedValue(v)
      case Value.ValueEnum(identifier, v) =>
        addTypeTag(EnumTag)
          .addOptional(identifier, _.addIdentifier)
          .addString(v)
      case Value.ValueGenMap(entries) =>
        addTypeTag(GenMapTag).addIterator(entries.iterator, entries.length)((acc, x) =>
          acc.addTypedValue(x._1).addTypedValue(x._2)
        )
    }

  def addCid(cid: Value.ContractId): this.type =
    addByteString(cid.toBytes.toByteString, s"${cid.coid} (contractId)")
}
