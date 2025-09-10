// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import com.daml.crypto.MessageDigestPrototype
import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.crypto.Hash.{Builder, Purpose, Version, underlyingHashLength}
import com.digitalasset.daml.lf.crypto.HashUtils.{HashTracer, formatByteToHexString}
import com.digitalasset.daml.lf.data.Ref.{Identifier, Name}
import com.digitalasset.daml.lf.data.{Bytes, FrontStack, ImmArray}
import com.digitalasset.daml.lf.language.Ast.VariantConName
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SValue.{SOptional, SText}
import com.digitalasset.daml.lf.value.Value

import java.nio.ByteBuffer
import scala.collection.immutable.{ArraySeq, TreeMap}

object SValueHash {

  private[crypto] final class SValueHashBuilder(
      version: Version,
      purpose: Purpose,
      cid2Bytes: Value.ContractId => Bytes,
      numericToBytes: data.Numeric => Bytes,
      hashTracer: HashTracer,
  ) extends Builder(numericToBytes) {

    private val INT64_TAG: Byte = 0
    private val NUMERIC_TAG: Byte = 1
    private val TEXT_TAG: Byte = 2
    private val TIMESTAMP_TAG: Byte = 3
    private val PARTY_TAG: Byte = 4
    private val BOOL_TAG: Byte = 5
    private val UNIT_TAG: Byte = 6
    private val DATE_TAG: Byte = 7
    private val RECORD_TAG: Byte = 8
    private val VARIANT_TAG: Byte = 9
    private val ENUM_TAG: Byte = 10
    private val LIST_TAG: Byte = 11
    private val OPTIONAL_TAG: Byte = 12
    private val TEXT_MAP_TAG: Byte = 13
    private val GEN_MAP_TAG: Byte = 14
    private val CONTRACT_ID_TAG: Byte = 15

    protected val md = MessageDigestPrototype.Sha256.newDigest

    override protected def update(a: ByteBuffer, context: => String): Unit = {
      hashTracer.trace(a, context)
      md.update(a)
    }

    override protected def update(a: Array[Byte], context: => String): Unit = {
      hashTracer.trace(a, context)
      md.update(a)
    }

    override protected def doFinal(buf: Array[Byte]): Unit =
      assert(md.digest(buf, 0, underlyingHashLength) == underlyingHashLength)

    def addVersion: this.type =
      addByte(version.id, s"${formatByteToHexString(version.id)} (Value Encoding Version)")
        .addByte(purpose.id, s"${formatByteToHexString(purpose.id)} (Value Encoding Purpose)")

    def addSValue(svalue: SValue): this.type =
      svalue match {
        case SValue.SUnit =>
          // We could use value.productPrefix to enrich the context here and for all values
          addByte(UNIT_TAG, "UNIT_TAG")
        case SValue.SBool(b) =>
          addByte(BOOL_TAG, "BOOL_TAG")
            .addBool(b)
        case SValue.SInt64(v) =>
          addByte(INT64_TAG, "INT64_TAG")
            .addLong(v)
        case SValue.SDate(v) =>
          addByte(DATE_TAG, "DATE_TAG")
            .addInt(v.days)
        case SValue.STimestamp(v) =>
          addByte(TIMESTAMP_TAG, "TIMESTAMP_TAG")
            .addLong(v.micros)
        case SValue.SNumeric(v) =>
          addByte(NUMERIC_TAG, "NUMERIC_TAG")
            .addNumeric(v)
        case SValue.SParty(v) =>
          addByte(PARTY_TAG, "PARTY_TAG")
            .addString(v)
        case SValue.SText(v) =>
          addByte(TEXT_TAG, "TEXT_TAG")
            .addString(v)
        case SValue.SContractId(cid) =>
          addByte(CONTRACT_ID_TAG, "CONTRACT_ID_TAG")
            .addCid(cid)
        case SValue.SOptional(opt) =>
          addByte(OPTIONAL_TAG, "OPTIONAL_TAG")
            .addOptional(opt)
        case SValue.SList(xs) =>
          addByte(LIST_TAG, "LIST_TAG")
            .addList(xs)
        case SValue.SRecord(id, fs, v) =>
          addByte(RECORD_TAG, "RECORD_TAG")
            .addRecord(id, fs, v)
        case SValue.SVariant(id, variant, rank, v) =>
          addByte(VARIANT_TAG, "VARIANT_TAG")
            .addVariant(id, variant, rank, v)
        case SValue.SEnum(id, ctor, rank) =>
          addByte(ENUM_TAG, "ENUM_TAG")
            .addEnum(id, ctor, rank)
        case SValue.SMap(isTextMap, entries) =>
          if (isTextMap) {
            addByte(TEXT_MAP_TAG, "TEXT_MAP_TAG")
              .addTextMap(entries)
          } else {
            addByte(GEN_MAP_TAG, "GEN_MAP_TAG")
              .addGenMap(entries)
          }
        case _: SValue.SAny | _: SValue.SBigNumeric | _: SValue.SPAP | _: SValue.SStruct |
            SValue.SToken | _: SValue.STypeRep =>
          throw new IllegalArgumentException(s"Unexpected SValue during hashing: $svalue")
      }

    def addCid(cid: Value.ContractId): this.type =
      addBytes(cid2Bytes(cid), s"${cid.coid} (contractId)")

    private def addList(vs: FrontStack[SValue]): this.type =
      iterateOver(vs.toImmArray)(_ addSValue _)

    private def addVariant(
        id: Identifier,
        variant: VariantConName,
        constructorRank: Int,
        value: SValue,
    ): this.type =
      addQualifiedName(id.qualifiedName)
        .addString(variant)
        .addInt(constructorRank)
        .addSValue(value)

    private def addEnum(
        id: Identifier,
        constructor: Name,
        constructorRank: Int,
    ): this.type =
      addQualifiedName(id.qualifiedName)
        .addString(constructor)
        .addInt(constructorRank)

    private def addOptional(opt: Option[SValue]): this.type =
      opt match {
        case Some(v) => addByte(1.toByte, "Some (optional)").addSValue(v)
        case None => addByte(0.toByte, "None (optional)")
      }

    def addRecord(id: Identifier, labels: ImmArray[Name], values: ArraySeq[SValue]): this.type = {
      discard(addQualifiedName(id.qualifiedName))
      val trailingNonesSize = values.reverseIterator.takeWhile {
        case SOptional(None) => true
        case _ => false
      }.size
      labels.iterator.take(labels.length - trailingNonesSize).zip(values).foreach { case (l, v) =>
        addString(l).addSValue(v)
      }
      // This delimits the end of the record. Note that this does not collide with any of the tags.
      addByte(0xff.toByte, "record end")
    }

    private def addTextMap(entries: TreeMap[SValue, SValue]): this.type =
      iterateOver(entries.iterator, entries.size) { (acc, entry) =>
        entry match {
          case (SText(k), v) => acc.addString(k).addSValue(v)
          case _ => throw new IllegalArgumentException("Unexpected non-text key in text map")
        }
      }

    private def addGenMap(entries: TreeMap[SValue, SValue]): this.type =
      iterateOver(entries.iterator, entries.size) { (acc, entry) =>
        acc.addSValue(entry._1).addSValue(entry._2)
      }
  }
}
