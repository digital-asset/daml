// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
// We use the same package as daml-lf/transaction/src/main/scala/com/digitalasset/daml/lf/crypto/Hash.scala in order
// to access classes and methods visible to the crypto package only. Yet this file is located in a different
// bazel package so that it can depend on SValues, which we do not want the transaction package to depend on.
package crypto

import com.daml.crypto.MessageDigestPrototype
import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.crypto.Hash.{
  Builder,
  HashingError,
  Purpose,
  Version,
  aCid2Bytes,
  bigIntNumericToBytes,
  noCid2String,
  underlyingHashLength,
}
import com.digitalasset.daml.lf.crypto.HashUtils.{HashTracer, formatByteToHexString}
import com.digitalasset.daml.lf.data.Ref.{Identifier, Name}
import com.digitalasset.daml.lf.data.{Bytes, FrontStack, ImmArray, Ref}
import com.digitalasset.daml.lf.language.Ast.VariantConName
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SValue.{SOptional, SText}
import com.digitalasset.daml.lf.value.Value

import java.nio.ByteBuffer
import scala.collection.immutable.{ArraySeq, TreeMap}

/** Methods for hashing SValues modulo trailing Nones and package IDs. */
object SValueHash {

  // --- public interface ---

  /** Hashes a contract key modulo trailing Nones. Throws [[HashingError]] if [key] contains non-serializable values. */
  @throws[HashingError]
  def assertHashContractKey(
      packageName: Ref.PackageName,
      templateName: Ref.QualifiedName,
      key: SValue,
  ): Hash = {
    builder(Purpose.ContractKey, noCid2String, HashTracer.NoOp)
      .addString(packageName)
      .addQualifiedName(templateName)
      .addSValue(key)
      .build
  }

  /** Hashes a contract key modulo trailing Nones. Returns a Left if [key] contains non-serializable values. */
  def hashContractKey(
      packageName: Ref.PackageName,
      templateName: Ref.QualifiedName,
      key: SValue,
  ): Either[HashingError, Hash] =
    Hash.handleError(assertHashContractKey(packageName, templateName, key))

  /** Hashes a contract modulo trailing Nones. Throws [[HashingError]] if [arg] contains non-serializable values. */
  @throws[HashingError]
  def assertHashContractInstance(
      packageName: Ref.PackageName,
      templateName: Ref.QualifiedName,
      arg: SValue,
  ): Hash = {
    builder(Purpose.ThinContractInstance, aCid2Bytes, HashTracer.NoOp)
      .addString(packageName)
      .addQualifiedName(templateName)
      .addSValue(arg)
      .build
  }

  /** Hashes a contract modulo trailing Nones. Returns a Left if [arg] contains non-serializable values. */
  def hashContractInstance(
      packageName: Ref.PackageName,
      templateName: Ref.QualifiedName,
      arg: SValue,
  ): Either[HashingError, Hash] =
    Hash.handleError(assertHashContractInstance(packageName, templateName, arg))

  // --- implementation ---

  private def builder(
      purpose: Purpose,
      cid2Bytes: Value.ContractId => Bytes,
      hashTracer: HashTracer,
  ): SValueHashBuilder =
    new SValueHashBuilder(purpose, cid2Bytes, hashTracer).addVersion

  private object Constants {
    val INT64_TAG: Byte = 0
    val NUMERIC_TAG: Byte = 1
    val TEXT_TAG: Byte = 2
    val TIMESTAMP_TAG: Byte = 3
    val PARTY_TAG: Byte = 4
    val BOOL_TAG: Byte = 5
    val UNIT_TAG: Byte = 6
    val DATE_TAG: Byte = 7
    val RECORD_TAG: Byte = 8
    val VARIANT_TAG: Byte = 9
    val ENUM_TAG: Byte = 10
    val LIST_TAG: Byte = 11
    val OPTIONAL_TAG: Byte = 12
    val TEXT_MAP_TAG: Byte = 13
    val GEN_MAP_TAG: Byte = 14
    val CONTRACT_ID_TAG: Byte = 15

    val END_OF_RECORD: Byte = 0xff.toByte
  }

  private[crypto] final class SValueHashBuilder(
      purpose: Purpose,
      cid2Bytes: Value.ContractId => Bytes,
      hashTracer: HashTracer,
  ) extends Builder(bigIntNumericToBytes) {

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

    private[crypto] def addVersion: this.type =
      addByte(
        Version.TypedNormalForm.id,
        s"${formatByteToHexString(Version.TypedNormalForm.id)} (Value Encoding Version)",
      )
        .addByte(purpose.id, s"${formatByteToHexString(purpose.id)} (Value Encoding Purpose)")

    private[crypto] def addSValue(svalue: SValue): this.type = {
      import Constants._

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
          throw new IllegalArgumentException(
            s"Unexpected non-serializable SValue during hashing: $svalue"
          )
      }
    }

    private def addCid(cid: Value.ContractId): this.type =
      addBytes(cid2Bytes(cid), s"${cid.coid} (contractId)")

    private def addList(vs: FrontStack[SValue]): this.type =
      iterateOver(vs.iterator, vs.length)(_ addSValue _)

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

    private def addRecord(
        id: Identifier,
        labels: ImmArray[Name],
        values: ArraySeq[SValue],
    ): this.type = {
      discard(addQualifiedName(id.qualifiedName))
      val trailingNonesSize = values.reverseIterator.takeWhile {
        case SOptional(None) => true
        case _ => false
      }.size
      (labels.toSeq zip values).dropRight(trailingNonesSize).foreach { case (l, v) =>
        discard[this.type](addString(l).addSValue(v))
      }
      // This delimits the end of the record. Note that this does not collide with any of the tags.
      addByte(Constants.END_OF_RECORD, "record end")
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
