// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util

import com.digitalasset.daml.lf.data.{ImmArray, Ref, Utf8}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value

class Hash private (private val bytes: Array[Byte]) {

  def toByteArray: Array[Byte] = bytes.clone()

  def toHexa: Ref.LedgerString =
    Ref.LedgerString.assertFromString(bytes.map("%02x" format _).mkString)

  override def toString: String = s"Hash($toHexa)"

  override def equals(other: Any): Boolean =
    other match {
      case otherHash: Hash => util.Arrays.equals(bytes, otherHash.bytes)
      case _ => false
    }

  private var _hashCode: Int = 0

  override def hashCode(): Int = {
    if (_hashCode == 0) {
      val code = util.Arrays.hashCode(bytes)
      _hashCode = if (code == 0) 1 else code
    }
    _hashCode
  }

}

object Hash {

  private[crypto] abstract class Builder {

    def add(a: Array[Byte]): this.type

    def add(a: ByteBuffer): this.type

    def add(a: Byte): this.type

    def build: Hash

    final def add(a: Hash): this.type = add(a.bytes)

    final def add(s: String): this.type = {
      val a = Utf8.getBytes(s)
      add(a.length).add(a)
    }

    private val intBuffer = ByteBuffer.allocate(java.lang.Integer.BYTES)

    def add(a: Int): this.type = {
      intBuffer.rewind()
      add(intBuffer.putInt(a).array())
    }

    private val longBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)

    def add(a: Long): this.type = {
      longBuffer.rewind()
      add(longBuffer.putLong(a).array())
    }

    def iterateOver[T, U](a: ImmArray[T])(f: (this.type, T) => this.type): this.type =
      a.foldLeft[this.type](add(a.length))(f)

    def addDottedName(name: Ref.DottedName): this.type =
      iterateOver(name.segments)(_ add _)

    def addQualifiedName(name: Ref.QualifiedName): this.type =
      addDottedName(name.module).addDottedName(name.name)

    def addIdentifier(id: Ref.Identifier): this.type =
      add(id.packageId).addQualifiedName(id.qualifiedName)

    // In order to avoid hash collision, this should be used together
    // with an other data representing uniquely the type of `value`.
    // See for instance hash : Node.GlobalKey => SHA256Hash
    def addTypedValue(value: Value[Value.AbsoluteContractId]): this.type =
      value match {
        case Value.ValueUnit =>
          add(0.toByte)
        case Value.ValueBool(b) =>
          add(if (b) 1.toByte else 0.toByte)
        case Value.ValueInt64(v) =>
          add(v)
        case Value.ValueNumeric(v) =>
          val a = v.unscaledValue().toByteArray
          add(a.length).add(a)
        case Value.ValueTimestamp(v) =>
          add(v.micros)
        case Value.ValueDate(v) =>
          add(v.days)
        case Value.ValueParty(v) =>
          add(v)
        case Value.ValueText(v) =>
          add(v)
        case Value.ValueContractId(v) =>
          add(v.coid)
        case Value.ValueOptional(None) =>
          add(0)
        case Value.ValueOptional(Some(v)) =>
          add(1).addTypedValue(v)
        case Value.ValueList(xs) =>
          iterateOver(xs.toImmArray)(_ addTypedValue _)
        case Value.ValueTextMap(xs) =>
          iterateOver(xs.toImmArray)((acc, x) => acc.add(x._1).addTypedValue(x._2))
        case Value.ValueRecord(_, fs) =>
          iterateOver(fs)(_ addTypedValue _._2)
        case Value.ValueVariant(_, variant, v) =>
          add(variant).addTypedValue(v)
        case Value.ValueEnum(_, v) =>
          add(v)
        case Value.ValueGenMap(_) =>
          sys.error("Hashing of generic map not implemented")
        // Struct: should never be encountered
        case Value.ValueStruct(_) =>
          sys.error("Hashing of struct values is not supported")
      }

  }

  // The purpose of a hash serves to avoid hash collisions due to equal encodings for different objects.
  // Each purpose should be used at most one.
  private[crypto] case class Purpose(id: Byte)

  private[crypto] object Purpose {
    val Testing = Purpose(1)
    val ContractKey = Purpose(2)
  }

  // package private for testing purpose.
  // Do not call this method from outside Hash object/
  private[crypto] def builder(purpose: Purpose): Builder = new Builder {

    private val md = MessageDigest.getInstance("SHA-256")

    override def add(a: Array[Byte]): this.type = {
      md.update(a)
      this
    }

    override def add(a: ByteBuffer): this.type = {
      md.update(a)
      this
    }

    override def add(a: Byte): this.type = {
      md.update(a)
      this
    }

    add(purpose.id)

    override def build: Hash = new Hash(md.digest)
  }

  // This function assumes that key is well typed, i.e. :
  // 1 - `key.identifier` is the identifier for a template with a key of type τ
  // 2 - `key.key` is a value of type τ
  def apply(key: Node.GlobalKey): Hash =
    Hash
      .builder(Hash.Purpose.ContractKey)
      .addIdentifier(key.templateId)
      .addTypedValue(key.key.value)
      .build

}
