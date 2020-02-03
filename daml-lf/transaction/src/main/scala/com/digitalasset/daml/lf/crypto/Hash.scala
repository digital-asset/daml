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
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import scala.util.control.NonFatal

final class Hash private (private val bytes: Array[Byte]) {

  def toByteArray: Array[Byte] = bytes.clone()

  def toLedgerString: Ref.LedgerString =
    Hash.toLedgerString(this)

  override def toString: String = s"Hash($toLedgerString)"

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

  private val version = 0.toByte
  private val underlyingHashLength = 32

  implicit val HashOrdering: Ordering[Hash] =
    ((hash1, hash2) => implicitly[Ordering[Iterable[Byte]]].compare(hash1.bytes, hash2.bytes))

  private[crypto] sealed abstract class Builder {

    protected def update(a: Array[Byte]): Unit

    protected def doFinal(buf: Array[Byte], offset: Int): Unit

    final def build: Hash = {
      val a = Array.ofDim[Byte](underlyingHashLength)
      doFinal(a, 0)
      new Hash(a)
    }

    final def add(a: Array[Byte]): this.type = {
      update(a)
      this
    }

    private val byteBuff = Array.ofDim[Byte](1)

    final def add(a: Byte): this.type = {
      byteBuff(0) = a
      add(byteBuff)
    }

    final def add(a: Hash): this.type = add(a.bytes)

    final def add(s: String): this.type = {
      val a = Utf8.getBytes(s)
      add(a.length).add(a)
    }

    private val intBuffer = ByteBuffer.allocate(java.lang.Integer.BYTES)

    final def add(a: Int): this.type = {
      intBuffer.rewind()
      add(intBuffer.putInt(a).array())
    }

    private val longBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)

    final def add(a: Long): this.type = {
      longBuffer.rewind()
      add(longBuffer.putLong(a).array())
    }

    final def iterateOver[T, U](a: ImmArray[T])(f: (this.type, T) => this.type): this.type =
      a.foldLeft[this.type](add(a.length))(f)

    final def iterateOver[T](a: Iterator[T], size: Int)(f: (this.type, T) => this.type): this.type =
      a.foldLeft[this.type](add(size))(f)

    final def addDottedName(name: Ref.DottedName): this.type =
      iterateOver(name.segments)(_ add _)

    final def addQualifiedName(name: Ref.QualifiedName): this.type =
      addDottedName(name.module).addDottedName(name.name)

    final def addIdentifier(id: Ref.Identifier): this.type =
      add(id.packageId).addQualifiedName(id.qualifiedName)

    // In order to avoid hash collision, this should be used together
    // with an other data representing uniquely the type of `value`.
    // See for instance hash : Node.GlobalKey => SHA256Hash
    final def addTypedValue(value: Value[Value.AbsoluteContractId]): this.type =
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
    val PrivateKey = Purpose(3)
  }

  // package private for testing purpose.
  // Do not call this method from outside Hash object/
  private[crypto] def builder(purpose: Purpose): Builder = new Builder {

    private val md = MessageDigest.getInstance("SHA-256")

    override protected def update(a: Array[Byte]): Unit =
      md.update(a)

    override protected def doFinal(buf: Array[Byte], offset: Int): Unit =
      assert(md.digest(buf, offset, underlyingHashLength) == underlyingHashLength)

    md.update(version)
    md.update(purpose.id)

  }

  private val hMacAlgorithm = "HmacSHA256"

  private[crypto] def hMacBuilder(key: Hash): Builder = new Builder {

    private val mac: Mac = Mac.getInstance(hMacAlgorithm)

    mac.init(new SecretKeySpec(key.bytes, hMacAlgorithm))

    override protected def update(a: Array[Byte]): Unit =
      mac.update(a)

    override protected def doFinal(buf: Array[Byte], offset: Int): Unit =
      mac.doFinal(buf, offset)

  }

  def toLedgerString(hash: Hash): Ref.LedgerString =
    Ref.LedgerString.assertFromString(hash.bytes.map("%02x" format _).mkString)

  def fromString(s: String): Either[String, Hash] = {
    def error = s"Cannot parse hash $s"
    try {
      val bytes = s.sliding(2, 2).map(Integer.parseInt(_, 16).toByte).toArray
      Either.cond(
        bytes.length == underlyingHashLength,
        new Hash(bytes),
        error,
      )
    } catch {
      case NonFatal(_) => Left(error)
    }
  }

  def assertFromString(s: String): Hash =
    data.assertRight(fromString(s))

  def hashPrivateKey(s: String): Hash =
    builder(Purpose.PrivateKey).add(s).build

  // This function assumes that key is well typed, i.e. :
  // 1 - `key.identifier` is the identifier for a template with a key of type τ
  // 2 - `key.key` is a value of type τ
  def hashContractKey(key: Node.GlobalKey): Hash =
    builder(Hash.Purpose.ContractKey)
      .addIdentifier(key.templateId)
      .addTypedValue(key.key.value)
      .build

  def deriveTransactionSeed(
      nonce: Hash,
      participantId: Ref.LedgerString,
      applicationId: Ref.LedgerString,
      commandId: Ref.LedgerString,
      submitter: Ref.Party,
  ): Hash =
    hMacBuilder(nonce)
      .add(participantId)
      .add(applicationId)
      .add(commandId)
      .add(submitter)
      .build

  def deriveNodeDiscriminator(
      parentDiscriminator: Hash,
      childIdx: Int,
  ): Hash =
    hMacBuilder(parentDiscriminator).add(childIdx).build

  def deriveContractDiscriminator(
      nodeDiscriminator: Hash,
      parties: Set[Ref.Party],
  ) =
    hMacBuilder(nodeDiscriminator)
      .iterateOver(parties.toSeq.sorted[String].iterator, parties.size)(_ add _)
      .build

}
