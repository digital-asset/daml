// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import java.nio.ByteBuffer
import java.security.{MessageDigest, SecureRandom}
import java.util

import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time, Utf8}
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.value.Value
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import scala.util.control.NonFatal

final class Hash private (private val bytes: Array[Byte]) {

  def toByteArray: Array[Byte] = bytes.clone()

  def toHexaString: String =
    bytes.map("%02x" format _).mkString

  override def toString: String = s"Hash($toHexaString)"

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

  def fromBytes(a: Array[Byte]): Either[String, Hash] =
    Either.cond(
      a.length == underlyingHashLength,
      new Hash(a.clone()),
      s"hash should have ${underlyingHashLength} bytes, found ${a.length}",
    )

  def assertFromBytes(a: Array[Byte]): Hash =
    data.assertRight(fromBytes(a))

  def secureRandom(seed: Array[Byte]): () => Hash = {
    val random = new SecureRandom(seed)
    () =>
      {
        val a = Array.ofDim[Byte](underlyingHashLength)
        random.nextBytes(a)
        new Hash(a)
      }
  }

  def secureRandom: () => Hash =
    secureRandom(SecureRandom.getSeed(underlyingHashLength))

  implicit val HashOrdering: Ordering[Hash] =
    ((hash1, hash2) => implicitly[Ordering[Iterable[Byte]]].compare(hash1.bytes, hash2.bytes))

  private[crypto] sealed abstract class Builder(purpose: Purpose) {

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
          purpose match {
            case Purpose.ContractKey =>
              sys.error("Hashing of contract id for contract keys is not supported")
            case _ => add(v.coid)
          }
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
  private[crypto] def builder(purpose: Purpose): Builder = new Builder(purpose) {

    private val md = MessageDigest.getInstance("SHA-256")

    override protected def update(a: Array[Byte]): Unit =
      md.update(a)

    override protected def doFinal(buf: Array[Byte], offset: Int): Unit =
      assert(md.digest(buf, offset, underlyingHashLength) == underlyingHashLength)

    md.update(version)
    md.update(purpose.id)

  }

  private val hMacAlgorithm = "HmacSHA256"

  private[crypto] def hMacBuilder(key: Hash): Builder = new Builder(Purpose.PrivateKey) {

    private val mac: Mac = Mac.getInstance(hMacAlgorithm)

    mac.init(new SecretKeySpec(key.bytes, hMacAlgorithm))

    override protected def update(a: Array[Byte]): Unit =
      mac.update(a)

    override protected def doFinal(buf: Array[Byte], offset: Int): Unit =
      mac.doFinal(buf, offset)

  }

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
  // 1 - `templateId` is the identifier for a template with a key of type τ
  // 2 - `key` is a value of type τ
  def hashContractKey(templateId: Identifier, key: Value[Nothing]): Hash =
    builder(Hash.Purpose.ContractKey)
      .addIdentifier(templateId)
      .addTypedValue(key)
      .build

  def deriveSubmissionSeed(
      nonce: Hash,
      applicationId: Ref.LedgerString,
      commandId: Ref.LedgerString,
      submitter: Ref.Party,
  ): Hash =
    hMacBuilder(nonce)
      .add(applicationId)
      .add(commandId)
      .add(submitter)
      .build

  def deriveTransactionSeed(
      submissionSeed: Hash,
      participantId: Ref.ParticipantId,
      submitTime: Time.Timestamp,
  ): Hash =
    hMacBuilder(submissionSeed)
      .add(participantId)
      .add(submitTime.micros)
      .build

  def deriveNodeSeed(
      parentDiscriminator: Hash,
      childIdx: Int,
  ): Hash =
    hMacBuilder(parentDiscriminator).add(childIdx).build

  def deriveContractDiscriminator(
      nodeSeed: Hash,
      submitTime: Time.Timestamp,
      parties: Set[Ref.Party],
  ): Hash =
    hMacBuilder(nodeSeed)
      .add(submitTime.micros)
      .iterateOver(parties.toSeq.sorted[String].iterator, parties.size)(_ add _)
      .build

}
