// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package crypto

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong

import com.daml.lf.data.{Bytes, ImmArray, Ref, Time, Utf8}
import com.daml.lf.value.Value
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import scala.util.control.NoStackTrace

final class Hash private (val bytes: Bytes) {

  override def hashCode(): Int = bytes.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case that: Hash => this.bytes equals that.bytes
    case _ => false
  }

  def toHexString: Ref.HexString = Ref.HexString.encode(bytes)

  override def toString: String = s"Hash($toHexString)"
}

object Hash {

  val version = 0.toByte
  val underlyingHashLength = 32

  private case class HashingError(msg: String) extends Exception with NoStackTrace

  private def error(msg: String): Nothing =
    throw HashingError(msg)

  private def handleError[X](x: => X): Either[String, X] =
    try {
      Right(x)
    } catch {
      case HashingError(msg) => Left(msg)
    }

  def fromBytes(bs: Bytes): Either[String, Hash] =
    Either.cond(
      bs.length == underlyingHashLength,
      new Hash(bs),
      s"hash should have ${underlyingHashLength} bytes, got ${bs.length}",
    )

  def assertFromBytes(bs: Bytes): Hash =
    data.assertRight(fromBytes(bs))

  def fromByteArray(a: Array[Byte]): Either[String, Hash] =
    fromBytes(Bytes.fromByteArray(a))

  def assertFromByteArray(a: Array[Byte]): Hash =
    data.assertRight(fromByteArray(a))

  // A cryptographic pseudo random generator of Hashes based on hmac
  // Must be given a high entropy seed when used in production.
  // Thread safe
  def secureRandom(seed: Hash): () => Hash = {
    val counter = new AtomicLong
    () =>
      hMacBuilder(seed).add(counter.getAndIncrement()).build
  }

  implicit val ordering: Ordering[Hash] =
    Ordering.by(_.bytes)

  @throws[HashingError]
  private[lf] val aCid2Bytes: Value.ContractId => Bytes = {
    case cid @ Value.AbsoluteContractId.V1(_, _) =>
      cid.toBytes
    case Value.AbsoluteContractId.V0(s) =>
      Utf8.getBytes(s)
    case Value.RelativeContractId(_) =>
      error("Hashing of relative contract id is not supported")
  }

  @throws[HashingError]
  private[lf] val noCid2String: Value.ContractId => Nothing =
    _ => error("Hashing of relative contract id is not supported in contract key")

  private[crypto] sealed abstract class Builder(cid2Bytes: Value.ContractId => Bytes) {

    protected def update(a: ByteBuffer): Unit

    protected def update(a: Array[Byte]): Unit

    protected def doFinal(buf: Array[Byte]): Unit

    final def build: Hash = {
      val a = Array.ofDim[Byte](underlyingHashLength)
      doFinal(a)
      new Hash(Bytes.fromByteArray(a))
    }

    final def add(buffer: Array[Byte]): this.type = {
      update(buffer)
      this
    }

    final def add(buffer: ByteBuffer): this.type = {
      update(buffer)
      this
    }

    final def add(a: Bytes): this.type =
      add(a.toByteBuffer)

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

    final def add(b: Boolean): this.type =
      add(if (b) 1.toByte else 0.toByte)

    private val intBuffer = ByteBuffer.allocate(java.lang.Integer.BYTES)

    final def add(a: Int): this.type = {
      intBuffer.rewind()
      intBuffer.putInt(a).position(0)
      add(intBuffer)
    }

    private val longBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)

    final def add(a: Long): this.type = {
      longBuffer.rewind()
      longBuffer.putLong(a).position(0)
      add(longBuffer)
    }

    final def iterateOver[T, U](a: ImmArray[T])(f: (this.type, T) => this.type): this.type =
      a.foldLeft[this.type](add(a.length))(f)

    final def iterateOver[T](a: Iterator[T], size: Int)(f: (this.type, T) => this.type): this.type =
      a.foldLeft[this.type](add(size))(f)

    final def iterateOver[T](a: Option[T])(f: (this.type, T) => this.type): this.type =
      a.fold[this.type](add(0))(f(add(1), _))

    final def addDottedName(name: Ref.DottedName): this.type =
      iterateOver(name.segments)(_ add _)

    final def addQualifiedName(name: Ref.QualifiedName): this.type =
      addDottedName(name.module).addDottedName(name.name)

    final def addIdentifier(id: Ref.Identifier): this.type =
      add(id.packageId).addQualifiedName(id.qualifiedName)

    final def addStringSet[S <: String](set: Set[S]): this.type = {
      val ss = set.toSeq.sorted[String]
      iterateOver(ss.iterator, ss.size)(_ add _)
    }

    @throws[HashingError]
    final def addCid(cid: Value.ContractId): this.type = {
      val bytes = cid2Bytes(cid)
      add(bytes.length)
      add(bytes)
    }

    // In order to avoid hash collision, this should be used together
    // with another data representing uniquely the type of `value`.
    // See for instance hash : Node.GlobalKey => SHA256Hash
    @throws[HashingError]
    final def addTypedValue(value: Value[Value.ContractId]): this.type =
      value match {
        case Value.ValueUnit =>
          add(0.toByte)
        case Value.ValueBool(b) =>
          add(b)
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
        case Value.ValueContractId(cid) =>
          addCid(cid)
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
          error("Hashing of generic map not implemented")
        // Struct: should never be encountered
        case Value.ValueStruct(_) =>
          error("Hashing of struct values is not supported")
      }
  }

  // The purpose of a hash serves to avoid hash collisions due to equal encodings for different objects.
  // Each purpose should be used at most one.
  private[crypto] case class Purpose(id: Byte)

  private[crypto] object Purpose {
    val Testing = Purpose(1)
    val ContractKey = Purpose(2)
    val MaintainerContractKeyUUID = Purpose(4)
    val PrivateKey = Purpose(3)
  }

  // package private for testing purpose.
  // Do not call this method from outside Hash object/
  private[crypto] def builder(
      purpose: Purpose,
      cid2Bytes: Value.ContractId => Bytes,
  ): Builder = new Builder(cid2Bytes) {

    private val md = MessageDigest.getInstance("SHA-256")

    override protected def update(a: ByteBuffer): Unit =
      md.update(a)

    override protected def update(a: Array[Byte]): Unit =
      md.update(a)

    override protected def doFinal(buf: Array[Byte]): Unit =
      assert(md.digest(buf, 0, underlyingHashLength) == underlyingHashLength)

    md.update(version)
    md.update(purpose.id)

  }

  private val hMacAlgorithm = "HmacSHA256"

  private[crypto] def hMacBuilder(key: Hash): Builder = new Builder(noCid2String) {

    private val mac: Mac = Mac.getInstance(hMacAlgorithm)

    mac.init(new SecretKeySpec(key.bytes.toByteArray, hMacAlgorithm))

    override protected def update(a: ByteBuffer): Unit =
      mac.update(a)

    override protected def update(a: Array[Byte]): Unit =
      mac.update(a)

    override protected def doFinal(buf: Array[Byte]): Unit =
      mac.doFinal(buf, 0)

  }

  def fromHexString(s: Ref.HexString): Either[String, Hash] = {
    val bytes = Ref.HexString.decode(s)
    Either.cond(
      bytes.length == underlyingHashLength,
      new Hash(bytes),
      s"Cannot parse hash $s",
    )
  }

  def fromString(s: String): Either[String, Hash] =
    for {
      hexaString <- Ref.HexString.fromString(s)
      hash <- fromHexString(hexaString)
    } yield hash

  def assertFromString(s: String): Hash =
    data.assertRight(fromString(s))

  def hashPrivateKey(s: String): Hash =
    builder(Purpose.PrivateKey, noCid2String).add(s).build

  // This function assumes that key is well typed, i.e. :
  // 1 - `templateId` is the identifier for a template with a key of type τ
  // 2 - `key` is a value of type τ
  @throws[HashingError]
  def assertHashContractKey(templateId: Ref.Identifier, key: Value[Value.ContractId]): Hash =
    builder(Purpose.ContractKey, noCid2String)
      .addIdentifier(templateId)
      .addTypedValue(key)
      .build

  def safeHashContractKey(templateId: Ref.Identifier, key: Value[Nothing]): Hash =
    assertHashContractKey(templateId, key)

  def hashContractKey(
      templateId: Ref.Identifier,
      key: Value[Value.ContractId],
  ): Either[String, Hash] =
    handleError(assertHashContractKey(templateId, key))

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
      .addStringSet(parties)
      .build

  // For DAML-on-Corda to ensure the hashing is performed in a way that will work with upgrades.
  def deriveMaintainerContractKeyUUID(
      keyHash: Hash,
      maintainer: Ref.Party,
  ): Hash =
    builder(Purpose.MaintainerContractKeyUUID, noCid2String)
      .add(keyHash)
      .add(maintainer)
      .build
}
