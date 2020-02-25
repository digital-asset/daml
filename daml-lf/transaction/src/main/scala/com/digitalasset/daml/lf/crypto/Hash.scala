// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import java.nio.ByteBuffer
import java.security.{MessageDigest, SecureRandom}
import java.util
import java.util.concurrent.atomic.AtomicLong

import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time, Utf8}
import com.digitalasset.daml.lf.value.Value
import com.google.common.io.BaseEncoding
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import scala.util.control.{NoStackTrace, NonFatal}

final class Hash private (private val bytes: Array[Byte]) {

  def toByteArray: Array[Byte] = bytes.clone()

  def toHexaString: String = Hash.hexEncoding.encode(bytes)

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

  private val hexEncoding = BaseEncoding.base16().lowerCase()

  private case class HashingError(msg: String) extends Error with NoStackTrace

  private def error(msg: String): Nothing =
    throw HashingError(msg)

  private def handleError[X](x: => X): Either[String, X] =
    try {
      Right(x)
    } catch {
      case HashingError(msg) => Left(msg)
    }

  def fromBytes(a: Array[Byte]): Either[String, Hash] =
    Either.cond(
      a.length == underlyingHashLength,
      new Hash(a.clone()),
      s"hash should have ${underlyingHashLength} bytes, found ${a.length}",
    )

  def assertFromBytes(a: Array[Byte]): Hash =
    data.assertRight(fromBytes(a))

  // A pseudo random generator for Hash based on hmac
  // We mix the given seed with time to mitigate very bad seed.
  def random(seed: Array[Byte]): () => Hash = {
    if (seed.length != underlyingHashLength)
      throw new IllegalArgumentException(s"expected a 32 bytes seed, get ${seed.length} bytes.")
    val counter = new AtomicLong
    val seedWithTime =
      hMacBuilder(new Hash(seed)).add(Time.Timestamp.now().micros).build
    () =>
      hMacBuilder(seedWithTime).add(counter.getAndIncrement()).build
  }

  def random(seed: Hash): () => Hash = random(seed.bytes)

  // A pseudo random generator for Hash using the best available source of entropy to generate the seed.
  def secureRandom: () => Hash =
    random(SecureRandom.getInstanceStrong.generateSeed(underlyingHashLength))

  implicit val HashOrdering: Ordering[Hash] =
    ((hash1, hash2) => implicitly[Ordering[Iterable[Byte]]].compare(hash1.bytes, hash2.bytes))

  @throws[HashingError]
  private[lf] val aCid2String: Value.ContractId => String = {
    case (Value.AbsoluteContractId(cid)) =>
      cid
    case (Value.RelativeContractId(_)) =>
      error("Hashing of relative contract id is not supported")
  }

  @throws[HashingError]
  private[lf] val noCid2String: Value.ContractId => Nothing =
    _ => error("Hashing of relative contract id is not supported in contract key")

  private[crypto] sealed abstract class Builder(cid2String: Value.ContractId => String) {

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

    final def add(b: Boolean): this.type =
      add(if (b) 1.toByte else 0.toByte)

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
    final def addCid(cid: Value.ContractId): this.type =
      add(cid2String(cid))

    // In order to avoid hash collision, this should be used together
    // with an other data representing uniquely the type of `value`.
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
    val PrivateKey = Purpose(3)
  }

  // package private for testing purpose.
  // Do not call this method from outside Hash object/
  private[crypto] def builder(
      purpose: Purpose,
      cid2String: Value.ContractId => String,
  ): Builder = new Builder(cid2String) {

    private val md = MessageDigest.getInstance("SHA-256")

    override protected def update(a: Array[Byte]): Unit =
      md.update(a)

    override protected def doFinal(buf: Array[Byte], offset: Int): Unit =
      assert(md.digest(buf, offset, underlyingHashLength) == underlyingHashLength)

    md.update(version)
    md.update(purpose.id)

  }

  private val hMacAlgorithm = "HmacSHA256"

  private[crypto] def hMacBuilder(key: Hash): Builder = new Builder(noCid2String) {

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
      val bytes = hexEncoding.decode(s)
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

}
