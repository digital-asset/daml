// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import com.daml.crypto.{MacPrototype, MessageDigestPrototype}

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time, Utf8}
import com.digitalasset.daml.lf.value.Value
import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.transaction.TransactionVersion
import scalaz.Order

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

  sealed abstract class HashingError(val msg: String) extends Exception with NoStackTrace
  object HashingError {
    final case class ForbiddenContractId()
        extends HashingError("Contract IDs are not supported in contract keys.")
  }

  private def handleError[X](x: => X): Either[HashingError, X] =
    try {
      Right(x)
    } catch {
      case e: HashingError => Left(e)
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
    () => hMacBuilder(seed).addLong(counter.getAndIncrement()).build
  }

  implicit val ordering: Ordering[Hash] =
    Ordering.by(_.bytes)

  implicit val order: Order[Hash] = Order.fromScalaOrdering

  private[lf] val aCid2Bytes: Value.ContractId => Bytes = { case cid @ Value.ContractId.V1(_, _) =>
    cid.toBytes
  }

  private[lf] val noCid2String: Value.ContractId => Nothing =
    _ => throw HashingError.ForbiddenContractId()

  private[crypto] sealed abstract class Builder(
      cid2Bytes: Value.ContractId => Bytes,
      upgradable: Boolean,
  ) {

    protected def update(a: ByteBuffer): Unit

    protected def update(a: Array[Byte]): Unit

    protected def doFinal(buf: Array[Byte]): Unit

    final def build: Hash = {
      val a = Array.ofDim[Byte](underlyingHashLength)
      doFinal(a)
      new Hash(Bytes.fromByteArray(a))
    }

    final def addBytes(buffer: Array[Byte]): this.type = {
      update(buffer)
      this
    }

    final def addBytes(buffer: ByteBuffer): this.type = {
      update(buffer)
      this
    }

    final def addBytes(a: Bytes): this.type =
      addBytes(a.toByteBuffer)

    private val byteBuff = Array.ofDim[Byte](1)

    final def addByte(a: Byte): this.type = {
      byteBuff(0) = a
      addBytes(byteBuff)
    }

    final def addHash(a: Hash): this.type = addBytes(a.bytes)

    final def addString(s: String): this.type = {
      val a = Utf8.getBytes(s)
      addInt(a.length).addBytes(a)
    }

    private def addString(fieldNumber: Int, v: String): this.type =
      if (v.nonEmpty) addInt(fieldNumber).addString(v) else this

    final def addBool(b: Boolean): this.type =
      addByte(if (b) 1.toByte else 0.toByte)

    private def addBool(fieldNumber: Int, v: Boolean): this.type = {
      if (v) addInt(fieldNumber).addBool(v) else this
    }

    private val intBuffer = ByteBuffer.allocate(java.lang.Integer.BYTES)

    final def addInt(a: Int): this.type = {
      discard(intBuffer.rewind())
      discard(intBuffer.putInt(a).position(0))
      addBytes(intBuffer)
    }

    private def addInt(fieldNumber: Int, v: Int): this.type =
      if (v != 0) addInt(fieldNumber).addInt(v) else this

    private val longBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)

    final def addLong(a: Long): this.type = {
      discard(longBuffer.rewind())
      discard(longBuffer.putLong(a).position(0))
      addBytes(longBuffer)
    }

    private def addLong(fieldNumber: Int, v: Long): this.type =
      if (v != 0) addInt(fieldNumber).addLong(v) else this

    final def addNumeric(v: data.Numeric): this.type = {
      val a = v.unscaledValue().toByteArray
      addInt(a.length).addBytes(a)
    }

    private def addNumeric(fieldNumber: Int, v: data.Numeric): this.type =
      if (v.signum() != 0) addInt(fieldNumber).addNumeric(v) else this

    final def iterateOver[T, U](a: ImmArray[T])(f: (this.type, T) => this.type): this.type =
      a.foldLeft[this.type](addInt(a.length))(f)

    private def iterateOver[T, U](fieldNumber: Int, a: ImmArray[T])(
        f: (this.type, T) => this.type
    ): this.type =
      if (a.nonEmpty)
        a.foldLeft[this.type](addInt(fieldNumber).addInt(a.length))(f)
      else
        this

    final def iterateOver[T](a: Iterator[T], size: Int)(f: (this.type, T) => this.type): this.type =
      a.foldLeft[this.type](addInt(size))(f)

    final def iterateOver[T](fieldNumber: Int, a: Iterator[T], size: Int)(
        f: (this.type, T) => this.type
    ): this.type =
      if (a.nonEmpty)
        a.foldLeft[this.type](addInt(fieldNumber).addInt(size))(f)
      else
        this

    final def iterateOver[T](v: Option[T])(f: (this.type, T) => this.type): this.type =
      v match {
        case Some(v) =>
          f(addInt(1), v)
        case None =>
          addInt(0)
      }

    final def iterateOver[T](fieldNumber: Int, v: Option[T])(
        f: (this.type, T) => this.type
    ): this.type =
      v match {
        case Some(v) =>
          f(addInt(fieldNumber), v)
        case None =>
          this
      }

    final def addDottedName(name: Ref.DottedName): this.type =
      iterateOver(name.segments)(_ addString _)

    final def addQualifiedName(name: Ref.QualifiedName): this.type =
      addDottedName(name.module).addDottedName(name.name)

    final def addIdentifier(id: Ref.Identifier): this.type =
      addString(id.packageId).addQualifiedName(id.qualifiedName)

    final def addStringSet[S <: String](set: Set[S]): this.type = {
      val ss = set.toSeq.sorted[String]
      iterateOver(ss.iterator, ss.size)(_ addString _)
    }

    @throws[HashingError]
    final def addCid(cid: Value.ContractId): this.type = {
      val bytes = cid2Bytes(cid)
      addInt(bytes.length).addBytes(bytes)
    }

    private def addRecord(fs: ImmArray[(_, Value)]): this.type =
      if (upgradable) {
        fs.iterator.zipWithIndex.foreach { case ((_, v), i) =>
          discard(addTypedValue(i, v))
        }
        this
      } else {
        iterateOver(fs)(_ addTypedValue _._2)
      }

    // In order to avoid hash collision, this should be used together
    // with another data representing uniquely the type of `value`.
    // See for instance hash : Node.GlobalKey => SHA256Hash
    @throws[HashingError]
    final def addTypedValue(value: Value): this.type =
      value match {
        case Value.ValueUnit =>
          addByte(0.toByte)
        case Value.ValueBool(b) =>
          addBool(b)
        case Value.ValueInt64(v) =>
          addLong(v)
        case Value.ValueNumeric(v) =>
          addNumeric(v)
        case Value.ValueTimestamp(v) =>
          addLong(v.micros)
        case Value.ValueDate(v) =>
          addInt(v.days)
        case Value.ValueParty(v) =>
          addString(v)
        case Value.ValueText(v) =>
          addString(v)
        case Value.ValueContractId(cid) =>
          addCid(cid)
        case Value.ValueOptional(opt) =>
          iterateOver(opt)(_ addTypedValue _)
        case Value.ValueList(xs) =>
          iterateOver(xs.toImmArray)(_ addTypedValue _)
        case Value.ValueTextMap(xs) =>
          iterateOver(xs.toImmArray)((acc, x) => acc.addString(x._1).addTypedValue(x._2))
        case Value.ValueRecord(_, fs) =>
          addRecord(fs)
        case Value.ValueVariant(_, variant, v) =>
          addString(variant).addTypedValue(v)
        case Value.ValueEnum(_, v) =>
          addString(v)
        case Value.ValueGenMap(entries) =>
          iterateOver(entries.iterator, entries.length)((acc, x) =>
            acc.addTypedValue(x._1).addTypedValue(x._2)
          )
      }

    private def addTypedValue(fieldNumber: Int, value: Value): this.type =
      value match {
        case Value.ValueUnit =>
          this
        case Value.ValueBool(b) =>
          addBool(fieldNumber, b)
        case Value.ValueInt64(v) =>
          addLong(fieldNumber, v)
        case Value.ValueNumeric(v) =>
          addNumeric(fieldNumber, v)
        case Value.ValueTimestamp(v) =>
          addLong(fieldNumber, v.micros)
        case Value.ValueDate(v) =>
          addInt(fieldNumber, v.days)
        case Value.ValueParty(v) =>
          // No default
          addInt(fieldNumber).addString(v)
        case Value.ValueText(v) =>
          addString(fieldNumber, v)
        case Value.ValueContractId(cid) =>
          // No default
          addInt(fieldNumber).addCid(cid)
        case Value.ValueOptional(opt) =>
          iterateOver(fieldNumber, opt)(_ addTypedValue _)
        case Value.ValueList(xs) =>
          iterateOver(fieldNumber, xs.toImmArray)(_ addTypedValue _)
        case Value.ValueTextMap(xs) =>
          iterateOver(fieldNumber, xs.toImmArray)((acc, x) =>
            acc.addString(x._1).addTypedValue(x._2)
          )
        case Value.ValueRecord(_, fs) =>
          // No default
          addInt(fieldNumber).addRecord(fs)
        case Value.ValueVariant(_, variant, v) =>
          // No default
          addInt(fieldNumber).addString(variant).addTypedValue(v)
        case Value.ValueEnum(_, v) =>
          // No default
          addInt(fieldNumber).addString(v)
        case Value.ValueGenMap(entries) =>
          iterateOver(fieldNumber, entries.iterator, entries.length)((acc, x) =>
            acc.addTypedValue(x._1).addTypedValue(x._2)
          )
      }
  }

  // The purpose of a hash serves to avoid hash collisions due to equal encodings for different objects.
  // Each purpose should be used at most once.
  private[crypto] case class Purpose(id: Byte)

  private[crypto] object Purpose {
    val Testing = Purpose(1)
    val ContractKey = Purpose(2)
    val MaintainerContractKeyUUID = Purpose(4)
    val PrivateKey = Purpose(3)
    val ContractInstance = Purpose(5)
    val ChangeId = Purpose(6)
  }

  // package private for testing purpose.
  // Do not call this method from outside Hash object/
  private[crypto] def builder(
      purpose: Purpose,
      cid2Bytes: Value.ContractId => Bytes,
      upgradable: Boolean = true,
  ): Builder = new Builder(cid2Bytes, upgradable) {

    private val md = MessageDigestPrototype.Sha256.newDigest

    override protected def update(a: ByteBuffer): Unit =
      md.update(a)

    override protected def update(a: Array[Byte]): Unit =
      md.update(a)

    override protected def doFinal(buf: Array[Byte]): Unit =
      assert(md.digest(buf, 0, underlyingHashLength) == underlyingHashLength)

    md.update(version)
    md.update(purpose.id)

  }

  private[crypto] def hMacBuilder(key: Hash): Builder =
    new Builder(noCid2String, upgradable = true) {

      private val macPrototype: MacPrototype = MacPrototype.HmacSha256
      private val mac: Mac = macPrototype.newMac

      mac.init(new SecretKeySpec(key.bytes.toByteArray, macPrototype.algorithm))

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
    builder(Purpose.PrivateKey, noCid2String).addString(s).build

  // This function assumes that key is well typed, i.e. :
  // 1 - `templateId` is the identifier for a template with a key of type τ
  // 2 - `key` is a value of type τ
  @throws[HashingError]
  def assertHashContractKey(
      templateId: Ref.Identifier,
      packageName: Ref.PackageName,
      key: Value,
  ): Hash = {
    val hashBuilder = builder(Purpose.ContractKey, noCid2String)
    hashBuilder
      .addQualifiedName(templateId.qualifiedName)
      .addString(packageName)
      .addTypedValue(key)
      .build
  }

  def hashContractKey(
      templateId: Ref.Identifier,
      packageName: Ref.PackageName,
      key: Value,
  ): Either[HashingError, Hash] =
    handleError(assertHashContractKey(templateId, packageName: Ref.PackageName, key))

  // This function assumes that `arg` is well typed, i.e. :
  // 1 - `packageName` is the package name defined in the metadata of the package containing template `templateId`
  // 2 - `templateId` is the identifier for a template with a contract argument of type τ
  // 3 - `arg` is a value of type τ
  // The hash is not stable under suffixing of contract IDs
  @throws[HashingError]
  def assertHashContractInstance(
      templateId: Ref.Identifier,
      arg: Value,
      packageName: Ref.PackageName = Ref.PackageName.assertFromString("default"),
      langVersion: TransactionVersion,
  ): Hash = {
    import Ordering.Implicits._
    val b = builder(
      Purpose.ContractInstance,
      aCid2Bytes,
      langVersion >= TransactionVersion.minContractKeys,
    )
    discard(b.addString(packageName).addIdentifier(templateId))
    discard(b.addTypedValue(arg))
    b.build
  }

  def hashContractInstance(
      packageName: Ref.PackageName = Ref.PackageName.assertFromString("default"),
      templateId: Ref.Identifier,
      arg: Value,
      langVersion: TransactionVersion,
  ): Either[HashingError, Hash] =
    handleError(assertHashContractInstance(templateId, arg, packageName, langVersion))

  def hashChangeId(
      applicationId: Ref.ApplicationId,
      commandId: Ref.CommandId,
      actAs: Set[Ref.Party],
  ): Hash =
    builder(Purpose.ChangeId, noCid2String)
      .addString(applicationId)
      .addString(commandId)
      .addStringSet(actAs)
      .build

  def deriveSubmissionSeed(
      nonce: Hash,
      applicationId: Ref.LedgerString,
      commandId: Ref.LedgerString,
      submitter: Ref.Party,
  ): Hash =
    hMacBuilder(nonce)
      .addString(applicationId)
      .addString(commandId)
      .addString(submitter)
      .build

  def deriveTransactionSeed(
      submissionSeed: Hash,
      participantId: Ref.ParticipantId,
      submitTime: Time.Timestamp,
  ): Hash =
    hMacBuilder(submissionSeed)
      .addString(participantId)
      .addLong(submitTime.micros)
      .build

  def deriveNodeSeed(
      parentDiscriminator: Hash,
      childIdx: Int,
  ): Hash =
    hMacBuilder(parentDiscriminator).addInt(childIdx).build

  def deriveContractDiscriminator(
      nodeSeed: Hash,
      submitTime: Time.Timestamp,
      parties: Set[Ref.Party],
  ): Hash =
    hMacBuilder(nodeSeed)
      .addLong(submitTime.micros)
      .addStringSet(parties)
      .build

  // For Daml-on-Corda to ensure the hashing is performed in a way that will work with upgrades.
  def deriveMaintainerContractKeyUUID(
      keyHash: Hash,
      maintainer: Ref.Party,
  ): Hash =
    builder(Purpose.MaintainerContractKeyUUID, noCid2String)
      .addHash(keyHash)
      .addString(maintainer)
      .build
}
