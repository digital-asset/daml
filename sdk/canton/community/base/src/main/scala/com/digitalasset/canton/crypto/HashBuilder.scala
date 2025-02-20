// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.checked
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.digitalasset.daml.lf.data
import com.digitalasset.daml.lf.data.ImmArray
import com.google.protobuf.ByteString

import java.nio.ByteBuffer
import java.security.MessageDigest

/** The methods of [[HashBuilder]] change its internal state and return `this` for convenience.
  *
  * Requirements for all implementations:
  *
  * For any [[HashBuilder]] hb, it is computationally infeasible to find two sequences `as1` and
  * `as2` of calls to `add` such that the concatenation of `as1` differs from the concatenation
  * `as2`, yet their computed hashes are the same, i.e., `as1.foldLeft(hb)((hb, a) =>
  * hb.add(a)).finish` is the same as `as2.foldLeft(hb)((hb, a) => hb.add(a)).finish`.
  */
trait HashBuilder {

  /** Appends a [[com.google.protobuf.ByteString]] `a` to the sequence of bytes to be hashed. Use
    * `add` for [[com.google.protobuf.ByteString]]s of variable length to prevent hash collisions
    * due to concatenation of variable-length strings.
    *
    * Document at the call site in production code why it is not necessary to include a length
    * prefix.
    *
    * @return
    *   the updated hash builder
    * @throws java.lang.IllegalStateException
    *   if the [[finish]] method has already been called on this [[HashBuilder]]
    */
  def addWithoutLengthPrefix(a: ByteString): this.type

  /** Same as addWithoutLengthPrefix but takes an additional context argument. The context is used
    * by a [[com.digitalasset.canton.protocol.hash.HashTracer]] to trace hashing steps.
    */
  def addWithoutLengthPrefixWithContext(a: ByteString, context: => String): this.type

  /** Appends the length of `a` (encoded as fixed length [[com.google.protobuf.ByteString]]) as well
    * as `a` to this builder.
    *
    * @return
    *   the updated hash builder
    * @throws java.lang.IllegalStateException
    *   if the [[finish]] method has already been called on this [[HashBuilder]]
    */
  def add(a: ByteString): this.type =
    add(a.size).addWithoutLengthPrefix(a)

  /** Same as add but with an additional context. The context is used by a
    * [[com.digitalasset.canton.protocol.hash.HashTracer]] to trace hashing steps.
    */
  def add(a: ByteString, context: => String): this.type =
    add(a.size).addWithoutLengthPrefixWithContext(a, context)

  /** Shorthand for `addWithoutLengthPrefix(ByteString.copyFrom(a))` */
  def addWithoutLengthPrefix(a: Array[Byte]): this.type = addWithoutLengthPrefix(
    ByteString.copyFrom(a)
  )

  /** Shorthand for `addWithoutLengthPrefix(ByteString.copyFromUtf8(a))` Use `add` for strings of
    * variable length to prevent hash collisions due to concatenation of variable-length strings.
    *
    * Document at the call site in production code why it is not necessary to include a length
    * prefix.
    */
  def addWithoutLengthPrefix(a: String): this.type = addWithoutLengthPrefix(
    ByteString.copyFromUtf8(a)
  )

  /** Shorthand for `add(ByteString.copyFromUtf8(a))` */
  def add(a: String): this.type = add(ByteString.copyFromUtf8(a), s"$a (string)")

  /** Shorthand for `addWithoutLengthPrefix(DeterministicEncoding.encodeInt(a))` */
  def add(a: Int): this.type =
    addWithoutLengthPrefixWithContext(DeterministicEncoding.encodeInt(a), s"$a (int)")

  /** Shorthand for `addWithoutLengthPrefix(DeterministicEncoding.encodeLong(a))` */
  def add(a: Long): this.type =
    addWithoutLengthPrefixWithContext(DeterministicEncoding.encodeLong(a), s"$a (long)")

  /** Terminates the building of the hash. No more additions can be made using
    * `HashBuilder.addWithoutLengthPrefix` after this method has been called.
    *
    * @return
    *   The hash of the array accumulated so far.
    * @throws java.lang.IllegalStateException
    *   if [[finish]] had been called before on this [[HashBuilder]]
    */
  def finish(): Hash
}

object HashBuilderFromMessageDigest {
  // The apply method to make sure the
  // purpose is always added first thing, while allowing subclasses to not automatically add the purpose.
  // This is useful when building transaction hashes for example which involve instantiating multiple builders
  // and where repeating the purpose in the encoding would be redundant
  def apply(algorithm: HashAlgorithm, purpose: HashPurpose): HashBuilderFromMessageDigest =
    new HashBuilderFromMessageDigest(algorithm, purpose).addPurpose
}

/** Constructs a [[HashBuilder]] from the specified [[java.security.MessageDigest]] ALWAYS use the
  * apply method unless you know what you're doing.
  */
class HashBuilderFromMessageDigest private[crypto] (algorithm: HashAlgorithm, purpose: HashPurpose)
    extends HashBuilder {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var finished: Boolean = false

  private val md: MessageDigest = MessageDigest.getInstance(algorithm.name)
  protected lazy val purposeByteArray: Array[Byte] =
    ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(purpose.id).array()

  private[canton] def addPurpose: this.type = {
    md.update(purposeByteArray)
    this
  }

  def addByte(byte: Byte, context: => String): this.type = {
    md.update(byte)
    this
  }

  override def addWithoutLengthPrefix(a: Array[Byte]): this.type = {
    assertNotFinished()
    md.update(a)
    this
  }

  override def addWithoutLengthPrefix(a: ByteString): this.type = addWithoutLengthPrefix(
    a.toByteArray
  )

  override def addWithoutLengthPrefixWithContext(a: ByteString, context: => String): this.type =
    addWithoutLengthPrefix(a)

  override def finish(): Hash = {
    assertNotFinished()
    finished = true
    val hash = ByteString.copyFrom(md.digest)
    checked(Hash.tryCreate(hash, algorithm))
  }

  private def assertNotFinished(): Unit =
    if (finished)
      throw new IllegalStateException(s"HashBuilder for $purpose has already been finalized.")
}

private[canton] class PrimitiveHashBuilder(purpose: HashPurpose, hashTracer: HashTracer)
    extends HashBuilderFromMessageDigest(Sha256, purpose) {

  override private[canton] def addPurpose: this.type = {
    hashTracer.traceByteArray(purposeByteArray, "Hash Purpose")
    super.addPurpose
  }

  override def addByte(byte: Byte, context: => String): this.type = {
    hashTracer.traceByte(byte, context)
    super.addByte(byte, context)
  }

  override def addWithoutLengthPrefixWithContext(a: ByteString, context: => String): this.type = {
    hashTracer.traceByteString(a, context)
    super.addWithoutLengthPrefix(a)
  }

  /* no size delimitation as hashes have fixed size  */
  final def addHash(a: Hash, context: => String): this.type = {
    addWithoutLengthPrefixWithContext(a.unwrap, context)
    this
  }

  /* no size delimitation as hashes have fixed size  */
  final def addLfHash(a: LfHash, context: => String): this.type = {
    addWithoutLengthPrefixWithContext(a.bytes.toByteString, context)
    this
  }

  final def addBool(b: Boolean): this.type =
    addByte(if (b) 1.toByte else 0.toByte, s"${b.toString} (bool)")

  final def addNumeric(v: data.Numeric): this.type =
    add(ByteString.copyFromUtf8(data.Numeric.toString(v)), s"${data.Numeric.toString(v)} (numeric)")

  final def iterateOver[T, U](a: ImmArray[T])(f: (this.type, T) => this.type): this.type =
    a.foldLeft[this.type](add(a.length))(f)

  final def iterateOver[T](a: Iterator[T], size: Int)(f: (this.type, T) => this.type): this.type =
    a.foldLeft[this.type](add(size))(f)

  final def addStringSet[S <: String](set: Set[S]): this.type = {
    val ss = set.toSeq.sorted[String]
    iterateOver(ss.iterator, ss.size)(_ add _)
  }

  final def addOptional[S](opt: Option[S], hashS: this.type => S => this.type): this.type =
    opt match {
      case None => addByte(0.toByte, "None")
      case Some(value) => hashS(addByte(1.toByte, "Some"))(value)
    }

  def addContext(context: => String): this.type =
    withContext(context)(identity)

  def withContext(context: => String)(f: this.type => this.type): this.type = {
    hashTracer.context(s"# $context")
    f(this)
  }
}
