// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.checked
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.protocol.hash.HashTracer.NoOp
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
    * `addByteString` for [[com.google.protobuf.ByteString]]s of variable length to prevent hash
    * collisions due to concatenation of variable-length strings.
    *
    * Document at the call site in production code why it is not necessary to include a length
    * prefix.
    *
    * @return
    *   the updated [[HashBuilder]]
    * @throws java.lang.IllegalStateException
    *   if the [[finish]] method has already been called on this [[HashBuilder]]
    */
  def addWithoutLengthPrefix(a: ByteString): this.type

  /** Same as `addWithoutLengthPrefix(a: ByteString)` but takes an additional context argument. The
    * context is currently used by a [[com.digitalasset.canton.protocol.hash.HashTracer]] to trace
    * hashing steps.
    */
  def addWithoutLengthPrefixWithContext(a: ByteString, context: => String): this.type

  /** Shorthand for `addWithoutLengthPrefix(ByteString.copyFromUtf8(a))`. Use [[addString]] for
    * strings of variable length to prevent hash collisions due to concatenation of variable-length
    * strings.
    *
    * Document at the call site in production code why it is not necessary to include a length
    * prefix.
    */
  def addWithoutLengthPrefix(a: String): this.type = addWithoutLengthPrefix(
    ByteString.copyFromUtf8(a)
  )

  /** Appends the length of `a` (encoded as fixed length [[com.google.protobuf.ByteString]]) as well
    * as `a` to this builder.
    */
  def addByteString(a: ByteString): this.type =
    addInt(a.size).addWithoutLengthPrefix(a)

  /** Same as `addByteString` but with an additional context. */
  def addByteString(a: ByteString, context: => String): this.type =
    addInt(a.size).addWithoutLengthPrefixWithContext(a, context)

  /** Shorthand for `addByteString(ByteString.copyFromUtf8(a))` */
  def addString(a: String): this.type = addByteString(ByteString.copyFromUtf8(a), s"$a (string)")

  /** Shorthand for `addWithoutLengthPrefix(ByteString.copyFrom(Array(a)))`. Takes an additional
    * context argument. The context is currently used by a
    * [[com.digitalasset.canton.protocol.hash.HashTracer]] to trace hashing steps.
    */
  def addByte(a: Byte, context: Byte => String): this.type =
    addWithoutLengthPrefixWithContext(ByteString.copyFrom(Array(a)), context(a))

  /** Shorthand for `addWithoutLengthPrefix(DeterministicEncoding.encodeInt(a))` */
  def addInt(a: Int): this.type =
    addWithoutLengthPrefixWithContext(DeterministicEncoding.encodeInt(a), s"$a (int)")

  /** Shorthand for `addWithoutLengthPrefix(DeterministicEncoding.encodeLong(a))` */
  def addLong(a: Long): this.type =
    addWithoutLengthPrefixWithContext(DeterministicEncoding.encodeLong(a), s"$a (long)")

  /** No size delimitation as hashes have fixed size. */
  final def addHash(a: Hash, context: => String): this.type = {
    addWithoutLengthPrefixWithContext(a.unwrap, context)
    this
  }

  /** No size delimitation as hashes have fixed size. */
  final def addLfHash(a: LfHash, context: => String): this.type = {
    addWithoutLengthPrefixWithContext(a.bytes.toByteString, context)
    this
  }

  /** Shorthand for boolean encoded as a single byte (0 or 1). */
  final def addBool(b: Boolean): this.type =
    addByte(if (b) 1.toByte else 0.toByte, _ => s"${b.toString} (bool)")

  /** Hash `Numeric` by hashing its string representation. */
  final def addNumeric(v: data.Numeric): this.type =
    addByteString(
      ByteString.copyFromUtf8(data.Numeric.toString(v)),
      s"${data.Numeric.toString(v)} (numeric)",
    )

  /** Hash an array by first prefixing it with its length, then hashing each element in order. */
  final def addArray[T, U](a: ImmArray[T])(f: (this.type, T) => this.type): this.type =
    a.foldLeft[this.type](addInt(a.length))(f)

  /** Hash an iterator by prefixing it with the expected size, then hashing each element in order.
    * Verifies at the end that the actual number of elements matches the expected size.
    */
  final def addIterator[T](a: Iterator[T], size: Int)(f: (this.type, T) => this.type): this.type = {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var count = 0
    val hash = a.foldLeft[this.type](addInt(size)) { (hb, elem) =>
      count += 1
      f(hb, elem)
    }
    require(count == size, s"Iterator size mismatch: expected $size, counted $count")
    hash
  }

  /** Add a set of strings by sorting it, prefixing with the set length, and hashing each element.
    */
  final def addStringSet[S <: String](set: Set[S]): this.type = {
    val ss = set.toSeq.sorted[String]
    addIterator(ss.iterator, ss.size)(_ addString _)
  }

  /** Add an optional value by prefixing with a tag byte (0 = None, 1 = Some). */
  final def addOptional[S](opt: Option[S], hashS: this.type => S => this.type): this.type =
    opt match {
      case None => addByte(0.toByte, _ => "None")
      case Some(value) => hashS(addByte(1.toByte, _ => "Some"))(value)
    }

  /** Terminates the building of the hash. No more additions can be made using
    * `addWithoutLengthPrefix` and `addWithoutLengthPrefixWithContext` after this method has been
    * called.
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
  def apply(
      algorithm: HashAlgorithm,
      purpose: HashPurpose,
      hashTracer: HashTracer = NoOp,
  ): HashBuilderFromMessageDigest =
    new HashBuilderFromMessageDigest(algorithm, purpose, hashTracer).addPurpose()
}

/** Constructs a [[HashBuilder]] from the specified [[java.security.MessageDigest]] ALWAYS use the
  * apply method unless you know what you're doing.
  */
class HashBuilderFromMessageDigest private[canton] (
    algorithm: HashAlgorithm,
    purpose: HashPurpose,
    hashTracer: HashTracer = NoOp,
) extends HashBuilder {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var finished: Boolean = false

  private val md: MessageDigest = MessageDigest.getInstance(algorithm.name)
  protected lazy val purposeByteArray: Array[Byte] =
    ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(purpose.id).array()

  private[canton] def addPurpose(): this.type = {
    hashTracer.traceByteArray(purposeByteArray, "Hash Purpose")
    md.update(purposeByteArray)
    this
  }

  override def addWithoutLengthPrefix(a: ByteString): this.type = {
    assertNotFinished()
    md.update(a.toByteArray)
    this
  }

  override def addWithoutLengthPrefixWithContext(a: ByteString, context: => String): this.type = {
    hashTracer.traceByteString(a, context)
    addWithoutLengthPrefix(a)
  }

  override def addByte(a: Byte, context: Byte => String): this.type = {
    hashTracer.traceByte(a, context(a))
    md.update(a)
    this
  }

  override def finish(): Hash = {
    assertNotFinished()
    finished = true
    val hash = ByteString.copyFrom(md.digest)
    checked(Hash.tryCreate(hash, algorithm))
  }

  private def assertNotFinished(): Unit =
    if (finished)
      throw new IllegalStateException(s"HashBuilder for $purpose has already been finalized.")

  def addContext(context: => String): this.type =
    withContext(context)(identity)

  def withContext(context: => String)(f: this.type => this.type): this.type = {
    hashTracer.context(s"# $context")
    f(this)
  }
}
