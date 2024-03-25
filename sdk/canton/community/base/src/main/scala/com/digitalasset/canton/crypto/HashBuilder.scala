// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.checked
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.google.protobuf.ByteString

import java.nio.ByteBuffer
import java.security.MessageDigest

/** The methods of [[HashBuilder]] change its internal state and return `this` for convenience.
  *
  * Requirements for all implementations:
  *
  * For any [[HashBuilder]] hb,
  * it is computationally infeasible to find two sequences `as1` and `as2` of calls to `add`
  * such that the concatenation of `as1` differs from the concatenation `as2`,
  * yet their computed hashes are the same, i.e.,
  * `as1.foldLeft(hb)((hb, a) => hb.add(a)).finish` is the same as `as2.foldLeft(hb)((hb, a) => hb.add(a)).finish`.
  */
trait HashBuilder {

  /** Appends a [[com.google.protobuf.ByteString]] `a` to the sequence of bytes to be hashed.
    * Use `add` for [[com.google.protobuf.ByteString]]s of variable length
    * to prevent hash collisions due to concatenation of variable-length strings.
    *
    * Document at the call site in production code why it is not necessary to include a length prefix.
    *
    * @return the updated hash builder
    * @throws java.lang.IllegalStateException if the [[finish]] method has already been called on this [[HashBuilder]]
    */
  def addWithoutLengthPrefix(a: ByteString): this.type

  /** Appends the length of `a` (encoded as fixed length [[com.google.protobuf.ByteString]]) as well as `a` to this builder.
    *
    * @return the updated hash builder
    * @throws java.lang.IllegalStateException if the [[finish]] method has already been called on this [[HashBuilder]]
    */
  def add(a: ByteString): this.type = add(a.size).addWithoutLengthPrefix(a)

  /** Shorthand for `addWithoutLengthPrefix(ByteString.copyFrom(a))` */
  def addWithoutLengthPrefix(a: Array[Byte]): this.type = addWithoutLengthPrefix(
    ByteString.copyFrom(a)
  )

  /** Shorthand for `addWithoutLengthPrefix(ByteString.copyFromUtf8(a))`
    * Use `add` for strings of variable length
    * to prevent hash collisions due to concatenation of variable-length strings.
    *
    * Document at the call site in production code why it is not necessary to include a length prefix.
    */
  def addWithoutLengthPrefix(a: String): this.type = addWithoutLengthPrefix(
    ByteString.copyFromUtf8(a)
  )

  /** Shorthand for `add(ByteString.copyFromUtf8(a))` */
  def add(a: String): this.type = add(ByteString.copyFromUtf8(a))

  /** Shorthand for `addWithoutLengthPrefix(DeterministicEncoding.encodeInt(a))` */
  def add(a: Int): this.type = addWithoutLengthPrefix(DeterministicEncoding.encodeInt(a))

  /** Shorthand for `addWithoutLengthPrefix(DeterministicEncoding.encodeLong(a))` */
  def add(a: Long): this.type = addWithoutLengthPrefix(DeterministicEncoding.encodeLong(a))

  /** Terminates the building of the hash.
    * No more additions can be made using `HashBuilder.addWithoutLengthPrefix` after this method has been called.
    *
    * @return The hash of the array accumulated so far.
    * @throws java.lang.IllegalStateException if [[finish]] had been called before on this [[HashBuilder]]
    */
  def finish(): Hash
}

/** Constructs a [[HashBuilder]] from the specified [[MessageDigest]]
  */
private[crypto] class HashBuilderFromMessageDigest(algorithm: HashAlgorithm, purpose: HashPurpose)
    extends HashBuilder {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var finished: Boolean = false

  private val md: MessageDigest = MessageDigest.getInstance(algorithm.name)

  {
    md.update(ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(purpose.id).array())
  }

  override def addWithoutLengthPrefix(a: Array[Byte]): this.type = {
    assertNotFinished()
    md.update(a)
    this
  }

  override def addWithoutLengthPrefix(a: ByteString): this.type = addWithoutLengthPrefix(
    a.toByteArray
  )

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
