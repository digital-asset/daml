// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.serialization

import com.daml.crypto.MessageDigestPrototype

import java.nio.ByteBuffer
import java.security.MessageDigest
import com.daml.lf.data.{Numeric, Utf8}
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value

/** @deprecated in favor of [[GlobalKey.hash]]
  */
trait KeyHasher {

  /** @deprecated in favor of [[GlobalKey.hash]]
    * Returns the hash of the given Daml-LF value
    */
  def hashKey(key: GlobalKey): Array[Byte]

  /** @deprecated in favor of [[GlobalKey.hash]]
    * Returns a string representation of the hash of the given Daml-LF value
    */
  def hashKeyString(key: GlobalKey): String = hashKey(key).map("%02x" format _).mkString
}

/** @deprecated in favor of [[GlobalKey.hash]]
  */
object KeyHasher extends KeyHasher {

  /** ADT for data elements that appear in the input stream of the hash function
    * used to hash Daml-LF values.
    */
  private sealed abstract class HashToken extends Product with Serializable
  private final case class HashTokenText(value: String) extends HashToken
  private final case class HashTokenByte(value: Byte) extends HashToken
  private final case class HashTokenInt(value: Int) extends HashToken
  private final case class HashTokenLong(value: Long) extends HashToken
  private final case class HashTokenCollectionBegin(length: Int) extends HashToken
  private final case class HashTokenCollectionEnd() extends HashToken

  /** Traverses the given value in a stable way, producing "hash tokens" for any encountered primitive values.
    * These tokens can be used as the input to a hash function.
    *
    * @param value the Daml-LF value to hash
    * @param z initial hash value
    * @param op operation to append a hash token
    * @return the final hash value
    */
  def foldLeft[T](value: Value, z: T, op: (T, HashToken) => T): T = {
    import com.daml.lf.value.Value._

    value match {
      case ValueContractId(v) => op(z, HashTokenText(v.coid))
      case ValueInt64(v) => op(z, HashTokenLong(v))
      case ValueNumeric(v) => op(z, HashTokenText(Numeric.toUnscaledString(v)))
      case ValueText(v) => op(z, HashTokenText(v))
      case ValueTimestamp(v) => op(z, HashTokenLong(v.micros))
      case ValueParty(v) => op(z, HashTokenText(v))
      case ValueBool(v) => op(z, HashTokenByte(if (v) 1.toByte else 0.toByte))
      case ValueDate(v) => op(z, HashTokenInt(v.days))
      case ValueUnit => op(z, HashTokenByte(0))

      // Record: [CollectionBegin(), Token(value)*, CollectionEnd()]
      case ValueRecord(_, fs) =>
        val z1 = op(z, HashTokenCollectionBegin(fs.length))
        val z2 = fs.foldLeft[T](z1)((t, v) => foldLeft(v._2, t, op))
        op(z2, HashTokenCollectionEnd())

      // Optional: [CollectionBegin(), Token(value), CollectionEnd()]
      case ValueOptional(Some(v)) =>
        val z1 = op(z, HashTokenCollectionBegin(1))
        val z2 = foldLeft(v, z1, op)
        op(z2, HashTokenCollectionEnd())

      case ValueOptional(None) =>
        val z1 = op(z, HashTokenCollectionBegin(0))
        op(z1, HashTokenCollectionEnd())

      // Variant: [CollectionBegin(), Text(variant), Token(value), CollectionEnd()]
      case ValueVariant(_, variant, v) =>
        val z1 = op(z, HashTokenCollectionBegin(1))
        val z2 = op(z1, HashTokenText(variant))
        val z3 = foldLeft(v, z2, op)
        op(z3, HashTokenCollectionEnd())

      // Enum: [Text(variant)]
      case ValueEnum(_, value_) =>
        op(z, HashTokenText(value_))

      // List: [CollectionBegin(), Token(value)*, CollectionEnd()]
      case ValueList(xs) =>
        val arr = xs.toImmArray
        val z1 = op(z, HashTokenCollectionBegin(xs.length))
        val z2 = arr.foldLeft[T](z1)((t, v) => foldLeft(v, t, op))
        op(z2, HashTokenCollectionEnd())

      // Map: [CollectionBegin(), (Text(key), Token(value))*, CollectionEnd()]
      case ValueTextMap(xs) =>
        val arr = xs.toImmArray
        val z1 = op(z, HashTokenCollectionBegin(arr.length))
        val z2 = arr.foldLeft[T](z1)((t, v) => {
          val zz1 = op(t, HashTokenText(v._1))
          foldLeft(v._2, zz1, op)
        })
        op(z2, HashTokenCollectionEnd())
      case ValueGenMap(entries) =>
        val z1 = op(z, HashTokenCollectionBegin(entries.length))
        val z2 = entries.foldLeft[T](z1) { case (t, (k, v)) => foldLeft(k, foldLeft(v, t, op), op) }
        op(z2, HashTokenCollectionEnd())
    }
  }

  private[this] def putInt(digest: MessageDigest, value: Int): Unit =
    digest.update(ByteBuffer.allocate(4).putInt(value).array())

  private[this] def putLong(digest: MessageDigest, value: Long): Unit =
    digest.update(ByteBuffer.allocate(8).putLong(value).array())

  private[this] def putString(digest: MessageDigest, value: String): Unit = {
    val bytes = Utf8.getBytes(value)
    putInt(digest, bytes.length)
    digest.update(bytes.toByteBuffer)
  }

  // Do not use directly. It is package visible for testing purpose.
  private[serialization] def putValue(
      digest: MessageDigest,
      value: Value,
  ): MessageDigest = {
    // Then, write the value
    foldLeft[MessageDigest](
      value,
      digest,
      (d, token) => {
        // Append bytes:
        // - Fixed-width values are appended as-is
        // - Variable-width values are prefixed with their length
        // - Collections are prefixed with their size
        token match {
          case HashTokenByte(v) => d.update(v)
          case HashTokenInt(v) => putInt(d, v)
          case HashTokenLong(v) => putLong(d, v)
          case HashTokenText(v) => putString(d, v)
          case HashTokenCollectionBegin(length) => putInt(d, length)
          case HashTokenCollectionEnd() => // no-op
        }
        d
      },
    )
  }

  /** @deprecated in favor of [[GlobalKey.hash]]
    */
  override def hashKey(key: GlobalKey): Array[Byte] = {
    val digest = MessageDigestPrototype.Sha256.newDigest

    // First, write the template ID
    putString(digest, key.templateId.packageId)
    putString(digest, key.templateId.qualifiedName.toString())

    // Note: We do not emit the type as it is implied by the template ID.
    putValue(digest, key.key)

    digest.digest()
  }

}
