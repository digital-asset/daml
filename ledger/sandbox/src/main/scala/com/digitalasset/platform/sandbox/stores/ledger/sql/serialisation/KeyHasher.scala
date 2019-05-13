// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation

import java.nio.ByteBuffer
import java.security.MessageDigest

import com.digitalasset.daml.lf.data.Utf8String
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId

trait KeyHasher {

  /** Returns the hash of the given DAML-LF value */
  def hashKey(key: GlobalKey): Array[Byte]

  /** Returns a string representation of the hash of the given DAML-LF value */
  def hashKeyString(key: GlobalKey): String = hashKey(key).map("%02x" format _).mkString
}

/**
  * ADT for data elements that appear in the input stream of the hash function
  * used to hash DAML-LF values.
  */
sealed abstract class HashToken extends Product with Serializable
final case class HashTokenText(value: Utf8String) extends HashToken
final case class HashTokenByte(value: Byte) extends HashToken
final case class HashTokenInt(value: Int) extends HashToken
final case class HashTokenLong(value: Long) extends HashToken
final case class HashTokenBigDecimal(value: BigDecimal) extends HashToken
final case class HashTokenCollectionBegin(length: Int) extends HashToken
final case class HashTokenCollectionEnd() extends HashToken

object KeyHasher extends KeyHasher {

  /**
    * Traverses the given value in a stable way, producing "hash tokens" for any encountered primitive values.
    * These tokens can be used as the input to a hash function.
    *
    * @param value the DAML-LF value to hash
    * @param z initial hash value
    * @param op operation to append a hash token
    * @return the final hash value
    */
  def foldLeft[T](value: Value[AbsoluteContractId], z: T, op: (T, HashToken) => T): T = {
    import com.digitalasset.daml.lf.value.Value._

    value match {
      case ValueContractId(v) => op(z, HashTokenText(Utf8String(v.coid)))
      case ValueInt64(v) => op(z, HashTokenLong(v))
      case ValueDecimal(v) => op(z, HashTokenBigDecimal(v))
      case ValueText(v) => op(z, HashTokenText(v))
      case ValueTimestamp(v) => op(z, HashTokenLong(v.micros))
      case ValueParty(v) => op(z, HashTokenText(Utf8String(v)))
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
        val z2 = op(z1, HashTokenText(Utf8String(variant)))
        val z3 = foldLeft(v, z2, op)
        op(z3, HashTokenCollectionEnd())

      // List: [CollectionBegin(), Token(value)*, CollectionEnd()]
      case ValueList(xs) =>
        val arr = xs.toImmArray
        val z1 = op(z, HashTokenCollectionBegin(xs.length))
        val z2 = arr.foldLeft[T](z1)((t, v) => foldLeft(v, t, op))
        op(z2, HashTokenCollectionEnd())

      // Map: [CollectionBegin(), (Text(key), Token(value))*, CollectionEnd()]
      case ValueMap(xs) =>
        val arr = xs.toImmArray
        val z1 = op(z, HashTokenCollectionBegin(arr.length))
        val z2 = arr.foldLeft[T](z1)((t, v) => {
          val zz1 = op(t, HashTokenText(v._1))
          foldLeft(v._2, zz1, op)
        })
        op(z2, HashTokenCollectionEnd())

      // Tuple: should never be encountered
      case ValueTuple(xs) =>
        sys.error("Hashing of tuple values is not supported")
    }
  }

  private[this] def putInt(digest: MessageDigest, value: Int): Unit =
    digest.update(ByteBuffer.allocate(4).putInt(value).array())

  private[this] def putLong(digest: MessageDigest, value: Long): Unit =
    digest.update(ByteBuffer.allocate(8).putLong(value).array())

  private[this] def putStringContent(digest: MessageDigest, value: Utf8String): Unit =
    digest.update(value.getBytes)

  private[this] def putUtf8String(digest: MessageDigest, value: Utf8String): Unit = {
    // FixMe we probably should not use UTF16 length.
    putInt(digest, value.javaString.length)
    putStringContent(digest, value)
  }

  override def hashKey(key: GlobalKey): Array[Byte] = {
    val digest = MessageDigest.getInstance("SHA-256")

    // First, write the template ID
    putUtf8String(digest, Utf8String(key.templateId.packageId))
    putUtf8String(digest, Utf8String(key.templateId.qualifiedName.toString()))

    // Note: We do not emit the value or language version, as both are
    // implied by the template ID.

    // Then, write the value
    foldLeft[MessageDigest](
      key.key.value,
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
          case HashTokenText(v) => putUtf8String(d, v)
          // FixMe we probably should use Decimal.toString
          // Java docs: "The toString() method provides a canonical representation of a BigDecimal."
          case HashTokenBigDecimal(v) => putUtf8String(d, Utf8String(v.toString))
          case HashTokenCollectionBegin(length) => putInt(d, length)
          case HashTokenCollectionEnd() => // no-op
        }

        // MessageDigest is a mutable object modified above
        d
      }
    ).digest()
  }

}
