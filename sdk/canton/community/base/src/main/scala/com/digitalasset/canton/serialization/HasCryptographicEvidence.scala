// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.serialization

import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.HasRepresentativeProtocolVersion
import com.google.protobuf.ByteString

/** Trait for deterministically serializing an object to a [[com.google.protobuf.ByteString]].
  *
  * A class should extend this trait to indicate that the `serialize` method yields the same `ByteString` if invoked
  * several times.
  * Typical use cases of this behavior include:
  * <ul>
  *   <li>Classes that represent a leaf of a Merkle tree.</li>
  *   <li>Classes that represent content that is signed e.g. Hash or SignedProtocolMessageContent.</li>
  *   <li>Classes that are mere wrappers of ByteString (for convenience of the caller) e.g. AuthenticationToken.</li>
  * </ul>
  * '''If a class merely represents content that is transmitted over a network, the class does not need to extend this
  * trait.'''
  *
  * It is strongly recommended to extend this trait by mixing in [[ProtocolVersionedMemoizedEvidence]] or
  * [[MemoizedEvidenceWithFailure]], instead of directly extending this trait.
  *
  * Classes `C` implementing [[HasCryptographicEvidence]] must define a
  * `fromByteString: ByteString => C` method in their companion object
  * that converts a serialization back into an equal object.
  * In particular, `c.fromByteString(byteString).toByteString` must equal `byteString`.
  */
trait HasCryptographicEvidence {

  /** Returns the serialization of the object into a [[com.google.protobuf.ByteString]].
    * In particular, every instance `i` of this trait must equal `fromByteString(i.toByteString)`.
    *
    * <strong>This method must yield the same result if it is invoked several times.</strong>
    */
  def getCryptographicEvidence: ByteString
}

/** Effectively immutable [[HasCryptographicEvidence]] classes can mix in this trait
  * to implement the memoization logic.
  *
  * Use this class if serialization always succeeds.
  *
  * Make sure that `fromByteString(byteString).deserializedFrom` equals `Some(byteString)`.
  *
  * Make sure that every public constructor and apply method yields an instance with `deserializedFrom == None`.
  *
  * @see MemoizedEvidenceWithFailure if serialization may fail
  */
trait ProtocolVersionedMemoizedEvidence
    extends HasCryptographicEvidence
    with HasRepresentativeProtocolVersion {

  /** Returns the [[com.google.protobuf.ByteString]] from which this object has been deserialized, if any.
    * If defined, [[getCryptographicEvidence]] will use this as the serialization.
    */
  def deserializedFrom: Option[ByteString]

  /** Computes the serialization of the object as a [[com.google.protobuf.ByteString]].
    *
    * Must meet the contract of [[getCryptographicEvidence]]
    * except that when called several times, different [[com.google.protobuf.ByteString]]s may be returned.
    */
  protected[this] def toByteStringUnmemoized: ByteString

  final override lazy val getCryptographicEvidence: ByteString = {
    deserializedFrom match {
      case Some(bytes) => bytes
      case None => toByteStringUnmemoized
    }
  }
}

/** Thrown by [[MemoizedEvidenceWithFailure]] classes during object construction if the serialization fails.
  *
  * @param serializationError The error raised during the serialization
  * @tparam E The type of errors that serialization may return
  */
final case class SerializationCheckFailed[+E](serializationError: E) extends Exception

/** Effectively immutable [[HasCryptographicEvidence]] classes can mix in this trait
  * to implement the memoization logic.
  *
  * Use this class if serialization may fail. This mix-in checks whenever an object is constructed that
  * either a serialization is given or that serialization will succeed.
  * It also ensures that no `copy` method is generated for case classes with this mixin.
  *
  * Make sure that `fromByteString(byteString).deserializedFrom` equals `Some(byteString)`.
  *
  * Make sure that every public constructor and apply method yields an instance with `deserializedFrom == None`.
  *
  * @tparam SerializationError The type of serialization errors
  * @see MemoizedEvidence if serialization always succeeds.
  * @throws SerializationCheckFailed if the serialization fails
  */
trait MemoizedEvidenceWithFailure[SerializationError] extends HasCryptographicEvidence with NoCopy {

  /** Returns the [[com.google.protobuf.ByteString]] from which this object has been deserialized, if any.
    * If defined, [[getCryptographicEvidence]] will use this as the serialization.
    */
  protected[this] def deserializedFrom: Option[ByteString]

  /** Computes the serialization of the object as a [[com.google.protobuf.ByteString]] or produces a `SerializationError`
    * if serialization fails.
    *
    * Must meet the contract of [[getCryptographicEvidence]]
    * except that different [[com.google.protobuf.ByteString]]s may be returned when called several times and
    * it may fail at any time.
    */
  protected[this] def toByteStringChecked: Either[SerializationError, ByteString]

  @throws[SerializationCheckFailed[SerializationError]]
  final override val getCryptographicEvidence: ByteString = {
    deserializedFrom match {
      case Some(bytes) => bytes
      case None =>
        toByteStringChecked match {
          case Left(error) => throw new SerializationCheckFailed[SerializationError](error)
          case Right(bytes) => bytes
        }
    }
  }
}

/** Wraps a [[com.google.protobuf.ByteString]] so that it is its own cryptographic evidence. */
final case class BytestringWithCryptographicEvidence(bytes: ByteString)
    extends HasCryptographicEvidence {
  override def getCryptographicEvidence: ByteString = bytes
}
