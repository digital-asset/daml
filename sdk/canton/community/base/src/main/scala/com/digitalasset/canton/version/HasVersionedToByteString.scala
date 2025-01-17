// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.google.protobuf.ByteString

/** Trait for classes that can be serialized to a [[com.google.protobuf.ByteString]].
  * Typical use cases of toByteString include:
  * <ul>
  *   <li>saving data in the database in serialized form (e.g. as in SequencedEvent)</li>
  *   <li>encrypting data (e.g. as in Encryption.scala) </li>  *
  * </ul>
  * In some exceptional cases, we also convert an object to a ByteString before including it in a Proto message (e.g. ViewCommonData or Envelope)
  *
  * Classes that use Protobuf for serialization should implement [[HasVersionedWrapper]] instead.
  * See "CONTRIBUTING.md" for our guidelines on serialization.
  */
trait HasVersionedToByteString {

  /** Returns the serialization of the object into a [[com.google.protobuf.ByteString]].
    * This method may yield different results if it is invoked several times.
    */
  def toByteString(version: ProtocolVersion): ByteString
}
