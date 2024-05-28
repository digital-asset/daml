// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.google.protobuf.ByteString

/** Trait for classes that can be serialized to a [[com.google.protobuf.ByteString]].
  *
  * Typically, these classes also implement the [[HasProtocolVersionedWrapper]] trait.
  * Such classes embed logic together with a representative protocol version that determines the ProtoBuf
  * serialization and deserialization.
  * Hence, [[HasToByteString.toByteString]] does not take any arguments.
  * In contrast, [[HasVersionedToByteString]] is tailored towards another
  * ProtoBuf serialization/deserialization tooling.
  *
  * See "README.md" for our guidelines on the (de-)serialization tooling.
  */
trait HasToByteString {

  def toByteString: ByteString

}
