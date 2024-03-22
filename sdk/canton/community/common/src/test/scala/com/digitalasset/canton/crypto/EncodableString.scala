// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.serialization.{DeterministicEncoding, HasCryptographicEvidence}
import com.google.protobuf.ByteString

final case class EncodableString(string: String) extends HasCryptographicEvidence {
  def encodeDeterministically: ByteString = DeterministicEncoding.encodeString(string)
  override def getCryptographicEvidence: ByteString = encodeDeterministically
}
