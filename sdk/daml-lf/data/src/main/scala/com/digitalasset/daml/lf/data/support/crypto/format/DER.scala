// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package support.crypto

import com.daml.scalautil.Statement.discard
import org.bouncycastle.asn1.ASN1Primitive
import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.Security

object DER {
  discard(Security.addProvider(new BouncyCastleProvider))

  def encode(data: ASN1Primitive): Bytes = {
    Bytes.fromByteArray(data.getEncoded())
  }

  def decode(data: Bytes): ASN1Primitive = {
    ASN1Primitive.fromByteArray(data.toByteArray)
  }
}
