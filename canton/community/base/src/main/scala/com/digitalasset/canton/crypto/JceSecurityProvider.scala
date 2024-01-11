// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import org.bouncycastle.jce.provider.BouncyCastleProvider

/*
  We use this new bouncyCastle provider to circumvent the ClassLoader issue that is triggered when
  two different runs in the same JVM generate an IESParameterSpec. We use this provider, and not the one
  set in Security.addProvider(), to get the cipher instances for
  encryption/decryption under EciesP256HmacSha256Aes128Cbc.
 */
object JceSecurityProvider {
  lazy val bouncyCastleProvider: BouncyCastleProvider = new BouncyCastleProvider
}
