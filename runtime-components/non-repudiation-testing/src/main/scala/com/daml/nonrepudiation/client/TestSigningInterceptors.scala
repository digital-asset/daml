// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.client

import java.security.PrivateKey
import java.security.cert.X509Certificate

object TestSigningInterceptors {

  private[nonrepudiation] def signEverything(
      key: PrivateKey,
      certificate: X509Certificate,
  ): SigningInterceptor =
    new SigningInterceptor(key, certificate, _ => true)

}
