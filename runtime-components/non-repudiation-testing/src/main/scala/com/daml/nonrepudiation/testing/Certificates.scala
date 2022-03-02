// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.testing

import java.security.cert.X509Certificate

import com.daml.nonrepudiation.{CertificateRepository, FingerprintBytes}

import scala.collection.concurrent.TrieMap

final class Certificates extends CertificateRepository {

  private val map = TrieMap.empty[FingerprintBytes, X509Certificate]

  override def get(fingerprint: FingerprintBytes): Option[X509Certificate] =
    map.get(fingerprint)

  override def put(certificate: X509Certificate): FingerprintBytes = {
    val fingerprint = FingerprintBytes.compute(certificate)
    map.put(fingerprint, certificate)
    fingerprint
  }
}
