// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.cert.X509Certificate

object CertificateRepository {

  trait Read {
    def get(fingerprint: FingerprintBytes): Option[X509Certificate]
  }

  trait Write {
    def put(certificate: X509Certificate): FingerprintBytes
  }

}

trait CertificateRepository extends CertificateRepository.Read with CertificateRepository.Write
