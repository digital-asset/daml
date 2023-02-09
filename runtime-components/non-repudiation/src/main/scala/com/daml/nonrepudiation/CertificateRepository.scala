// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.cert.X509Certificate

import com.daml.metrics.api.MetricHandle.Timer

object CertificateRepository {

  trait Read {

    def get(fingerprint: FingerprintBytes): Option[X509Certificate]

  }

  trait Write {

    /** Must guarantee idempotence. */
    def put(certificate: X509Certificate): FingerprintBytes

  }

  final class Timed(timer: Timer, delegate: Read) extends Read {
    override def get(fingerprint: FingerprintBytes): Option[X509Certificate] =
      timer.time(delegate.get(fingerprint))
  }

}

trait CertificateRepository extends CertificateRepository.Read with CertificateRepository.Write
