// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;

public final class Fingerprints {

  private Fingerprints() {}

  private static final String ALGORITHM = "SHA-256";

  public static byte[] compute(X509Certificate certificate) {
    try {
      MessageDigest md = MessageDigest.getInstance(ALGORITHM);
      byte[] encodedCertificate = certificate.getEncoded();
      return md.digest(encodedCertificate);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(
          String.format("Provider for algorithm '%s' not found", ALGORITHM), e);
    } catch (CertificateEncodingException e) {
      throw new IllegalArgumentException("Unable to encode the certificate", e);
    }
  }
}
