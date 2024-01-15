// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation;

import java.security.*;

public final class Signatures {

  private Signatures() {}

  public static byte[] sign(String algorithm, PrivateKey key, byte[] payload) {
    try {
      Signature signature = Signature.getInstance(algorithm);
      signature.initSign(key);
      signature.update(payload);
      return signature.sign();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(
          String.format("Provider for algorithm '%s' not found", algorithm), e);
    } catch (InvalidKeyException e) {
      throw new IllegalArgumentException("The signing key is invalid", e);
    } catch (SignatureException e) {
      throw new IllegalArgumentException("The payload could not be signed", e);
    }
  }
}
