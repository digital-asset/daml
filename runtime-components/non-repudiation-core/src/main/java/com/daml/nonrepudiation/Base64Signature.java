// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation;

import com.google.common.io.BaseEncoding;

import java.security.*;

public final class Base64Signature {

    private Base64Signature() {
    }

    public static String sign(String algorithm, PrivateKey key, byte[] payload) {
        try {
            Signature signature = Signature.getInstance(algorithm);
            signature.initSign(key);
            signature.update(payload);
            byte[] signatureBytes = signature.sign();
            return BaseEncoding.base64().encode(signatureBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(String.format("Provider for algorithm '%s' not found", algorithm), e);
        } catch (InvalidKeyException e) {
            throw new IllegalArgumentException("The signing key is invalid", e);
        } catch (SignatureException e) {
            throw new IllegalArgumentException("The payload could not be signed", e);
        }
    }

}
