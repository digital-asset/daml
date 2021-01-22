// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.client;

import com.google.common.io.BaseEncoding;

import java.security.*;

public final class Base64Signature {

    private Base64Signature() {}

    public static String sign(String algorithm, PrivateKey key, byte[] payload) {
        try {
            Signature signature = Signature.getInstance(algorithm);
            signature.initSign(key);
            signature.update(payload);
            return BaseEncoding.base64().encode(signature.sign());
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            throw new RuntimeException(e);
        }
    }

}
