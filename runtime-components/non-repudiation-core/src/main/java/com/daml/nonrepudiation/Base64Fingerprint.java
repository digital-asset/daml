// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation;

import com.google.common.io.BaseEncoding;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;

public class Base64Fingerprint {

    private Base64Fingerprint() {
    }

    private static final String ALGORITHM = "SHA-256";

    public static String compute(PublicKey key) {
        try {
            MessageDigest md = MessageDigest.getInstance(ALGORITHM);
            byte[] fingerprint = md.digest(key.getEncoded());
            return BaseEncoding.base64().encode(fingerprint);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(String.format("Provider for algorithm '%s' not found", ALGORITHM), e);
        }
    }

}
