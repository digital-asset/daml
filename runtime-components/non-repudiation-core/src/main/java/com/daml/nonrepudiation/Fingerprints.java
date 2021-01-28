// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;

public final class Fingerprints {

    private Fingerprints() {
    }

    private static final String ALGORITHM = "SHA-256";

    public static byte[] compute(PublicKey key) {
        try {
            MessageDigest md = MessageDigest.getInstance(ALGORITHM);
            byte[] encodedKey = key.getEncoded();
            return md.digest(encodedKey);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(String.format("Provider for algorithm '%s' not found", ALGORITHM), e);
        }
    }

}
