// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation;

import io.grpc.Metadata;

public final class Headers {

    private Headers() {
    }

    public static final Metadata.Key<String> SIGNATURE =
            Metadata.Key.of("signature", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> ALGORITHM =
            Metadata.Key.of("algorithm", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> FINGERPRINT =
            Metadata.Key.of("fingerprint", Metadata.ASCII_STRING_MARSHALLER);

}
