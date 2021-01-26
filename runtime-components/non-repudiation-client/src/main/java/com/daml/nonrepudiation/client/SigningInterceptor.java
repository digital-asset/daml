// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.client;

import com.daml.nonrepudiation.Base64Signature;
import com.daml.nonrepudiation.Headers;
import com.google.common.io.BaseEncoding;
import io.grpc.*;

import java.security.KeyPair;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;

/**
 * A gRPC client-side interceptor that uses a key pair to sign a payload
 * and adds it as metadata to the call, alongside a fingerprint of the
 * public key and the algorithm used to sign.
 */
public final class SigningInterceptor implements ClientInterceptor {

    private final PrivateKey key;
    private final String fingerprint;
    private final String algorithm;

    public SigningInterceptor(KeyPair keyPair, String signingAlgorithm) throws NoSuchAlgorithmException {
        super();
        this.key = keyPair.getPrivate();
        this.algorithm = signingAlgorithm;
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        byte[] publicKeyFingerPrint = sha256.digest(keyPair.getPublic().getEncoded());
        this.fingerprint = BaseEncoding.base64().encode(publicKeyFingerPrint);
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next) {
        ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(call) {
            private Listener<RespT> responseListener = null;
            private Metadata headers = null;
            private boolean started = false;
            private int requested = 0;

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                // Delay start until we have the message body since
                // the signature in the Metadata depends on the body.
                this.responseListener = responseListener;
                this.headers = headers;
            }

            @Override
            public void request(int numMessages) {
                // Delay until we have the message body since the
                // signature in the Metadata depends on the body.
                requested += numMessages;
            }

            @Override
            public void sendMessage(ReqT request) {
                byte[] requestBytes = ByteMarshaller.INSTANCE.parse(method.getRequestMarshaller().stream(request));
                String signature = Base64Signature.sign(algorithm, key, requestBytes);
                headers.put(Headers.SIGNATURE, signature);
                headers.put(Headers.ALGORITHM, algorithm);
                headers.put(Headers.FINGERPRINT, fingerprint);
                if (!started) {
                    delegate().start(responseListener, headers);
                    started = true;
                }
                // I have no idea if the order of `request` and `sendMessage` matters.
                delegate().request(requested);
                delegate().sendMessage(request);
            }
        };
    }
}
