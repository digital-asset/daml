// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.client;

import com.daml.ledger.api.v1.CommandServiceGrpc;
import com.daml.ledger.api.v1.CommandSubmissionServiceGrpc;
import com.daml.nonrepudiation.Fingerprints;
import com.daml.nonrepudiation.Headers;
import com.daml.nonrepudiation.Signatures;
import io.grpc.*;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A gRPC client-side interceptor that uses a key pair to sign a payload and adds it as metadata to
 * the call, alongside a fingerprint of the public key and the algorithm used to sign.
 */
public final class SigningInterceptor implements ClientInterceptor {

  private final PrivateKey key;
  private final byte[] fingerprint;
  private final String algorithm;
  private final Predicate<MethodDescriptor<?, ?>> signingPredicate;

  private static final Set<String> commandIssuingServices =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  CommandServiceGrpc.SERVICE_NAME, CommandSubmissionServiceGrpc.SERVICE_NAME)));

  public static SigningInterceptor signCommands(PrivateKey key, X509Certificate certificate) {
    return new SigningInterceptor(
        key, certificate, method -> commandIssuingServices.contains(method.getServiceName()));
  }

  // This is package private as it's not intended for general use and exists for testing
  // exclusively.
  SigningInterceptor(
      PrivateKey key,
      X509Certificate certificate,
      Predicate<MethodDescriptor<?, ?>> signingPredicate) {
    super();
    this.key = key;
    this.algorithm = certificate.getSigAlgName();
    this.fingerprint = Fingerprints.compute(certificate);
    this.signingPredicate = signingPredicate;
  }

  @Override
  public final <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
    if (signingPredicate.test(method)) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(call) {
        private Listener<RespT> responseListener = null;
        private Metadata headers = null;
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
          byte[] requestBytes =
              ByteMarshaller.INSTANCE.parse(method.getRequestMarshaller().stream(request));
          byte[] signature = Signatures.sign(algorithm, key, requestBytes);
          headers.put(Headers.SIGNATURE, signature);
          headers.put(Headers.ALGORITHM, algorithm);
          headers.put(Headers.FINGERPRINT, fingerprint);
          delegate().start(responseListener, headers);
          delegate().request(requested);
          delegate().sendMessage(request);
        }
      };
    } else {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(call) {};
    }
  }
}
