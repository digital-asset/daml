// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.daml.ledger.api.v1.LedgerIdentityServiceGrpc;
import com.daml.ledger.api.v1.LedgerIdentityServiceOuterClass;
import com.daml.ledger.rxjava.LedgerIdentityClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import io.grpc.Channel;
import io.reactivex.Single;
import java.time.Duration;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

/** @deprecated ledger identity string is optional for all Ledger API requests since Daml 2.0.0 */
@Deprecated
public class LedgerIdentityClientImpl implements LedgerIdentityClient {

  private LedgerIdentityServiceGrpc.LedgerIdentityServiceFutureStub serviceStub;
  private final Optional<Duration> timeout;

  /**
   * @deprecated Pass a timeout or {@code Optional.empty()} as the third argument, since Daml 2.4.0
   */
  @Deprecated
  public LedgerIdentityClientImpl(Channel channel, Optional<String> accessToken) {
    this(channel, accessToken, Optional.empty());
  }

  public LedgerIdentityClientImpl(
      Channel channel, Optional<String> accessToken, Optional<Duration> timeout) {
    this.serviceStub =
        StubHelper.authenticating(LedgerIdentityServiceGrpc.newFutureStub(channel), accessToken);
    this.timeout = timeout;
  }

  private Single<String> getLedgerIdentity(@NonNull Optional<String> accessToken) {
    this.serviceStub =
        this.timeout
            .map(t -> this.serviceStub.withDeadlineAfter(t.toMillis(), MILLISECONDS))
            .orElse(this.serviceStub);

    return Single.fromFuture(
            StubHelper.authenticating(this.serviceStub, accessToken)
                .getLedgerIdentity(
                    LedgerIdentityServiceOuterClass.GetLedgerIdentityRequest.getDefaultInstance()))
        .map(LedgerIdentityServiceOuterClass.GetLedgerIdentityResponse::getLedgerId);
  }

  @Override
  public Single<String> getLedgerIdentity() {
    return getLedgerIdentity(Optional.empty());
  }

  @Override
  public Single<String> getLedgerIdentity(@NonNull String accessToken) {
    return getLedgerIdentity(Optional.of(accessToken));
  }
}
