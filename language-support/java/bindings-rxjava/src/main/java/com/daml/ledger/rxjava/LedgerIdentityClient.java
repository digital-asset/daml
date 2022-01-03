// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import io.reactivex.Single;

/** An RxJava version of {@link com.daml.ledger.api.v1.LedgerIdentityServiceGrpc} */
public interface LedgerIdentityClient {

  Single<String> getLedgerIdentity();

  Single<String> getLedgerIdentity(String accessToken);
}
