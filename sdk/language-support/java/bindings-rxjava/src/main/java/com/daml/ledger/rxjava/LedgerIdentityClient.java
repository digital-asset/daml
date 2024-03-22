// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import io.reactivex.Single;

// TODO #15208 remove
/**
 * An RxJava version of {@link com.daml.ledger.api.v1.LedgerIdentityServiceGrpc}
 *
 * @deprecated Ledger identity string is optional for all ledger API requests, since Daml 2.0.0
 */
@Deprecated
public interface LedgerIdentityClient {

  Single<String> getLedgerIdentity();

  Single<String> getLedgerIdentity(String accessToken);
}
