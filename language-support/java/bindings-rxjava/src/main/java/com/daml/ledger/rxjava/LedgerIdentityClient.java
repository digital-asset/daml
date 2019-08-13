// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import io.reactivex.Single;

/**
 * An RxJava version of {@link com.digitalasset.ledger.api.v1.LedgerIdentityServiceGrpc}
 */
public interface LedgerIdentityClient {

    Single<String> getLedgerIdentity();
}
