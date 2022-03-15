// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

// XXX (#13113 SC) these take about 70s each, with lots of I/O wait; if they stick around
// for a while (rather than deprecating/deleting custom token) might be worth
// splitting into a suite and consequently librifying the above in bazel

final class FailureTestsCustomToken extends FailureTests with HttpServiceUserFixture.CustomToken

final class FailureTestsUserToken extends FailureTests with HttpServiceUserFixture.UserToken
