// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.config

final case class RetryConfig private (intervalMs: Positive[Long], timeoutMs: Positive[Long])
    extends IRetryConfig
