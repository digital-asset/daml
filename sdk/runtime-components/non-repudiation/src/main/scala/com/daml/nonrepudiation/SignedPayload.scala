// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.time.Instant

final case class SignedPayload(
    algorithm: AlgorithmString,
    fingerprint: FingerprintBytes,
    payload: PayloadBytes,
    signature: SignatureBytes,
    timestamp: Instant,
)
