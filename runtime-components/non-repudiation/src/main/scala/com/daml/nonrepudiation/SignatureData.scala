// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.PublicKey

final case class SignatureData(
    algorithm: AlgorithmString,
    fingerprint: FingerprintBytes,
    key: PublicKey,
    signature: SignatureBytes,
)
