// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.PublicKey

object KeyRepository {

  trait Read {
    def get(fingerprint: FingerprintBytes): Option[PublicKey]
  }

  trait Write {
    def put(key: PublicKey): FingerprintBytes
  }

}

trait KeyRepository extends KeyRepository.Read with KeyRepository.Write
