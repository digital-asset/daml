// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.crypto.*

import java.util.UUID

/** Creates seeds and UUIDs for requests. */
class SeedGenerator(randomOps: RandomOps) {

  def generateUuid(): UUID = UUID.randomUUID()

  def generateSaltSeed(length: Int = SaltSeed.defaultLength): SaltSeed =
    SaltSeed.generate(length)(randomOps)
}
