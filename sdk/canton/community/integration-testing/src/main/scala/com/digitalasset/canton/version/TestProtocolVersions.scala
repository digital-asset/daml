// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

// Provides protocol versions for testing which may be invalid and unsupported; and thus intentionally located
// within the version package so that ProtocolVersion internal functionality is accessible.
object TestProtocolVersions {

  /** An invalid, unsupported protocol version.
    */
  val UnsupportedPV: ProtocolVersion = ProtocolVersion(0)

  /** A valid, supported protocol version that is not part of the released protocol versions.
    */
  val UnreleasedValidPV: ProtocolVersion = ProtocolVersion.alpha.head
}
