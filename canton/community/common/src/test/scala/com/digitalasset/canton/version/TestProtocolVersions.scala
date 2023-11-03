// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

// Provides protocol versions for testing which may be invalid and unsupported; and thus intentionally located
// within the version package so that ProtocolVersion internal functionality is accessible.
object TestProtocolVersions {
  val UnsupportedPV: ProtocolVersion = ProtocolVersion(0)

  // a protocol version which is supported but is not part of the released protocol versions
  val SomeValidPV: ProtocolVersion = ProtocolVersion.unstable.head
}
