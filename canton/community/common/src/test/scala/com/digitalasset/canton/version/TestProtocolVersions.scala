// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

// Provides protocol versions for testing which may be invalid and unsupported; and thus intentionally located
// within the version package so that ProtocolVersion internal functionality is accessible.
object CommunityTestProtocolVersions {

  /** An old, unsupported protocol version.
    */
  val DeletedPv: ProtocolVersion = ProtocolVersion(5)
}
