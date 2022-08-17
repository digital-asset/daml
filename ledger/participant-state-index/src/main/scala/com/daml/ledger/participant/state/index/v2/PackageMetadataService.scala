// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.platform.packagemeta.PackageMetadata

/** Serves as a backend to implement package metadata related API calls.
  */
trait PackageMetadataService {
  def currentPackageMetadata(): PackageMetadata
}
