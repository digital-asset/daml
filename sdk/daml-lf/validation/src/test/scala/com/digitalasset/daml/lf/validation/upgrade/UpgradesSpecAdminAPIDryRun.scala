// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation
package upgrade

class UpgradesSpecAdminAPIDryRun
    extends UpgradesSpecAdminAPI("Admin API with dry run")
    with LongTests {
  override val uploadSecondPackageDryRun = true;
}
