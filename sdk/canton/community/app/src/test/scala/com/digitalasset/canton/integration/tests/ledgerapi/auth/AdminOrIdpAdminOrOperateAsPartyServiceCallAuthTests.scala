// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

trait AdminOrIdpAdminOrOperateAsPartyServiceCallAuthTests
    extends ReadOnlyServiceCallAuthTests
    with AdminOrIDPAdminServiceCallAuthTests {
  override def denyAdmin: Boolean =
    false; // Disable tests that deny admin in ReadOnlyServiceCallAuthTests
  override def denyReadAsAny: Boolean =
    false; // Disable tests that deny read-as-any in AdminOrIDPAdminServiceCallAuthTests
}
