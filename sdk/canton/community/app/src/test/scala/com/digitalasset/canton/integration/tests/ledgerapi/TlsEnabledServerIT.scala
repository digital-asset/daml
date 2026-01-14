// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.daml.tls.TlsVersion

class Tls1_2EnabledServerIT extends BaseTlsServerIT(Some(TlsVersion.V1_2))

class Tls1_3EnabledServerIT extends BaseTlsServerIT(Some(TlsVersion.V1_3))
