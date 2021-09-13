// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import com.daml.ledger.api.tls.TlsVersion
import com.daml.platform.sandbox.BaseTlsServerIT

class Tls1_2EnabledServerIT extends BaseTlsServerIT(Some(TlsVersion.V1_2)) with SandboxNextFixture

class Tls1_3EnabledServerIT extends BaseTlsServerIT(Some(TlsVersion.V1_3)) with SandboxNextFixture
