// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.ledger.api.tls.TlsVersion
import com.daml.platform.sandbox.fixture.SandboxFixture

class Tls1_2EnabledServerIT extends BaseTlsServerIT(Some(TlsVersion.V1_2)) with SandboxFixture

class Tls1_3EnabledServerIT extends BaseTlsServerIT(Some(TlsVersion.V1_3)) with SandboxFixture
