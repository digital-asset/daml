// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.daml.error.ValueSwitch
import io.grpc.StatusRuntimeException

final class ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes: Boolean)
    extends ValueSwitch[StatusRuntimeException](enableSelfServiceErrorCodes)
