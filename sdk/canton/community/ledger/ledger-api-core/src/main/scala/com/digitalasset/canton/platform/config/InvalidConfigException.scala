// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.daml.resources.ProgramResource.StartupException

class InvalidConfigException(message: String)
    extends RuntimeException(message)
    with StartupException
