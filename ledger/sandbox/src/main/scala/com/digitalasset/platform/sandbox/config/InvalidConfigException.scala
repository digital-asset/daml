// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.config

import com.digitalasset.resources.ProgramResource.StartupException

class InvalidConfigException(message: String)
    extends RuntimeException(message)
    with StartupException
