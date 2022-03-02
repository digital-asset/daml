// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.resources.ProgramResource.SuppressedStartupException

private[common] final class ConfigParseException
    extends RuntimeException
    with SuppressedStartupException
