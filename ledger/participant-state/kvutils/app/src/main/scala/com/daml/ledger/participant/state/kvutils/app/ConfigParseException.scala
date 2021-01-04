// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import com.daml.resources.ProgramResource.SuppressedStartupException

private[kvutils] final class ConfigParseException
    extends RuntimeException
    with SuppressedStartupException
