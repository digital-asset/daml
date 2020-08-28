// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

class InvalidExportVersionException(expected: String, actual: String)
    extends RuntimeException(s"Invalid export version. Expected '$expected', but was '$actual'.")
