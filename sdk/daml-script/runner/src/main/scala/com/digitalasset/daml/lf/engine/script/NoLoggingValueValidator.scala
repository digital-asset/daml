// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import com.daml.ledger.api.v2.{value => api}
import com.digitalasset.daml.lf.value.Value
import io.grpc.StatusRuntimeException
import com.digitalasset.canton.ledger.api.validation.ValueValidator

object NoLoggingValueValidator {

  def validateRecord(rec: api.Record): Either[StatusRuntimeException, Value.ValueRecord] =
    ValueValidator.validateRecord(rec)(com.daml.error.NoLogging)

  def validateValue(v0: api.Value): Either[StatusRuntimeException, Value] =
    ValueValidator.validateValue(v0)(com.daml.error.NoLogging)

}
