// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine.script.ledgerinteraction

import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.ScriptEngine.{TraceLog, WarningLog}

// Ledger clients before implementation is chosen
sealed trait ScriptLedgerClient extends Product with Serializable

final case class GrpcLedgerClient(
    grpcClient: LedgerClient,
    userId: Option[Ref.UserId],
) extends ScriptLedgerClient

object GrpcLedgerClient {}

final case class IdeLedgerClient(
    compiledPackages: PureCompiledPackages,
    traceLog: TraceLog,
    warningLog: WarningLog,
    canceled: () => Boolean,
) extends ScriptLedgerClient
