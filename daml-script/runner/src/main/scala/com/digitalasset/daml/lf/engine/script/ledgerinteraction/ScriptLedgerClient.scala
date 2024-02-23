// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine.script.ledgerinteraction

import com.digitalasset.canton.ledger.client.LedgerClient
import com.daml.lf.data.Ref
import com.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.AdminLedgerClient
import com.daml.lf.speedy.{TraceLog, WarningLog}

// Ledger clients before implementation is chosen
sealed trait ScriptLedgerClient extends Product with Serializable

final case class GrpcLedgerClient(
    grpcClient: LedgerClient,
    val applicationId: Option[Ref.ApplicationId],
    val grpcAdminClient: Option[AdminLedgerClient] = None,
) extends ScriptLedgerClient

final case class IdeLedgerClient(
    compiledPackages: PureCompiledPackages,
    traceLog: TraceLog,
    warningLog: WarningLog,
    canceled: () => Boolean,
) extends ScriptLedgerClient
