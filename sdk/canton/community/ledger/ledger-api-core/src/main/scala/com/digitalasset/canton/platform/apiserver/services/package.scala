// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.reassignment_commands.ReassignmentCommands
import com.daml.tracing.{SpanAttribute, Telemetry, TelemetryContext}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.tracing.TraceContext

package object services {

  def getAnnotatedCommandTraceContext(
      commands: Option[Commands],
      telemetry: Telemetry,
  ): TraceContext = {
    val telemetryContext: TelemetryContext =
      telemetry.contextFromGrpcThreadLocalContext()
    commands.foreach { commands =>
      telemetryContext
        .setAttribute(SpanAttribute.UserId, commands.userId)
        .setAttribute(SpanAttribute.CommandId, commands.commandId)
        .setAttribute(SpanAttribute.Submitter, commands.actAs.headOption.getOrElse(""))
        .setAttribute(SpanAttribute.WorkflowId, commands.workflowId)
    }
    TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())
  }

  def getAnnotatedReassignmentCommandTraceContext(
      commands: Option[ReassignmentCommands],
      telemetry: Telemetry,
  ): TraceContext = {
    val telemetryContext: TelemetryContext =
      telemetry.contextFromGrpcThreadLocalContext()
    commands.foreach { commands =>
      telemetryContext
        .setAttribute(SpanAttribute.UserId, commands.userId)
        .setAttribute(SpanAttribute.CommandId, commands.commandId)
        .setAttribute(SpanAttribute.Submitter, commands.submitter)
        .setAttribute(SpanAttribute.WorkflowId, commands.workflowId)
    }
    TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())
  }

  def getPrepareRequestTraceContext(
      userId: String,
      commandId: String,
      actAs: Seq[String],
      telemetry: Telemetry,
  ): TraceContext = {
    val telemetryContext: TelemetryContext =
      telemetry.contextFromGrpcThreadLocalContext()
    telemetryContext
      .setAttribute(SpanAttribute.UserId, userId)
      .setAttribute(SpanAttribute.CommandId, commandId)
      .setAttribute(SpanAttribute.Submitter, actAs.headOption.getOrElse(""))
      .discard[TelemetryContext]

    TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())
  }

  def getExecuteRequestTraceContext(
      userId: String,
      commandId: Option[String],
      actAs: Seq[String],
      telemetry: Telemetry,
  ): TraceContext = {
    val telemetryContext: TelemetryContext =
      telemetry.contextFromGrpcThreadLocalContext()
    telemetryContext
      .setAttribute(SpanAttribute.UserId, userId)
      .setAttribute(SpanAttribute.CommandId, commandId.getOrElse(""))
      .setAttribute(SpanAttribute.Submitter, actAs.headOption.getOrElse(""))
      .discard[TelemetryContext]

    TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())
  }

}
