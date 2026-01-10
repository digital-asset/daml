// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref

import scala.concurrent.Future

trait SchemaProcessors {

  def contractArgFromJsonToProto(
      template: v2.value.Identifier,
      jsonArgsValue: ujson.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[v2.value.Value]

  def contractArgFromProtoToJson(
      template: v2.value.Identifier,
      protoArgs: v2.value.Record,
  )(implicit
      traceContext: TraceContext
  ): Future[ujson.Value]

  def choiceArgsFromJsonToProto(
      template: v2.value.Identifier,
      choiceName: Ref.IdString.Name,
      jsonArgsValue: ujson.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[v2.value.Value]

  def choiceArgsFromProtoToJson(
      template: v2.value.Identifier,
      choiceName: Ref.IdString.Name,
      protoArgs: v2.value.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[ujson.Value]

  def keyArgFromProtoToJson(
      template: v2.value.Identifier,
      protoArgs: v2.value.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[ujson.Value]

  def keyArgFromJsonToProto(
      template: v2.value.Identifier,
      protoArgs: ujson.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[v2.value.Value]

  def exerciseResultFromProtoToJson(
      template: v2.value.Identifier,
      choiceName: Ref.IdString.Name,
      v: v2.value.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[ujson.Value]

  def exerciseResultFromJsonToProto(
      template: v2.value.Identifier,
      choiceName: Ref.IdString.Name,
      jvalue: ujson.Value,
  )(implicit
      traceContext: TraceContext
  ): Future[scala.Option[v2.value.Value]]
}
