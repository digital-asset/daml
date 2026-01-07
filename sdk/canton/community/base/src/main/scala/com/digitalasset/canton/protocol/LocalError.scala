// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.base.error.{ErrorCode, ErrorResource}
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.google.rpc.status.Status

trait LocalError extends TransactionError with PrettyPrinting with Product with Serializable {

  def reason(): Status = rpcStatusWithoutLoggingContext()

  def isMalformed: Boolean

  /** The first part of the cause. Typically, the same for all instances of the particular type.
    */
  // The leading underscore will exclude the field from the error context, so that it doesn't get logged twice.
  def _causePrefix: String

  /** The second part of the cause. Typically a class parameter.
    */
  def _details: String = ""

  override def cause: String = _causePrefix + _details

  override def code: ErrorCode

  /** Make sure to define this, if _resources is non-empty.
    */
  def _resourcesType: Option[ErrorResource] = None

  /** The affected resources. It is used as follows:
    *   - It will be logged as part of the context information.
    *   - It is included into the resulting LocalReject.
    *   - The LocalReject is sent via the sequencer to the mediator. Therefore: do not include any
    *     confidential data!
    *   - The LocalReject is also output through the ledger API.
    */
  def _resources: Seq[String] = Seq()

  override def resources: Seq[(ErrorResource, String)] =
    _resourcesType.fold(Seq.empty[(ErrorResource, String)])(rt => _resources.map(rs => (rt, rs)))

  override def context: Map[String, String] =
    _resourcesType.map(_.asString -> _resources.show).toList.toMap ++ super.context

  override protected def pretty: Pretty[LocalError] =
    prettyOfClass(
      param("code", _.code.id.unquoted),
      param("cause", _.cause.doubleQuoted),
      param("resources", _._resources.map(_.singleQuoted)),
      paramIfDefined("throwable", _.throwableO),
    )
}
