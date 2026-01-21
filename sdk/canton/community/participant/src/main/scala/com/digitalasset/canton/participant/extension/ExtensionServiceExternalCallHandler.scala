// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.platform.apiserver.execution.{ExternalCallError, ExternalCallHandler}
import com.digitalasset.canton.tracing.TraceContext

/** ExternalCallHandler implementation that delegates to ExtensionServiceManager.
  *
  * This bridges the ledger-api-core ExternalCallHandler interface with the
  * participant's ExtensionServiceManager for handling external calls during
  * command submission.
  */
class ExtensionServiceExternalCallHandler(
    extensionServiceManager: ExtensionServiceManager
) extends ExternalCallHandler {

  override def handleExternalCall(
      extensionId: String,
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExternalCallError, String]] = {
    extensionServiceManager
      .handleExternalCall(extensionId, functionId, configHash, input, mode)
      .map(_.left.map { extensionError =>
        ExternalCallError(
          statusCode = extensionError.statusCode,
          message = extensionError.message,
          requestId = extensionError.requestId,
        )
      })
  }
}

object ExtensionServiceExternalCallHandler {
  /** Create an ExternalCallHandler from an optional ExtensionServiceManager.
    *
    * @param extensionServiceManagerOpt Optional ExtensionServiceManager
    * @return ExternalCallHandler that delegates to the manager, or notSupported if None
    */
  def create(
      extensionServiceManagerOpt: Option[ExtensionServiceManager]
  ): ExternalCallHandler =
    extensionServiceManagerOpt match {
      case Some(manager) => new ExtensionServiceExternalCallHandler(manager)
      case None => ExternalCallHandler.notSupported
    }
}
