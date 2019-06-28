// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.commands

import com.digitalasset.ledger.api.v1.command_completion_service.Checkpoint
import com.digitalasset.ledger.api.v1.completion.Completion

sealed abstract class CompletionStreamElement extends Product with Serializable

object CompletionStreamElement {

  final case class CheckpointElement(checkpoint: Checkpoint) extends CompletionStreamElement

  final case class CompletionElement(completion: Completion) extends CompletionStreamElement

}
