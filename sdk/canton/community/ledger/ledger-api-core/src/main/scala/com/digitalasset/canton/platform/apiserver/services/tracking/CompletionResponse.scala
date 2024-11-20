// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.ledger.api.v2.completion.Completion as PbCompletion

final case class CompletionResponse(completion: PbCompletion)
