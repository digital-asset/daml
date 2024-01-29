// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

sealed trait EventDecoderError extends Product with Serializable
case object DecoderTableLookupFailure extends EventDecoderError
case object CreateEventToContractMappingError extends EventDecoderError
