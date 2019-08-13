// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

sealed trait EventDecoderError extends Product with Serializable
case object DecoderTableLookupFailure extends EventDecoderError
case object CreateEventToContractMappingError extends EventDecoderError
