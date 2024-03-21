// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

@SuppressWarnings(Array("org.wartremover.warts.Null"))
final case class SequencerClientSubscriptionException(error: SequencerClientSubscriptionError)
    extends RuntimeException(
      s"Handling of sequencer event failed with error: $error",
      error.mbException.orNull,
    )
