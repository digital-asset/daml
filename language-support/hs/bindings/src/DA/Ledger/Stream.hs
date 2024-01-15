-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Streams which are closable at both ends.
-- When closed (at the read-end), elements in flight are dropped, clients are notified
-- When closure is requested at the write-end, elements in flight are processed. The stream becomes properly closed when the closure request reaches the read-end of the stream. Subsequent writes are dropped.

module DA.Ledger.Stream(module X) where

import DA.Ledger.Stream.StreamCore as X
import DA.Ledger.Stream.StreamExtra as X
