-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Sdk.Control.Exception.Extra
    ( tryWith
    ) where

import DA.Sdk.Data.Either.Extra (fmapLeft)
import qualified Control.Exception.Safe as E

tryWith :: (E.MonadCatch m, E.Exception e) => (e -> a) -> m b -> m (Either a b)
tryWith f = fmapLeft f . E.try