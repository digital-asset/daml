-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Sdk.Data.Maybe.Extra
    ( mapMaybeM
    , forMaybeM
    ) where

import Data.Maybe

mapMaybeM :: (Monad m, Functor m) => (a -> m (Maybe b)) -> [a] -> m [b]
mapMaybeM f = fmap catMaybes . mapM f

forMaybeM :: (Monad m, Functor m) => [a] -> (a -> m (Maybe b)) -> m [b]
forMaybeM = flip mapMaybeM