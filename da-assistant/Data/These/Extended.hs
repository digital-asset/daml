-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module Data.These.Extended
    ( module Data.These
    , justHere
    , justThere
    ) where

import Data.These

-- | Extract the first half of 'These'.
justHere :: These a t -> Maybe a
justHere = \case
    This a -> Just a
    That _ -> Nothing
    These a _ -> Just a

-- | Extract the second half of 'These'.
justThere :: These t a -> Maybe a
justThere = \case
    This _ -> Nothing
    That a -> Just a
    These _ a -> Just a
