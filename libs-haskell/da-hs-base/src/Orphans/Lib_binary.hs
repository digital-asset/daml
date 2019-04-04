-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}

-- | Orphans instances for classes defined in the 'binary' library.
module Orphans.Lib_binary () where

import           Data.Binary

import qualified Data.DList              as DList
import           Data.Loc
import           Data.Tagged
import qualified Data.Vector             as V


-- base

instance Binary Loc

instance Binary a => Binary (Tagged tag a) where
    get = Tagged <$> get
    put = put . unTagged


-- vector

instance Binary a => Binary (V.Vector a) where
    put = put . V.toList
    get = V.fromList <$> get

-- dlist

instance Binary a => Binary (DList.DList a) where
    put = put . DList.toList
    get = DList.fromList <$> get

{-
-- Identity

instance Binary1 Identity where
    liftPut1 b (Identity x) = b x
    liftGet1 b = Identity <$> b
    -}
