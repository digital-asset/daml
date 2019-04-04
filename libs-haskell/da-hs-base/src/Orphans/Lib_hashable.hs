-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}

-- | Orphan instances for classes defined in the 'hashable' library.
module Orphans.Lib_hashable () where

import           Data.Hashable
import           Data.Tagged


-- tagged

instance Hashable a => Hashable (Tagged tag a) where
    hashWithSalt salt = hashWithSalt salt . unTagged
