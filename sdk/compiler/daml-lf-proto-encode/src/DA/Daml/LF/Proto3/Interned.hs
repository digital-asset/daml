-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Interned (
  module DA.Daml.LF.Proto3.Interned
  ) where

import           Control.Monad.State.Strict

import           Data.Int
import qualified Data.Vector         as V

{- Interned gives a generic interfacte to InternedMap and InternedArr. -}

class Interned i where
  empty :: Ord val => i val
  toVec :: i val -> V.Vector val
  internState :: val -> State (i val) Int32
