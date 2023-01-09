-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
module Orphans.Lib_pretty() where

import Data.Int
import qualified Data.Text as T
import           Text.PrettyPrint.Annotated.HughesPJClass

instance Pretty T.Text where
    pPrint = text . T.unpack

instance Pretty Int32 where
    pPrint i = pPrint (fromIntegral i :: Int)
