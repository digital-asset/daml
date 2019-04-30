-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
module Orphans.Lib_pretty() where

import qualified Data.Text as T
import           Text.PrettyPrint.Annotated.HughesPJClass

instance Pretty T.Text where
    pPrint = text . T.unpack
