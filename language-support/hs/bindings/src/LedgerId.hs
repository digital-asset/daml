-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module LedgerId(LedgerId(..)) where

import           Data.Text.Lazy (Text)

data LedgerId = LedgerId Text deriving Show
