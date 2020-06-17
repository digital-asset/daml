-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Migration.Util (symDiff, equivalent) where

import Data.List

symDiff :: Eq a => [a] -> [a] -> [a]
symDiff left right = (left \\ right) ++ (right \\ left)

equivalent :: Eq a => [a] -> [a] -> Bool
equivalent xs = null . symDiff xs