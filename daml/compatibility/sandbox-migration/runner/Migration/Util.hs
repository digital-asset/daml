-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Migration.Util (symDiff, equivalent) where

import Data.List

symDiff :: Eq a => [a] -> [a] -> [a]
symDiff left right = (left \\ right) ++ (right \\ left)

equivalent :: Ord a => [a] -> [a] -> Bool
equivalent left right = sort left == sort right
