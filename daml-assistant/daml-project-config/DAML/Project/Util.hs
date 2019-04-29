-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DAML.Project.Util where

-- | Same as 'fromRight' but monadic in the applied function.
fromRightM :: Applicative m => (a -> m b) -> Either a b -> m b
fromRightM f = either f pure

-- | Same as 'fromMaybe' but monadic in the default.
fromMaybeM :: Applicative m => m a -> Maybe a -> m a
fromMaybeM d = maybe d pure
