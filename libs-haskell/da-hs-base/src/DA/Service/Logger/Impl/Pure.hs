-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Service.Logger.Impl.Pure
    ( makeNopHandle
    ) where

import qualified DA.Service.Logger            as Logger

-- | Create a pure no-op logger
makeNopHandle :: Monad m => Logger.Handle m
makeNopHandle = Logger.Handle
    { Logger.logJson = \_prio _msg -> return ()
    , Logger.tagHandle = const makeNopHandle
    }
