-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
module Development.IDE.LSP.Server
  ( WithMessage(..)
  , PartialHandlers(..)
  ) where


import Data.Default

import           Language.Haskell.LSP.Types
import qualified Language.Haskell.LSP.Core as LSP
import qualified Language.Haskell.LSP.Messages as LSP
import Development.IDE.Core.Service


data WithMessage = WithMessage
    {withResponse :: forall m req resp .
        (ResponseMessage resp -> LSP.FromServerMessage) -> -- how to wrap a response
        (IdeState -> req -> IO resp) -> -- actual work
        Maybe (LSP.Handler (RequestMessage m req resp))
    ,withNotification :: forall m req .
        Maybe (LSP.Handler (NotificationMessage m req)) -> -- old notification handler
        (IdeState -> req -> IO ()) -> -- actual work
        Maybe (LSP.Handler (NotificationMessage m req))
    }

newtype PartialHandlers = PartialHandlers (WithMessage -> LSP.Handlers -> IO LSP.Handlers)

instance Default PartialHandlers where
    def = PartialHandlers $ \_ x -> pure x

instance Semigroup PartialHandlers where
    PartialHandlers a <> PartialHandlers b = PartialHandlers $ \w x -> a w x >>= b w

instance Monoid PartialHandlers where
    mempty = def
