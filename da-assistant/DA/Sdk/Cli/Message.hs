-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RankNTypes        #-}

module DA.Sdk.Cli.Message
    ( Message(..)
    , prettyMessage
    , prettyError
    , prettyWarning
    ) where

import           DA.Sdk.Prelude
import qualified DA.Sdk.Pretty        as P
import           Data.Bifunctor

data Message e w
    = Error e
    | Warning w
    deriving (Show, Eq)

-- | Adds a label to the error or warning contained within.
prettyMessage :: (P.Pretty e, P.Pretty w) => Message e w -> P.Doc ann
prettyMessage = \case
    Error er -> P.pretty er
    Warning w -> P.pretty w

-- | Only displays, if it's an error.
prettyError :: (P.Pretty e) => Message e a -> P.Doc ann
prettyError = \case
    Error er -> P.pretty er
    Warning _ -> mempty

-- | Only displays, if it's a warning.
prettyWarning :: (P.Pretty w) => Message a w -> P.Doc ann
prettyWarning = \case
    Error _ -> mempty
    Warning w -> P.pretty w

--------------------------------------------------------------------------------
-- Instances
--------------------------------------------------------------------------------

instance Bifunctor Message where
    bimap f g = \case
        Error e -> Error $ f e
        Warning w -> Warning $ g w

instance (P.Pretty e, P.Pretty w) => P.Pretty (Message e w) where
    pretty = prettyMessage
