-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings, DerivingStrategies #-}

-- | Types common to DA.Daml.Doc.Render
module DA.Daml.Doc.Render.Types
    ( module DA.Daml.Doc.Render.Types
    ) where

import qualified Data.Text as T

data DocFormat = Json | Rst | Markdown | Html | Hoogle
    deriving (Eq, Show, Read, Enum, Bounded)

-- | Control whether to render docs as a single file, or as
-- an interlinked folder of many files, one per DAML module.
data RenderMode
    = RenderToFile FilePath -- ^ render to single file
    | RenderToFolder FilePath -- ^ render to folder, one file per module

-- | Options that affect rendering.
data RenderOptions = RenderOptions
    { ro_mode :: RenderMode -- ^ control single file / multi file rendering
    , ro_format :: DocFormat -- ^ renderer output format
    , ro_baseUrl :: Maybe T.Text -- ^ base URL in which to render output
    }
