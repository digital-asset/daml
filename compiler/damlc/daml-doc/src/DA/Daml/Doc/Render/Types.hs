-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Types common to DA.Daml.Doc.Render
module DA.Daml.Doc.Render.Types
    ( module DA.Daml.Doc.Render.Types
    ) where

import DA.Daml.Doc.Types -- for Anchor, AnchorMap
import Data.Text qualified as T

data RenderFormat = Rst | Markdown | Html
    deriving (Eq, Show, Read, Enum, Bounded)

-- | Control whether to render docs as a single file, or as
-- an interlinked folder of many files, one per Daml module.
data RenderMode
    = RenderToFile FilePath -- ^ render to single file
    | RenderToFolder FilePath -- ^ render to folder, one file per module

-- | Options that affect rendering.
data RenderOptions = RenderOptions
    { ro_mode :: RenderMode -- ^ control single file / multi file rendering
    , ro_format :: RenderFormat -- ^ renderer output format
    , ro_title :: Maybe T.Text -- ^ title of rendered documentation
    , ro_template :: Maybe T.Text -- ^ renderer template
    , ro_indexTemplate :: Maybe T.Text -- ^ renderer template for index
    , ro_hoogleTemplate :: Maybe T.Text -- ^ renderer template for hoogle database
    , ro_baseURL :: Maybe T.Text -- ^ base URL for generated documentation
    , ro_hooglePath :: Maybe FilePath -- ^ path to output hoogle database
    , ro_anchorPath :: Maybe FilePath -- ^ path to output anchor table
    , ro_externalAnchors :: AnchorMap -- ^ external input anchor table
    , ro_globalInternalExt :: String -- ^ File extension for internal links
    }
