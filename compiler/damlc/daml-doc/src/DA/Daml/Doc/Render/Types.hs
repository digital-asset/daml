-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Types common to DA.Daml.Doc.Render
module DA.Daml.Doc.Render.Types
    ( module DA.Daml.Doc.Render.Types
    ) where

import qualified Data.Text as T

data RenderFormat = Rst | Markdown | Html
    deriving (Eq, Show, Read, Enum, Bounded)

-- | Control whether to render docs as a single file, or as
-- an interlinked folder of many files, one per DAML module.
data RenderMode
    = RenderToFile FilePath -- ^ render to single file
    | RenderToFolder FilePath -- ^ render to folder, one file per module

-- | Options that affect rendering.
data RenderOptions = RenderOptions
    { ro_mode :: RenderMode -- ^ control single file / multi file rendering
    , ro_format :: RenderFormat -- ^ renderer output format
    , ro_title :: Maybe T.Text -- ^ title of rendered documentation
    , ro_template :: Maybe T.Text -- ^ renderer template
    }
