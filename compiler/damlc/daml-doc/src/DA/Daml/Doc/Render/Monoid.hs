-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings, DerivingStrategies #-}

-- | Monoid with which to render documentation.
module DA.Daml.Doc.Render.Monoid
  ( module DA.Daml.Doc.Render.Monoid
  ) where

import DA.Daml.Doc.Types
import Control.Monad
import Data.Foldable
import Data.Maybe
import System.FilePath
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as T

-- | Environment in which to generate final documentation.
data RenderEnv = RenderEnv
    { lookupAnchor :: Anchor -> Maybe AnchorLocation
        -- ^ get location of anchor relative to render output, if available
    }

-- | Location of an anchor relative to the output being rendered. An anchor
-- that lives on the same page may be rendered differently from an anchor
-- that lives in the same folder but a different page, and that may be
-- rendered differently from an anchor that is external. Thus we can
-- handle every case correctly.
data AnchorLocation
    = SamePage  -- ^ anchor is in same file
    | SameFolder FilePath -- ^ anchor is in a file within same folder
    -- TODO: | External URL -- ^ anchor is in on a page at the given URL

-- | Build relative hyperlink from anchor and anchor location.
anchorRelativeHyperlink :: AnchorLocation -> Anchor -> T.Text
anchorRelativeHyperlink anchorLoc (Anchor anchor) =
    case anchorLoc of
        SamePage -> "#" <> anchor
        SameFolder fileName -> T.concat [T.pack fileName, "#", anchor]

-- | Is the anchor available in the rendering environment? Renderers should avoid
-- generating links to anchors that don't actually exist.
--
-- One reason an anchor may be unavailable is because of a @-- | HIDE@ directive.
-- Another possibly reason is that the anchor refers to a definition in another
-- package (and at the moment it's not possible to link accross packages).
renderAnchorAvailable :: RenderEnv -> Anchor -> Bool
renderAnchorAvailable RenderEnv{..} anchor = isJust (lookupAnchor anchor)

-- | Renderer output. This is the set of anchors that were generated, and a
-- list of output functions that depend on RenderEnv. The goal is to prevent
-- the creation of spurious anchors links (i.e. links to anchors that don't
-- exist), and link correctly any anchors that do appear.
--
-- Using a newtype here so we can derive the semigroup / monoid instances we
-- want automatically. :-)
newtype RenderOut = RenderOut (Set.Set Anchor, [RenderEnv -> [T.Text]])
    deriving newtype (Semigroup, Monoid)

-- | Render a single page doc. Any links to anchors not appearing on the
-- single page will be dropped.
renderPage :: RenderOut -> T.Text
renderPage (RenderOut (localAnchors, renderFns)) =
    T.unlines (concatMap ($ renderEnv) renderFns)
  where
    lookupAnchor :: Anchor -> Maybe AnchorLocation
    lookupAnchor anchor
        | Set.member anchor localAnchors = Just SamePage
        | otherwise = Nothing
    renderEnv = RenderEnv {..}

-- | Render a folder of modules.
renderFolder :: Map.Map Modulename RenderOut -> Map.Map Modulename T.Text
renderFolder fileMap =
    let globalAnchors = Map.fromList
            [ (anchor, moduleNameToFileName moduleName <.> "html")
            | (moduleName, RenderOut (anchors, _)) <- Map.toList fileMap
            , anchor <- Set.toList anchors
            ]
    in flip Map.map fileMap $ \(RenderOut (localAnchors, renderFns)) ->
        let lookupAnchor anchor = asum
                [ SamePage <$ guard (Set.member anchor localAnchors)
                , SameFolder <$> Map.lookup anchor globalAnchors
                ]
            renderEnv = RenderEnv {..}
        in T.unlines (concatMap ($ renderEnv) renderFns)

moduleNameToFileName :: Modulename -> FilePath
moduleNameToFileName = T.unpack . T.replace "." "-" . unModulename

-- | Declare an anchor for the purposes of rendering output.
renderDeclareAnchor :: Anchor -> RenderOut
renderDeclareAnchor anchor = RenderOut (Set.singleton anchor, [])

-- | Render a single line of text. A newline is automatically
-- added at the end of the line.
renderLine :: T.Text -> RenderOut
renderLine l = renderLines [l]

-- | Render multiple lines of text. A newline is automatically
-- added at the end of every line, including the last one.
renderLines :: [T.Text] -> RenderOut
renderLines ls = renderLinesDep (const ls)

-- | Render a single line of text that depends on the rendering environment.
-- A newline is automatically added at the end of the line.
renderLineDep :: (RenderEnv -> T.Text) -> RenderOut
renderLineDep f = renderLinesDep (pure . f)

-- | Render multiple lines of text that depend on the rendering environment.
-- A newline is automatically added at the end of every line, including the
-- last one.
renderLinesDep :: (RenderEnv -> [T.Text]) -> RenderOut
renderLinesDep f = RenderOut (mempty, [f])

-- | Prefix every output line by a particular text.
renderPrefix :: T.Text -> RenderOut -> RenderOut
renderPrefix p (RenderOut (env, fs)) =
    RenderOut (env, map (map (p <>) .) fs)

-- | Indent every output line by a particular amount.
renderIndent :: Int -> RenderOut -> RenderOut
renderIndent n = renderPrefix (T.replicate n " ")
