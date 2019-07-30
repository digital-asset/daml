-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}

-- | Monoid with which to render documentation.
module DA.Daml.Doc.Render.Monoid
    ( module DA.Daml.Doc.Render.Monoid
    ) where

import DA.Daml.Doc.Types
import Control.Monad
import Data.Foldable
import Data.Maybe
import Data.List.Extra
import System.FilePath
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as T

data RenderOut
    = RenderSpaced [RenderOut]
    | RenderModuleHeader T.Text
    | RenderSectionHeader T.Text
    | RenderAnchor Anchor
    | RenderBlock RenderOut
    | RenderList [RenderOut]
    | RenderFields [(RenderText, RenderText, RenderText)]
    | RenderPara RenderText
    | RenderDocs DocText

data RenderText
    = RenderConcat [RenderText]
    | RenderUnwords [RenderText]
    | RenderPlain T.Text
    | RenderStrong T.Text
    | RenderLink Anchor T.Text
    | RenderDocsInline DocText
    | RenderIntercalate T.Text [RenderText]

chunks :: RenderOut -> [RenderOut]
chunks (RenderSpaced xs) = concatMap chunks xs
chunks x = [x]

unchunks :: [RenderOut] -> RenderOut
unchunks [x] = x
unchunks xs = RenderSpaced xs

instance Semigroup RenderOut where
    a <> b = unchunks (chunks a ++ chunks b)

instance Monoid RenderOut where
    mempty = RenderSpaced []
    mconcat = unchunks . concatMap chunks

instance Semigroup RenderText where
    a <> b = RenderConcat [a, b]

instance Monoid RenderText where
    mempty = RenderConcat []
    mconcat = RenderConcat

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
    = SameFile -- ^ anchor is in same file
    | SameFolder FilePath -- ^ anchor is in a file within same folder
    -- TODO: | External URL -- ^ anchor is in on a page at the given URL


-- | Build relative hyperlink from anchor and anchor location.
anchorRelativeHyperlink :: AnchorLocation -> Anchor -> T.Text
anchorRelativeHyperlink anchorLoc (Anchor anchor) =
    case anchorLoc of
        SameFile -> "#" <> anchor
        SameFolder fileName -> T.concat [T.pack fileName, "#", anchor]



type RenderFormatter = RenderEnv -> RenderOut -> [T.Text]

getRenderAnchors :: RenderOut -> Set.Set Anchor
getRenderAnchors = \case
    RenderSpaced xs -> mconcatMap getRenderAnchors xs
    RenderModuleHeader _ -> Set.empty
    RenderSectionHeader _ -> Set.empty
    RenderAnchor anchor -> Set.singleton anchor
    RenderBlock x -> getRenderAnchors x
    RenderList xs -> mconcatMap getRenderAnchors xs
    RenderFields _ -> Set.empty
    RenderPara _ -> Set.empty
    RenderDocs _ -> Set.empty

renderPage :: RenderFormatter -> RenderOut -> T.Text
renderPage formatter output =
    T.unlines (formatter renderEnv output)
  where
    localAnchors = getRenderAnchors output

    lookupAnchor :: Anchor -> Maybe AnchorLocation
    lookupAnchor anchor
        | Set.member anchor localAnchors = Just SameFile
        | otherwise = Nothing

    renderEnv = RenderEnv {..}

-- | Render a folder of modules.
renderFolder ::
    RenderFormatter
    -> Map.Map Modulename RenderOut
    -> Map.Map Modulename T.Text
renderFolder formatter fileMap =
    let moduleAnchors = Map.map getRenderAnchors fileMap
        globalAnchors = Map.fromList
            [ (anchor, moduleNameToFileName moduleName <.> "html")
            | (moduleName, anchors) <- Map.toList moduleAnchors
            , anchor <- Set.toList anchors
            ]
    in flip Map.mapWithKey fileMap $ \moduleName output ->
        let localAnchors = fromMaybe Set.empty $
                Map.lookup moduleName moduleAnchors
            lookupAnchor anchor = asum
                [ SameFile <$ guard (Set.member anchor localAnchors)
                , SameFolder <$> Map.lookup anchor globalAnchors
                ]
            renderEnv = RenderEnv {..}
        in T.unlines (formatter renderEnv output)

moduleNameToFileName :: Modulename -> FilePath
moduleNameToFileName =
    T.unpack . T.replace "." "-" . unModulename

{-

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
-}