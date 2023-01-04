-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}

-- | Monoid with which to render documentation.
module DA.Daml.Doc.Render.Monoid
    ( module DA.Daml.Doc.Render.Monoid
    ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render.Types
import Control.Monad
import Data.Foldable
import Data.Maybe
import Data.List.Extra
import System.FilePath
import qualified Data.Map.Strict as Map
import qualified Data.HashMap.Strict as HMS
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Network.URI as URI

data RenderOut
    = RenderSpaced [RenderOut]
    | RenderModuleHeader T.Text
    | RenderSectionHeader T.Text
    | RenderAnchor Anchor
    | RenderBlock RenderOut
    | RenderList [RenderOut]
    | RenderRecordFields [(RenderText, RenderText, RenderText)]
    | RenderParagraph RenderText
    | RenderDocs DocText
    | RenderIndex [Modulename]

data RenderText
    = RenderConcat [RenderText]
    | RenderPlain T.Text
    | RenderStrong T.Text
    | RenderLink Reference T.Text
    | RenderDocsInline DocText

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

renderIntercalate :: T.Text -> [RenderText] -> RenderText
renderIntercalate t xs = mconcat (intersperse (RenderPlain t) xs)

renderUnwords :: [RenderText] -> RenderText
renderUnwords = renderIntercalate " "

-- | Environment in which to generate final documentation.
data RenderEnv = RenderEnv
    { re_separateModules :: Bool
        -- ^ are modules being rendered separately, one per page?
    , re_localAnchors :: Set.Set Anchor
        -- ^ anchors defined in the same file
    , re_globalAnchors :: Map.Map Anchor FilePath
        -- ^ anchors defined in the same folder
    , re_externalAnchors :: AnchorMap
        -- ^ anchors defined externally
    }

-- | Location of an anchor relative to the output being rendered. An anchor
-- that lives on the same page may be rendered differently from an anchor
-- that lives in the same folder but a different page, and that may be
-- rendered differently from an anchor that is external. Thus we can
-- handle every case correctly.
data AnchorLocation
    = SameFile -- ^ anchor is in same file
    | SameFolder FilePath -- ^ anchor is in a file within same folder
    | External URI.URI -- ^ anchor at the given URL

-- | Build hyperlink from anchor and anchor location. Hyperlink is
-- relative for anchors in the same package, absolute for external
-- packages.
anchorHyperlink :: AnchorLocation -> Anchor -> T.Text
anchorHyperlink anchorLoc (Anchor anchor) =
    case anchorLoc of
        SameFile -> "#" <> anchor
        SameFolder fileName -> T.concat [T.pack fileName, "#", anchor]
        External uri -> T.pack . show $ uri

-- | Find the location of an anchor by reference, if possible.
lookupReference ::
    RenderEnv
    -> Reference
    -> Maybe AnchorLocation
lookupReference RenderEnv{..} ref = asum
    [ SameFile <$ guard (Set.member (referenceAnchor ref) re_localAnchors)
    , SameFolder <$> Map.lookup (referenceAnchor ref) re_globalAnchors
    , External <$> (URI.parseURI =<< HMS.lookup (referenceAnchor ref) (unAnchorMap re_externalAnchors))
    ]

type RenderFormatter = RenderEnv -> RenderOut -> [T.Text]

getRenderAnchors :: RenderOut -> Set.Set Anchor
getRenderAnchors = \case
    RenderSpaced xs -> mconcatMap getRenderAnchors xs
    RenderModuleHeader _ -> Set.empty
    RenderSectionHeader _ -> Set.empty
    RenderAnchor anchor -> Set.singleton anchor
    RenderBlock x -> getRenderAnchors x
    RenderList xs -> mconcatMap getRenderAnchors xs
    RenderRecordFields _ -> Set.empty
    RenderParagraph _ -> Set.empty
    RenderDocs _ -> Set.empty
    RenderIndex _ -> Set.empty

renderPage :: RenderFormatter -> AnchorMap -> RenderOut -> T.Text
renderPage formatter externalAnchors output =
    T.unlines (formatter renderEnv output)
    where
        renderEnv = RenderEnv
          { re_separateModules = False
          , re_localAnchors = getRenderAnchors output
          , re_globalAnchors = Map.empty
          , re_externalAnchors = externalAnchors
          }

-- | Render a folder of modules.
renderFolder ::
    RenderFormatter
    -> AnchorMap
    -> Map.Map Modulename RenderOut
    -> (T.Text, Map.Map Modulename T.Text)
renderFolder formatter externalAnchors fileMap =
    let moduleAnchors = Map.map getRenderAnchors fileMap
        re_externalAnchors = externalAnchors
        re_separateModules = True
        re_globalAnchors = Map.fromList
            [ (anchor, moduleNameToFileName moduleName <.> "html")
            | (moduleName, anchors) <- Map.toList moduleAnchors
            , anchor <- Set.toList anchors
            ]
        moduleMap =
            flip Map.mapWithKey fileMap $ \moduleName output ->
                let re_localAnchors = fromMaybe Set.empty $
                        Map.lookup moduleName moduleAnchors
                in T.unlines (formatter RenderEnv{..} output)
        index =
            let re_localAnchors = Set.empty
                output = RenderIndex (Map.keys fileMap)
            in T.unlines (formatter RenderEnv{..} output)
    in (index, moduleMap)

moduleNameToFileName :: Modulename -> FilePath
moduleNameToFileName =
    T.unpack . T.replace "." "-" . unModulename

buildAnchorTable :: RenderOptions -> Map.Map Modulename RenderOut -> HMS.HashMap Anchor T.Text
buildAnchorTable RenderOptions{..} outputs
    | Just baseURL <- ro_baseURL
    = HMS.fromList
        [ (anchor, buildURL baseURL moduleName anchor)
        | (moduleName, output) <- Map.toList outputs
        , anchor <- Set.toList (getRenderAnchors output)
        ]
    where
        stripTrailingSlash :: T.Text -> T.Text
        stripTrailingSlash x = fromMaybe x (T.stripSuffix "/" x)

        buildURL :: T.Text -> Modulename -> Anchor -> T.Text
        buildURL = case ro_mode of
            RenderToFile _ -> buildFileURL
            RenderToFolder _ -> buildFolderURL

        buildFileURL :: T.Text -> Modulename -> Anchor -> T.Text
        buildFileURL baseURL _ anchor = T.concat
            [ baseURL
            , "#"
            , unAnchor anchor
            ]

        buildFolderURL :: T.Text -> Modulename -> Anchor -> T.Text
        buildFolderURL baseURL moduleName anchor = T.concat
            [ stripTrailingSlash baseURL
            , "/"
            , T.pack (moduleNameToFileName moduleName <.> "html")
            , "#"
            , unAnchor anchor
            ]

buildAnchorTable _ _ = HMS.empty
