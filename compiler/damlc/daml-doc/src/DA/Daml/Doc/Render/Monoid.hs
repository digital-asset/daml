-- Copyright (c) 2019 The DAML Authors. All rights reserved.
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
    { re_localAnchors :: Set.Set Anchor
        -- ^ anchors defined in the same file
    , re_globalAnchors :: Map.Map Anchor FilePath
        -- ^ anchors defined in the same folder
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
        External uri -> T.pack . show $
            uri { URI.uriFragment = "#" <> T.unpack anchor }

-- | Find the location of an anchor by reference, if possible.
lookupReference ::
    RenderEnv
    -> Reference
    -> Maybe AnchorLocation
lookupReference RenderEnv{..} ref = asum
    [ SameFile <$ guard (Set.member (referenceAnchor ref) re_localAnchors)
    , SameFolder <$> Map.lookup (referenceAnchor ref) re_globalAnchors
    , External <$> (packageURI =<< referencePackage ref)
    ]

-- | Map package names to URLs. In the future this should be configurable
-- but for now we are hardcoding the standard library packages which are
-- documented on docs.daml.com
packageURI :: Packagename -> Maybe URI.URI
packageURI (Packagename "daml-prim") = Just damlBaseURI
packageURI (Packagename "daml-stdlib") = Just damlBaseURI
packageURI _ = Nothing

damlBaseURI :: URI.URI
damlBaseURI = fromJust $ URI.parseURI "https://docs.daml.com/daml/reference/base.html"

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

renderPage :: RenderFormatter -> RenderOut -> T.Text
renderPage formatter output =
    T.unlines (formatter renderEnv output)
  where
    renderEnv = RenderEnv
        { re_localAnchors = getRenderAnchors output
        , re_globalAnchors = Map.empty
        }

-- | Render a folder of modules.
renderFolder ::
    RenderFormatter
    -> Map.Map Modulename RenderOut
    -> Map.Map Modulename T.Text
renderFolder formatter fileMap =
    let moduleAnchors = Map.map getRenderAnchors fileMap
        re_globalAnchors = Map.fromList
            [ (anchor, moduleNameToFileName moduleName <.> "html")
            | (moduleName, anchors) <- Map.toList moduleAnchors
            , anchor <- Set.toList anchors
            ]
    in flip Map.mapWithKey fileMap $ \moduleName output ->
        let re_localAnchors = fromMaybe Set.empty $
                Map.lookup moduleName moduleAnchors
            renderEnv = RenderEnv {..}
        in T.unlines (formatter renderEnv output)

moduleNameToFileName :: Modulename -> FilePath
moduleNameToFileName =
    T.unpack . T.replace "." "-" . unModulename
