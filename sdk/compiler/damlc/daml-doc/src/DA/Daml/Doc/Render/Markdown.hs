-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Doc.Render.Markdown
  ( renderMd
  ) where

import DA.Daml.Doc.Anchor
import DA.Daml.Doc.Types
import DA.Daml.Doc.Render.Util (adjust, escapeText, (<->))
import DA.Daml.Doc.Render.Monoid

import Data.List.Extra
import qualified Data.Text as T

renderMd :: RenderEnv -> RenderOut -> [T.Text]
renderMd env = \case
    RenderSpaced chunks -> renderMdSpaced env chunks
    RenderModuleHeader title -> ["#" <-> title]
    RenderSectionHeader title -> ["##" <-> title]
    RenderBlock block -> blockquote (renderMd env block)
    RenderList items -> spaced (map (bullet . renderMd env) items)
    RenderRecordFields fields -> renderMdFields env fields
    RenderParagraph text -> [renderMdText env text]
    RenderDocs docText -> T.lines . unDocText $ docText
    RenderAnchor anchor -> [anchorTag anchor]
    RenderIndex moduleNames ->
        [ "*" <-> renderMdLink env
                (Reference Nothing (moduleAnchor moduleName))
                (unModulename moduleName)
        | moduleName <- moduleNames
        ]

renderMdWithAnchor :: RenderEnv -> Anchor -> RenderOut -> [T.Text]
renderMdWithAnchor env anchor = \case
    RenderModuleHeader title -> ["#" <-> anchorTag anchor <> title]
    RenderSectionHeader title -> ["##" <-> anchorTag anchor <> title]
    RenderParagraph text -> [anchorTag anchor <> renderMdText env text]
    other -> anchorTag anchor : renderMd env other

renderMdSpaced :: RenderEnv -> [RenderOut] -> [T.Text]
renderMdSpaced env = spaced . renderMds env

renderMds :: RenderEnv -> [RenderOut] -> [[T.Text]]
renderMds env = \case
    RenderAnchor anchor : next : rest ->
        renderMdWithAnchor env anchor next : renderMds env rest
    next : rest -> renderMd env next : renderMds env rest
    [] -> []

renderMdText :: RenderEnv -> RenderText -> T.Text
renderMdText env = \case
    RenderConcat ts -> mconcatMap (renderMdText env) ts
    RenderPlain text -> escapeMd text
    RenderStrong text -> T.concat ["**", escapeMd text, "**"]
    RenderLink ref text -> renderMdLink env ref text
    RenderDocsInline docText ->
        T.unwords . T.lines . unDocText $ docText

renderMdLink :: RenderEnv -> Reference -> T.Text -> T.Text
renderMdLink env ref text =
    case lookupReference env ref of
        Nothing -> escapeMd text
        Just anchorLoc -> T.concat
            [ "["
            , escapeMd text
            , "]("
            , anchorHyperlink anchorLoc (referenceAnchor ref)
            , ")"]

anchorTag :: Anchor -> T.Text
anchorTag (Anchor anchor) = T.concat ["<a name=\"", anchor, "\"></a>"]

-- Utilities

spaced :: [[T.Text]] -> [T.Text]
spaced = intercalate [""]

blockquote :: [T.Text] -> [T.Text]
blockquote = map (">" <->)

indent :: [T.Text] -> [T.Text]
indent = map indent1 where
  indent1 t
    | T.null t = ""
    | otherwise = "  " <> t

bullet :: [T.Text] -> [T.Text]
bullet [] = []
bullet (x : xs) = ("*" <-> x) : indent xs

escapeMd :: T.Text -> T.Text
escapeMd = escapeText (`elem` ("[]*_~`<>\\&" :: String))

-- | Render fields as a pipe-table, like this:
-- >  | Field    | Type     | Description
-- >  | :------- | :------- | :----------
-- >  | anA      | a        |
-- >  | another  | a        | another a
-- >  | andText  | Text     | and text
renderMdFields :: RenderEnv -> [(RenderText, RenderText, RenderText)] -> [T.Text]
renderMdFields _ []  = ["(no fields)"]
renderMdFields env fields = header <> fieldRows
  where
    textFields =
        [ ( renderMdText env name
          , renderMdText env ty
          , renderMdText env doc
          )
        | (name, ty, doc) <- fields
        ]

    fLen = maximum $ T.length "Field" : T.length "Type" :
        [ max (T.length name) (T.length ty)
        | (name, ty, _) <- textFields ]

    header =
        [ T.concat
            [ "| "
            , adjust fLen "Field"
            , " | "
            , adjust fLen "Type"
            , " | Description |"
            ]
        , T.concat
            [ "| :"
            , T.replicate (fLen - 1) "-"
            , " | :"
            , T.replicate (fLen - 1) "-"
            , " | :---------- |"
            ]
        ]

    fieldRows =
        [ T.concat
            [ "| "
            , adjust fLen name
            , " | "
            , adjust fLen ty
            , " | "
            , doc
            , " |"
            ]
        | (name, ty, doc) <- textFields
        ]
