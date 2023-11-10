-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}

module DA.Daml.Doc.Render.Rst
  ( renderRst
  ) where

import CMarkGFM
import DA.Daml.Doc.Render.Monoid
import DA.Daml.Doc.Render.Util (escapeText, (<->))
import DA.Daml.Doc.Types
import Data.Char
import Data.List.Extra
import Data.Text qualified as T
import Prettyprinter (Doc, defaultLayoutOptions, layoutPretty, pretty, (<+>))
import Prettyprinter qualified as Pretty
import Prettyprinter.Render.Text (renderStrict)

renderRst :: RenderEnv -> RenderOut -> [T.Text]
renderRst env = \case
    RenderSpaced chunks -> spaced (map (renderRst env) chunks)
    RenderModuleHeader title -> header (pick "=" "-") title
    RenderSectionHeader title -> header (pick "-" "^") title
    RenderBlock block -> indent (renderRst env block)
    RenderList items -> spaced (map (bullet . renderRst env) items)
    RenderRecordFields fields -> renderRstFields env fields
    RenderParagraph text -> [renderRstText env text]
    RenderDocs docText -> docTextToRst docText
    RenderAnchor anchor -> [".. _" <> unAnchor anchor <> ":"]
    RenderIndex moduleNames ->
        [ ".. toctree::"
        , "   :maxdepth: 3"
        , "   :titlesonly:"
        , ""
        ] ++
        [ T.concat
            [ "   "
            , unModulename moduleName
            , " <"
            , T.pack (moduleNameToFileName moduleName)
            , ">"
            ]
        | moduleName <- moduleNames
        ]
  where
    pick :: t -> t -> t
    pick a b =
        if re_separateModules env
            then a
            else b

renderRstText :: RenderEnv -> RenderText -> T.Text
renderRstText env = \case
    RenderConcat ts -> mconcatMap (renderRstText env) ts
    RenderPlain text -> escapeRst text
    RenderStrong text -> T.concat ["**", escapeRst text, "**"]
    RenderLink ref text ->
        case lookupReference env ref of
            Nothing -> escapeRst text
            Just anchorLoc@(External _) ->
                T.concat
                    [ "`", escapeLinkText text
                    , " <", anchorHyperlink anchorLoc (referenceAnchor ref)
                    , ">`_"]
            Just SameFile ->
                T.concat
                    [ "`", escapeLinkText text
                    , " <", unAnchor (referenceAnchor ref)
                    , "_>`_" ]
            Just (SameFolder _) ->
                T.concat
                    [ ":ref:`", escapeLinkText text
                    , " <", unAnchor (referenceAnchor ref)
                    , ">`" ]
                    -- We use :ref: to get automatic cross-referencing
                    -- across all pages. This is a Sphinx extension.
    RenderDocsInline docText ->
        T.unwords (docTextToRst docText)

-- Utilities

-- | Put an extra newline in between chunks. Because Rst has support for definition
-- lists, we *sometimes* don't put a newline in between, in particular when the
-- next line is indented and looks like a type signature or definition. This
-- should affect the output very little either way (it's only spacing).
spaced :: [[T.Text]] -> [T.Text]
spaced = intercalate [""] . respace
  where
    respace = \case
        [line1] : (line2 : block) : xs
            | any (`T.isPrefixOf` line1) ["`", "**type**", "**template instance**"]
            , any (`T.isPrefixOf` line2) ["  \\:", "  \\="] ->
                (line1 : line2 : block) : respace xs
        x : xs -> x : respace xs
        [] -> []

indent :: [T.Text] -> [T.Text]
indent = map indent1 where
  indent1 t
    | T.null t = ""
    | otherwise = "  " <> t

bullet :: [T.Text] -> [T.Text]
bullet [] = []
bullet (x : xs) = ("+" <-> x) : indent xs

header :: T.Text -> T.Text -> [T.Text]
header headerChar title =
    [ title
    , T.replicate (T.length title) headerChar
    ]

renderRstFields :: RenderEnv -> [(RenderText, RenderText, RenderText)] -> [T.Text]
renderRstFields _ []  = ["(no fields)"]
renderRstFields env fields = concat
    [ [ ".. list-table::"
      , "   :widths: 15 10 30"
      , "   :header-rows: 1"
      , ""
      , "   * - Field"
      , "     - Type"
      , "     - Description" ]
    , fieldRows
    ]
  where
    fieldRows = concat
        [ [ "   * -" <-> renderRstText env name
          , "     -" <-> renderRstText env ty
          , "     -" <-> renderRstText env doc ]
        | (name, ty, doc) <- fields
        ]

-- TODO (MK) Handle doctest blocks. Currently the parse as nested blockquotes.
docTextToRst :: DocText -> [T.Text]
docTextToRst = T.lines . renderStrict . layoutPretty defaultLayoutOptions . render . commonmarkToNode opts exts . unDocText
  where
    opts = []
    exts = []
    headingSymbol :: Int -> Char
    headingSymbol i =
      case i of
        1 -> '#'
        2 -> '*'
        3 -> '='
        4 -> '-'
        5 -> '^'
        6 -> '"'
        _ -> '='
    render :: Node -> Doc ()
    render node@(Node _ ty ns) =
      case ty of
        DOCUMENT -> Pretty.align (Pretty.concatWith (\x y -> x <> Pretty.line <> Pretty.line <> y) (map render ns))

        PARAGRAPH -> Pretty.align (foldMap render ns)
        CODE_BLOCK info t ->
          -- Force rendering of code that doesn’t pass the highlighter.
          let force = ["  :force:" | info == "daml-force"]
          in Pretty.align $ Pretty.vsep $
               ".. code-block:: daml" :
               force <>
               ["", Pretty.indent 2 (pretty t)]
        LIST ListAttributes{..} -> Pretty.align (Pretty.vsep (zipWith (renderListItem listType) [1..] ns))

        EMPH -> Pretty.enclose "*" "*" (foldMap render ns)
        STRONG -> Pretty.enclose "**" "**" (foldMap render ns)

        HEADING i ->
          Pretty.align $
            Pretty.width (foldMap render ns) $ \n ->
            pretty (replicate n (headingSymbol i))

        TEXT t -> prettyRst t
        CODE t -> Pretty.enclose "``" "``" (pretty t)

        SOFTBREAK -> Pretty.line

        LINK url _text -> foldMap render ns <> Pretty.enclose "(" ")" (pretty url)
          -- Proper links in RST mean to render the content within backticks
          -- and trailing underscore, and then carry around the URL to generate
          -- a ref under the paragraph.
          -- Simple solution: slap the URL into the text to avoid introducing
          -- that ref-collecting state.

        HTML_INLINE txt -> prettyRst txt
          -- Treat alleged HTML as text (no support for inline html) to avoid
          -- introducing bad line breaks (which would lead to misaligned rst).

        _ -> pretty (nodeToCommonmark opts Nothing node)

    renderListItem :: ListType -> Int -> Node -> Doc ()
    renderListItem ty i (Node _ _ ns) =
      itemStart <+> Pretty.align (foldMap render ns)
      where
        itemStart = case ty of
          BULLET_LIST -> "*"
          ORDERED_LIST -> pretty (show i) <> "."

    -- escape trailing underscores (which means a link in Rst) from words.
    -- Loses the newline structure (unwords . ... . words), but which
    -- commonMarkToNode destroyed earlier at the call site here.
    prettyRst :: T.Text -> Doc ()
    prettyRst txt = pretty $ leadingWhite <> T.unwords (map escapeRst (T.words txt)) <> trailingWhite
      where trailingWhite = T.takeWhileEnd isSpace txt
            leadingWhite  = T.takeWhile isSpace txt

escapeSlash :: T.Text -> T.Text
escapeSlash = escapeText (== '\\')

-- | There doesn't seem to be any rhyme or reason to what gets
-- escaped in link text. Certainly slashes are *special*,
-- they need to be escaped twice (i.e. each slash in the link text
-- needs to appear as four slashes, otherwise it may interfere with
-- the rendering of another character). But trying to escape
-- indiscriminately just results in a lot of slashes being rendered
-- as link text.
escapeLinkText :: T.Text -> T.Text
escapeLinkText = escapeSlash . escapeSlash

escapeRst :: T.Text -> T.Text
escapeRst = escapeText (`elem` ("\\*_`<>#=-^\":.[]+" :: String))
