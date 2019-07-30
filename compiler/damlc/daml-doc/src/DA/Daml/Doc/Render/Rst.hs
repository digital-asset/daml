-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}

module DA.Daml.Doc.Render.Rst
  ( renderRst
  ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render.Monoid

import qualified Data.Text.Prettyprint.Doc as Pretty
import Data.Text.Prettyprint.Doc (Doc, defaultLayoutOptions, layoutPretty, pretty, (<+>))
import Data.Text.Prettyprint.Doc.Render.Text (renderStrict)

import Data.Char
import qualified Data.Text as T
import Data.List.Extra

import CMarkGFM

renderRst :: RenderEnv -> RenderOut -> [T.Text]
renderRst env = \case
    RenderSpaced chunks -> spaced (map (renderRst env) chunks)
    RenderModuleHeader title -> header "-" title
    RenderSectionHeader title -> header "^" title
    RenderBlock block -> indent (renderRst env block)
    RenderList items -> spaced (map (bullet . renderRst env) items)
    RenderFields fields -> renderRstFields env fields
    RenderPara text -> [renderRstText env text]
    RenderDocs docText -> docTextToRst docText
    RenderAnchor anchor -> [".. _" <> unAnchor anchor <> ":"]

renderRstText :: RenderEnv -> RenderText -> T.Text
renderRstText env = \case
    RenderConcat ts -> mconcatMap (renderRstText env) ts
    RenderUnwords ts -> T.unwords (map (renderRstText env) ts)
    RenderIntercalate x ts -> T.intercalate x (map (renderRstText env) ts)
    RenderPlain text -> text
    RenderStrong text -> T.concat ["**", text, "**"]
    RenderLink anchor text ->
        case lookupAnchor env anchor of
            Nothing -> text
            Just _ -> T.concat ["`", text, " <", unAnchor anchor, "_>`_"]
    RenderDocsInline docText ->
        T.unwords (docTextToRst docText)

-- Utilities

spaced :: [[T.Text]] -> [T.Text]
spaced = intercalate [""]

indent :: [T.Text] -> [T.Text]
indent = map ("  " <>)

bullet :: [T.Text] -> [T.Text]
bullet [] = []
bullet (x : xs) = ("+ " <> x) : indent xs

header :: T.Text -> T.Text -> [T.Text]
header headerChar title =
    [ title
    , T.replicate (T.length title) headerChar
    ]

renderRstFields :: RenderEnv -> [(RenderText, RenderText, RenderText)] -> [T.Text]
renderRstFields _ []  = mempty
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
        [ [ "   * - " <> escapeTr_ (renderRstText env name)
          , "     - " <> renderRstText env ty
          , "     - " <> renderRstText env doc ]
        | (name, ty, doc) <- fields
        ]


{-
renderAnchor :: Maybe Anchor -> RenderOut
renderAnchor Nothing = mempty
renderAnchor (Just anchor) = mconcat
    [ renderDeclareAnchor anchor
    , renderLines
        [ ""
        , ".. _" <> unAnchor anchor <> ":"
        , ""
        ]
    ]

renderDocText :: DocText -> RenderOut
renderDocText = renderLines . T.lines . docTextToRst

renderSimpleRst :: ModuleDoc -> RenderOut
renderSimpleRst ModuleDoc{..}
  | null md_templates && null md_classes &&
    null md_adts && null md_functions &&
    isNothing md_descr = mempty
renderSimpleRst ModuleDoc{..} = mconcat
    [ renderAnchor md_anchor
    , renderLines -- h1 heading -- anchor link is added by Rst here.
        [ title
        , T.replicate (T.length title) "-"
        , maybe "" docTextToRst md_descr
        ]
    , section "Templates" renderTemplateDoc md_templates
    , section "Template Instances" renderTemplateInstanceDoc md_templateInstances
    , section "Typeclasses" renderClassDoc md_classes
    , section "Data types" renderADTDoc md_adts
    , section "Functions" renderFunctionDoc md_functions
    ]

  where
    title = "Module " <> unModulename md_name

    section :: T.Text -> (a -> RenderOut) -> [a] -> RenderOut
    section _ _ [] = mempty
    section sectionTitle f xs = mconcat
        [ renderLines
            [ ""
            , sectionTitle
            , T.replicate (T.length sectionTitle) "^"
            ]
        , mconcatMap f xs
        ]

renderTemplateDoc :: TemplateDoc -> RenderOut
renderTemplateDoc TemplateDoc{..} = mconcat $
    [ renderAnchor td_anchor
    , renderLineDep $ \env -> T.unwords . concat $
        [ [bold "template"]
        , renderContext env td_super
        , [makeAnchorLink env td_anchor (unTypename td_name)]
        , td_args
        ]
    , maybe mempty ((renderLine "" <>) . renderIndent 2 . renderDocText) td_descr
    , renderLine ""
    , renderIndent 2 (fieldTable td_payload)
    , renderLine ""
    ] ++ map (renderIndent 2 . renderChoiceDoc) td_choices

renderTemplateInstanceDoc :: TemplateInstanceDoc -> RenderOut
renderTemplateInstanceDoc TemplateInstanceDoc{..} = mconcat
    [ renderAnchor ti_anchor
    , renderLinesDep $ \env ->
        [ "template instance " <>
            makeAnchorLink env ti_anchor (unTypename ti_name)
        , "  = " <> renderType env ti_rhs
        , ""
        ]
    , maybe mempty ((<> renderLine "") . renderIndent 2 . renderDocText) ti_descr
    ]

renderChoiceDoc :: ChoiceDoc -> RenderOut
renderChoiceDoc ChoiceDoc{..} = mconcat
    [ renderLine $ prefix "+ " $ bold $ "Choice " <> unTypename cd_name
    , maybe mempty ((renderLine "" <>) . renderIndent 2 . renderDocText) cd_descr
    , renderIndent 2 (fieldTable cd_fields)
    ]

renderClassDoc :: ClassDoc -> RenderOut
renderClassDoc ClassDoc{..} = mconcat
    [ renderAnchor cl_anchor
    , renderLineDep $ \env -> T.unwords . concat $
        [ [bold "class"]
        , renderContext env cl_super
        , [makeAnchorLink env cl_anchor (unTypename cl_name)]
        , cl_args
        , [bold "where"]
        ]
    , maybe mempty ((renderLine "" <>) . renderIndent 2 . renderDocText) cl_descr
    , mconcat $ map (renderIndent 2 . renderFunctionDoc) cl_functions
    ]

renderADTDoc :: ADTDoc -> RenderOut
renderADTDoc TypeSynDoc{..} = mconcat
    [ renderAnchor ad_anchor
    , renderLinesDep $ \env ->
        [ T.unwords
            $ bold "type"
            : makeAnchorLink env ad_anchor (unTypename ad_name)
            : ad_args
        , "  = " <> renderType env ad_rhs
        , ""
        ]
    , maybe mempty ((<> renderLine "") . renderIndent 2 . renderDocText) ad_descr
    ]
renderADTDoc ADTDoc{..} = mconcat
    [ renderAnchor ad_anchor
    , renderLinesDep $ \env ->
        [ T.unwords
            $ bold "data"
            : makeAnchorLink env ad_anchor (unTypename ad_name)
            : ad_args
        , ""
        ]
    , maybe mempty ((<> renderLine "") . renderIndent 2 . renderDocText) ad_descr
    , mconcatMap (renderIndent 2 . (renderLine "" <>) . renderADTConstr) ad_constrs
    ]

renderADTConstr :: ADTConstr -> RenderOut
renderADTConstr PrefixC{..} = mconcat
    [ renderAnchor ac_anchor
    , renderLineDep $ \env ->
        T.unwords
            $ makeAnchorLink env ac_anchor (unTypename ac_name)
            : map (renderTypePrec env 2) ac_args
    , maybe mempty ((renderLine "" <>) . renderDocText) ac_descr
    ]

renderADTConstr RecordC{..} = mconcat
    [ renderAnchor ac_anchor
    , renderLineDep $ \env ->
        makeAnchorLink env ac_anchor (unTypename ac_name)
    , renderLine ""
    , maybe mempty renderDocText ac_descr
    , renderLine ""
    , fieldTable ac_fields
    ]


{- | Render fields as an rst list-table (editing-friendly), like this:

> .. list-table:: Contract Template Parameters>
>     :widths: 15 10 30
>    :header-rows: 1
>
>    * - Field
>      - Type
>      - Description
>    * - anA
>      - `a`
>      -
>    * - another
>      - `a`
>      - another a
>    * - andText
>      - `Text`
>      - and text
-}
fieldTable :: [FieldDoc] -> RenderOut
fieldTable []  = mempty
fieldTable fds = mconcat -- NB final empty line is essential and intended
    [ renderLines
        [ ".. list-table::"
        , "   :widths: 15 10 30"
        , "   :header-rows: 1"
        , ""
        , "   * - Field"
        , "     - Type"
        , "     - Description" ]
    , fieldRows
    ]
  where
    fieldRows = renderLinesDep $ \env -> concat
       [ [ prefix "   * - " $ escapeTr_ (unFieldname fd_name)
         , prefix "     - " $ renderType env fd_type
         , prefix "     - " $ maybe " " (docTextToRst . DocText . T.unwords . T.lines . unDocText) fd_descr ]-- FIXME: indent properly instead of this
       | FieldDoc{..} <- fds ]


renderFunctionDoc :: FunctionDoc -> RenderOut
renderFunctionDoc FunctionDoc{..} = mconcat
    [ renderAnchor fct_anchor
    , renderLinesDep $ \ env ->
        [ makeAnchorLink env fct_anchor (wrapOp (unFieldname fct_name))
        , T.unwords . concat $
            [ ["  :"]
            , renderContext env fct_context
            , [renderType env fct_type]
            ]
        , ""
        ]
    , maybe (renderLine "") (renderIndent 2 . renderDocText) fct_descr
    ]

------------------------------------------------------------
-- helpers

-- | Render a type.
renderType :: RenderEnv -> Type -> T.Text
renderType env = renderTypePrec env 0

-- | Render a type at a given precedence level. The precedence
-- level controls whether parentheses will be placed around the
-- type or not. Thus:
--
-- * precedence 0: no brackets
-- * precedence 1: brackets around function types
-- * precedence 2: brackets around function types and type application
renderTypePrec :: RenderEnv -> Int -> Type -> T.Text
renderTypePrec env prec = \case
    TypeApp anchorM (Typename typename) args ->
        (if prec >= 2 && notNull args then inParens else id)
            . T.unwords
            $ makeAnchorLink env anchorM typename
            : map (renderTypePrec env 2) args
    TypeFun ts ->
        (if prec >= 1 then inParens else id)
            . T.intercalate " -> "
            $ map (renderTypePrec env 1) ts
    TypeList t ->
        "[" <> renderTypePrec env 0 t <> "]"
    TypeTuple [t] ->
        renderTypePrec env prec t
    TypeTuple ts ->
        "(" <> T.intercalate ", " (map (renderTypePrec env 0) ts) <> ")"

-- | Render type context as a list of words. Nothing is rendered as [],
-- and Just t is rendered as [render t, "=>"].
renderContext :: RenderEnv -> Maybe Type -> [T.Text]
renderContext env = maybe [] (\x -> [renderTypePrec env 1 x, "=>"])

makeAnchorLink :: RenderEnv -> Maybe Anchor -> T.Text -> T.Text
makeAnchorLink env anchorM linkText = fromMaybe linkText $ do
    anchor <- anchorM
    _anchorLoc <- lookupAnchor env anchor
        -- note: AnchorLoc doesn't affect the link format at the moment,
        -- but this will change once we add external links.
        -- At the moment all this line does is ensure the link has
        -- been defined in the environment.
    Just $ T.concat ["`", linkText, " <", unAnchor anchor, "_>`_"]
-}

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
        CODE_BLOCK _info t ->
          Pretty.align (Pretty.vsep [".. code-block:: daml", "", Pretty.indent 2 (pretty t)])
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
    prettyRst txt = pretty $ leadingWhite <> T.unwords (map escapeTr_ (T.words txt)) <> trailingWhite
      where trailingWhite = T.takeWhileEnd isSpace txt
            leadingWhite  = T.takeWhile isSpace txt

escapeTr_ :: T.Text -> T.Text
escapeTr_ w | T.null w        = w
            | T.last w == '_' = T.init w <> "\\_"
            | otherwise       = w
