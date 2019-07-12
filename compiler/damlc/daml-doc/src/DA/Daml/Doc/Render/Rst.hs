-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.Doc.Render.Rst
  ( renderSimpleRst
  ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render.Util

import qualified Data.Text.Prettyprint.Doc as Pretty
import           Data.Text.Prettyprint.Doc (Doc, defaultLayoutOptions, layoutPretty, pretty, (<+>))
import           Data.Text.Prettyprint.Doc.Render.Text (renderStrict)

import           Data.Char
import           Data.Maybe
import qualified Data.Text as T

import CMarkGFM

renderAnchor :: Maybe Anchor -> T.Text
renderAnchor Nothing = ""
renderAnchor (Just anchor) = "\n.. _" <> unAnchor anchor <> ":\n"

renderSimpleRst :: ModuleDoc -> T.Text
renderSimpleRst ModuleDoc{..}
  | null md_templates && null md_classes &&
    null md_adts && null md_functions &&
    isNothing md_descr = T.empty
renderSimpleRst ModuleDoc{..} = T.unlines $
  [ renderAnchor md_anchor
  , title
  , T.replicate (T.length title) "-"
  , maybe "" docTextToRst md_descr
  ]
  <> concat
  [ if null md_templates
    then []
    else [""
         , "Templates"
         , "^^^^^^^^^"
         , T.unlines $ map tmpl2rst md_templates
         ]
  , if null md_classes
    then []
    else [ ""
         , "Typeclasses"
         , "^^^^^^^^^^^"
         , T.unlines $ map cls2rst md_classes
         ]
  , if null md_adts
    then []
    else [ ""
         , "Data types"
         , "^^^^^^^^^^"
         , T.unlines $ map adt2rst md_adts
         ]
  , if null md_functions
    then []
    else [ ""
         , "Functions"
         , "^^^^^^^^^"
         , T.unlines $ map fct2rst md_functions
         ]
  ]

  where title = "Module " <> unModulename md_name

tmpl2rst :: TemplateDoc -> T.Text
tmpl2rst TemplateDoc{..} = T.unlines $
  renderAnchor td_anchor :
  ("template " <> enclosedIn "**" (unTypename td_name)) :
  maybe "" (T.cons '\n' . indent 2 . docTextToRst) td_descr :
  "" :
  indent 2 (fieldTable td_payload) :
  "" :
  map (indent 2 . choiceBullet) td_choices -- ends by "\n" because of unlines above


choiceBullet :: ChoiceDoc -> T.Text
choiceBullet ChoiceDoc{..} = T.unlines
  [ prefix "+ " $ enclosedIn "**" $ "Choice " <> unTypename cd_name
  , maybe "" (flip T.snoc '\n' . indent 2 . docTextToRst) cd_descr
  , indent 2 (fieldTable cd_fields)
  ]

cls2rst ::  ClassDoc -> T.Text
cls2rst ClassDoc{..} = T.unlines $
  renderAnchor cl_anchor :
  "**class " <> maybe "" (\x -> type2rst x <> " => ") cl_super <> T.unwords (unTypename cl_name : cl_args) <> " where**" :
  maybe [] ((:[""]) . indent 2 . docTextToRst) cl_descr ++
  map (indent 2 . fct2rst) cl_functions

adt2rst :: ADTDoc -> T.Text
adt2rst TypeSynDoc{..} = T.unlines $
    [ renderAnchor ad_anchor
    , "type " <> enclosedIn "**"
        (T.unwords (unTypename ad_name : ad_args))
    , "    = " <> type2rst ad_rhs
    ] ++ maybe [] ((:[]) . T.cons '\n' . indent 2 . docTextToRst) ad_descr
adt2rst ADTDoc{..} = T.unlines $
    [ renderAnchor ad_anchor
    , "data " <> enclosedIn "**"
        (T.unwords (unTypename ad_name : ad_args))
    , maybe "" (T.cons '\n' . indent 2 . docTextToRst) ad_descr
    ] ++ map (indent 2 . T.cons '\n' . constr2rst) ad_constrs


constr2rst ::  ADTConstr -> T.Text
constr2rst PrefixC{..} = T.unlines $
    [ renderAnchor ac_anchor
    , T.unwords (enclosedIn "**" (unTypename ac_name) : map type2rst ac_args)
        -- FIXME: Parentheses around args seems necessary here
        -- if they are type application or function (see type2rst).
    ] ++ maybe [] ((:[]) . T.cons '\n' . docTextToRst) ac_descr

constr2rst RecordC{..} = T.unlines
    [ renderAnchor ac_anchor
    , enclosedIn "**" (unTypename ac_name)
    , maybe "" (T.cons '\n' . docTextToRst) ac_descr
    , ""
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
fieldTable :: [FieldDoc] -> T.Text
fieldTable []  = ""
fieldTable fds = T.unlines $ -- NB final empty line is essential and intended
  [ ".. list-table::", "   :widths: 15 10 30", "   :header-rows: 1", ""]
  <> map (indent 3) (headerRow <> fieldRows)
  where
    headerRow = [ "* - Field"
                , "  - Type"
                , "  - Description" ]
    fieldRows = concat
       [ [ prefix "* - " $ escapeTr_ (unFieldname fd_name)
         , prefix "  - " $ type2rst fd_type
         , prefix "  - " $ maybe " " (docTextToRst . DocText . T.unwords . T.lines . unDocText) fd_descr ] -- FIXME: this makes no sense
       | FieldDoc{..} <- fds ]

-- | Render a type. Nested type applications are put in parentheses.
type2rst :: Type -> T.Text
type2rst = f 0
  where
    -- 0 = no brackets
    -- 1 = brackets around function
    -- 2 = brackets around function AND application
    f :: Int -> Type -> T.Text
    f _ (TypeApp a n []) = link a n
    f i (TypeApp a n as) = (if i >= 2 then inParens else id) $
        T.unwords (link a n : map (f 2) as)
    f i (TypeFun ts) = (if i >= 1 then inParens else id) $
        T.intercalate " -> " $ map (f 1) ts
    f _ (TypeList t1) = "[" <> f 0 t1 <> "]"
    f _ (TypeTuple ts) = "(" <> T.intercalate ", " (map (f 0) ts) <>  ")"

    link :: Maybe Anchor -> Typename -> T.Text
    link Nothing n = unTypename n
    link (Just anchor) n =
        if anchor `elem` excludedAnchors
            then unTypename n
            else T.concat ["`", unTypename n, " <", unAnchor anchor, "_>`_"]

-- | A list of anchors to exclude because they don't appear in the stdlib docs.
-- This is a temporary approach -- missing anchors should be derived automatically
-- before rendering everything.
excludedAnchors :: [Anchor]
excludedAnchors =
    [ "class-ghc-classes-eq-21216"
    , "class-ghc-classes-ord-70960"
    , "type-ghc-types-textlit-43215"
    ]

fct2rst :: FunctionDoc -> T.Text
fct2rst FunctionDoc{..} = T.unlines
    [ renderAnchor fct_anchor
    , enclosedIn "**" (wrapOp (unFieldname fct_name))
    , T.concat
        [ "  : "
        , maybe "" ((<> " => ") . type2rst) fct_context
        , maybe "" ((<> "\n\n") . type2rst) fct_type
            -- FIXME: when would a function not have a type?
        , maybe "" (indent 2 . docTextToRst) fct_descr
        ]
    ]

------------------------------------------------------------
-- helpers

-- TODO (MK) Handle doctest blocks. Currently the parse as nested blockquotes.
docTextToRst :: DocText -> T.Text
docTextToRst = renderStrict . layoutPretty defaultLayoutOptions . render . commonmarkToNode opts exts . unDocText
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
