-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings, DerivingStrategies #-}

module DA.Daml.Doc.Render.Rst
  ( renderSimpleRst
  , renderFinish
  ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render.Util

import qualified Data.Text.Prettyprint.Doc as Pretty
import           Data.Text.Prettyprint.Doc (Doc, defaultLayoutOptions, layoutPretty, pretty, (<+>))
import           Data.Text.Prettyprint.Doc.Render.Text (renderStrict)

import           Data.Char
import           Data.Maybe
import qualified Data.Text as T
import qualified Data.Set as Set

import CMarkGFM

-- | Renderer output. This is the set of anchors that were generated, and a
-- list of output functions that depend on that set. The goal is to prevent
-- the creation of spurious anchors links (i.e. links to anchors that don't
-- exist).
--
-- (In theory this could be done in two steps, but that seems more error prone
-- than building up both steps at the same time, and combining them at the
-- end, as is done here.)
--
-- Using a newtype here so we can derive the semigroup / monoid instances we
-- want automatically. :-)
newtype RenderOut = RenderOut (RenderEnv, [RenderEnv -> [T.Text]])
    deriving newtype (Semigroup, Monoid)

newtype RenderEnv = RenderEnv (Set.Set Anchor)
    deriving newtype (Semigroup, Monoid)

-- | Is the anchor available in the render environment? Renderers should avoid
-- generating links to anchors that don't actually exist.
--
-- One reason an anchor may be unavailable is because of a @-- | HIDE@ directive.
-- Another possibly reason is that the anchor refers to a definition in another
-- package (and at the moment it's not possible to link accross packages).
renderAnchorAvailable :: RenderEnv -> Anchor -> Bool
renderAnchorAvailable (RenderEnv anchors) anchor = Set.member anchor anchors

renderFinish :: RenderOut -> T.Text
renderFinish (RenderOut (xs, fs)) = T.unlines (concatMap ($ xs) fs)

renderAnchor :: Maybe Anchor -> RenderOut
renderAnchor Nothing = mempty
renderAnchor (Just anchor) = RenderOut
    ( RenderEnv (Set.singleton anchor)
    , [const ["", ".. _" <> unAnchor anchor <> ":", ""]]
    )

renderLine :: T.Text -> RenderOut
renderLine l = renderLines [l]

renderLines :: [T.Text] -> RenderOut
renderLines ls = renderLinesDep (const ls)

renderLineDep :: (RenderEnv -> T.Text) -> RenderOut
renderLineDep f = renderLinesDep (pure . f)

renderLinesDep :: (RenderEnv -> [T.Text]) -> RenderOut
renderLinesDep f = RenderOut (mempty, [f])

renderIndent :: Int -> RenderOut -> RenderOut
renderIndent n (RenderOut (env, fs)) =
    RenderOut (env, map (map (T.replicate n " " <>) .) fs)

renderDocText :: DocText -> RenderOut
renderDocText = renderLines . T.lines . docTextToRst

renderSimpleRst :: ModuleDoc -> RenderOut
renderSimpleRst ModuleDoc{..}
  | null md_templates && null md_classes &&
    null md_adts && null md_functions &&
    isNothing md_descr = mempty
renderSimpleRst ModuleDoc{..} = mconcat $
  [ renderAnchor md_anchor
  , renderLines
      [ title
      , T.replicate (T.length title) "-"
      , maybe "" docTextToRst md_descr
      ]
  ]
  <> concat
  [ if null md_templates
    then []
    else [ renderLines
              [ ""
              , "Templates"
              , "^^^^^^^^^" ]
         , mconcat $ map tmpl2rst md_templates
         ]
  , if null md_classes
    then []
    else [ renderLines
              [ ""
              , "Typeclasses"
              , "^^^^^^^^^^^" ]
         , mconcat $ map cls2rst md_classes
         ]
  , if null md_adts
    then []
    else [ renderLines
              [ ""
              , "Data types"
              , "^^^^^^^^^^"]
         , mconcat $ map adt2rst md_adts
         ]
  , if null md_functions
    then []
    else [ renderLines
              [ ""
              , "Functions"
              , "^^^^^^^^^" ]
         , mconcat $ map fct2rst md_functions
         ]
  ]

  where title = "Module " <> unModulename md_name

tmpl2rst :: TemplateDoc -> RenderOut
tmpl2rst TemplateDoc{..} = mconcat $
  [ renderAnchor td_anchor
  , renderLine $ "template " <> enclosedIn "**" (unTypename td_name)
  , maybe mempty ((renderLine "" <>) . renderIndent 2 . renderDocText) td_descr
  , renderLine ""
  , renderIndent 2 (fieldTable td_payload)
  , renderLine ""
  ] ++ map (renderIndent 2 . choiceBullet) td_choices

choiceBullet :: ChoiceDoc -> RenderOut
choiceBullet ChoiceDoc{..} = mconcat
  [ renderLine $ prefix "+ " $ enclosedIn "**" $ "Choice " <> unTypename cd_name
  , maybe mempty ((renderLine "" <>) . renderIndent 2 . renderDocText) cd_descr
  , renderIndent 2 (fieldTable cd_fields)
  ]

cls2rst ::  ClassDoc -> RenderOut
cls2rst ClassDoc{..} = mconcat
    [ renderAnchor cl_anchor
    , renderLineDep $ \env ->
        "**class " <> maybe "" (\x -> type2rst env x <> " => ") cl_super <> T.unwords (unTypename cl_name : cl_args) <> " where**"
    , maybe mempty ((renderLine "" <>) . renderIndent 2 . renderDocText) cl_descr
    , mconcat $ map (renderIndent 2 . fct2rst) cl_functions
    ]

adt2rst :: ADTDoc -> RenderOut
adt2rst TypeSynDoc{..} = mconcat
    [ renderAnchor ad_anchor
    , renderLinesDep $ \env ->
        [ "type " <> enclosedIn "**"
            (T.unwords (unTypename ad_name : ad_args))
        , "    = " <> type2rst env ad_rhs
        , ""
        ]
    , maybe mempty ((<> renderLine "") . renderIndent 2 . renderDocText) ad_descr
    ]
adt2rst ADTDoc{..} = mconcat $
    [ renderAnchor ad_anchor
    , renderLines
        [ "data " <> enclosedIn "**" (T.unwords (unTypename ad_name : ad_args))
        , "" ]
    , maybe mempty ((<> renderLine "") . renderIndent 2 . renderDocText) ad_descr
    ] ++ map (renderIndent 2 . (renderLine "" <>) . constr2rst) ad_constrs


constr2rst ::  ADTConstr -> RenderOut
constr2rst PrefixC{..} = mconcat
    [ renderAnchor ac_anchor
    , renderLineDep $ \env ->
        T.unwords (enclosedIn "**" (unTypename ac_name) : map (type2rst env) ac_args)
            -- FIXME: Parentheses around args seems necessary here
            -- if they are type application or function (see type2rst).
    , maybe mempty ((renderLine "" <>) . renderDocText) ac_descr
    ]

constr2rst RecordC{..} = mconcat
    [ renderAnchor ac_anchor
    , renderLine $ enclosedIn "**" (unTypename ac_name)
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
         , prefix "     - " $ type2rst env fd_type
         , prefix "     - " $ maybe " " (docTextToRst . DocText . T.unwords . T.lines . unDocText) fd_descr ] -- FIXME: indent properly instead of this
       | FieldDoc{..} <- fds ]

-- | Render a type. Nested type applications are put in parentheses.
type2rst :: RenderEnv -> Type -> T.Text
type2rst env = f 0
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
        if renderAnchorAvailable env anchor
            then T.concat ["`", unTypename n, " <", unAnchor anchor, "_>`_"]
            else unTypename n

fct2rst :: FunctionDoc -> RenderOut
fct2rst FunctionDoc{..} = mconcat
    [ renderAnchor fct_anchor
    , renderLinesDep $ \ env ->
        [ enclosedIn "**" (wrapOp (unFieldname fct_name))
        , T.concat
            [ "  : "
            , maybe "" ((<> " => ") . type2rst env) fct_context
            , maybe "" ((<> "\n\n") . type2rst env) fct_type
                -- FIXME: when would a function not have a type?
            , maybe "" (indent 2 . docTextToRst) fct_descr
            ]
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
