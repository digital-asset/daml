-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Doc.Render.Markdown
  ( renderMd
  ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render.Util (adjust)
import DA.Daml.Doc.Render.Monoid

import Data.List.Extra
import qualified Data.Text as T

renderMd :: RenderEnv -> RenderOut -> [T.Text]
renderMd env = \case
    RenderSpaced chunks -> spaced (map (renderMd env) chunks)
    RenderModuleHeader title -> ["# " <> title]
    RenderSectionHeader title -> ["## " <> title]
    RenderBlock block -> blockquote (renderMd env block)
    RenderList items -> spaced (map (bullet . renderMd env) items)
    RenderFields fields -> renderMdFields env fields
    RenderPara text -> [renderMdText env text]
    RenderDocs docText -> T.lines . unDocText $ docText
    RenderAnchor anchor -> ["<a name=\"" <> unAnchor anchor <> "\"></a>"]

renderMdText :: RenderEnv -> RenderText -> T.Text
renderMdText env = \case
    RenderConcat ts -> mconcatMap (renderMdText env) ts
    RenderUnwords ts -> T.unwords $ map (renderMdText env) ts
    RenderIntercalate x ts -> T.intercalate x $ map (renderMdText env) ts
    RenderPlain text -> escapeMd text
    RenderStrong text -> T.concat ["**", escapeMd text, "**"]
    RenderLink anchor text ->
        case lookupAnchor env anchor of
            Nothing -> escapeMd text
            Just anchorLoc -> T.concat
                ["["
                , escapeMd text
                , "]("
                , anchorRelativeHyperlink anchorLoc anchor
                , ")"]
    RenderDocsInline docText ->
        T.unwords . T.lines . unDocText $ docText

-- Utilities

spaced :: [[T.Text]] -> [T.Text]
spaced = intercalate [""]

blockquote :: [T.Text] -> [T.Text]
blockquote = map ("> " <>)

indent :: [T.Text] -> [T.Text]
indent = map ("  " <>)

bullet :: [T.Text] -> [T.Text]
bullet [] = []
bullet (x : xs) = ("* " <> x) : indent xs

escapeMd :: T.Text -> T.Text
escapeMd = T.pack . concatMap escapeChar . T.unpack
  where
    escapeChar c
        | shouldEscape c = ['\\', c]
        | otherwise = [c]

    shouldEscape = (`elem` ("[]*_~`<>\\&" :: String))


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

    fLen = maximum $ 5 :
        [ max (T.length name) (T.length ty)
        | (name, ty, _) <- textFields ]

    header =
        [ T.concat
            [ "| "
            , adjust fLen "Field"
            , " | "
            , adjust fLen "Type"
            , " | Description "
            ]
        , T.concat
            [ "| :"
            , T.replicate (fLen - 1) "-"
            , " | :"
            , T.replicate (fLen - 1) "-"
            , " | :----------------"
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
            ]
        | (name, ty, doc) <- textFields
        ]

{-

-- | Declare anchor and generate HTML tag to insert in renderer output.
withAnchorTag :: Maybe Anchor -> (T.Text -> RenderOut) -> RenderOut
withAnchorTag Nothing fn = fn ""
withAnchorTag (Just anchor) fn = mconcat
    [ renderDeclareAnchor anchor
    , fn $ T.concat ["<a name=\"", unAnchor anchor, "\"></a>"]
    ]

-- | Declare anchor and output HTML tag between two pieces of text.
renderAnchorInfix :: T.Text -> Maybe Anchor ->  T.Text -> RenderOut
renderAnchorInfix pre anchor post =
    withAnchorTag anchor (\tag -> renderLine $ T.concat [ pre, tag, post ])

-- | Render doc text. If Nothing, renders an empty line. If Just, renders
-- doc text block between two empty lines.
renderDocText :: Maybe DocText -> RenderOut
renderDocText Nothing = renderLine ""
renderDocText (Just (DocText d)) = renderLines ("" : T.lines d ++ [""])

renderSimpleMD :: ModuleDoc -> RenderOut
renderSimpleMD ModuleDoc{..}
  | null md_templates && null md_classes &&
    null md_adts && null md_functions &&
    isNothing md_descr = mempty
renderSimpleMD ModuleDoc{..} = mconcat
    [ renderAnchorInfix "# " md_anchor ("Module " <> escapeMd (unModulename md_name))
    , renderDocText md_descr
    , section "Templates" tmpl2md md_templates
    , section "Template Instances" renderTemplateInstanceDocAsMarkdown md_templateInstances
    , section "Typeclasses" cls2md md_classes
    , section "Data types" adt2md md_adts
    , section "Functions" fct2md md_functions
    ]
  where
    section :: T.Text -> (a -> RenderOut) -> [a] -> RenderOut
    section _ _ [] = mempty
    section sectionTitle f xs = mconcat
        [ renderLines
            [ "## " <> sectionTitle
            , ""
            ]
        , mconcatMap f xs
        , renderLine ""
        ]


tmpl2md :: TemplateDoc -> RenderOut
tmpl2md TemplateDoc{..} = withAnchorTag td_anchor $ \tag -> mconcat
    [ renderLineDep $ \env -> T.concat
        [ "### "
        , tag
        , "template "
        , maybe "" ((<> " => ") . type2md env) td_super
        , escapeMd . T.unwords $ unTypename td_name : td_args
        ]
    , renderDocText td_descr
    , fieldTable td_payload
    , if null td_choices
        then mempty
        else mconcat
            [ renderLines ["", "Choices:", ""]
            , mconcatMap choiceBullet td_choices
            ]
    ]
  where
    choiceBullet :: ChoiceDoc -> RenderOut
    choiceBullet ChoiceDoc{..} = mconcat
        [ renderLine ("* " <> escapeMd (unTypename cd_name))
        , renderIndent 2 $ mconcat
            [ renderDocText cd_descr
            , fieldTable cd_fields
            ]
        ]

renderTemplateInstanceDocAsMarkdown :: TemplateInstanceDoc -> RenderOut
renderTemplateInstanceDocAsMarkdown TemplateInstanceDoc{..} = mconcat
    [ renderAnchorInfix "**template instance " ti_anchor $
        escapeMd (unTypename ti_name) <> "**  "
    , renderLineDep $ \env -> T.concat ["&nbsp; = ", type2md env ti_rhs]
    , renderDocText ti_descr
    ]

cls2md :: ClassDoc -> RenderOut
cls2md ClassDoc{..} = withAnchorTag cl_anchor $ \tag -> mconcat
    [ renderLineDep $ \env -> T.concat
        [ tag
        , "**class "
        , maybe "" (\x -> type2md env x <> " => ") cl_super
        , escapeMd $ T.unwords (unTypename cl_name : cl_args)
        , " where**"
        ]
    , blockQuote $ mconcat
        [ renderDocText cl_descr
        , mconcatMap fct2md cl_functions
        ]
    ]

adt2md :: ADTDoc -> RenderOut
adt2md TypeSynDoc{..} = mconcat
    [ renderAnchorInfix "**type " ad_anchor $
        escapeMd (T.unwords (unTypename ad_name : ad_args)) <> "**  "
    , blockQuote $ mconcat
        [ renderLineDep $ \env -> T.concat ["= ", type2md env ad_rhs]
        , renderDocText ad_descr
        ]
    ]

adt2md ADTDoc{..} = mconcat
    [ renderAnchorInfix "**data " ad_anchor $
        escapeMd (T.unwords (unTypename ad_name : ad_args)) <> "**"
    , blockQuote $ mconcat
        [ renderDocText ad_descr
        , mconcatMap constrMdItem ad_constrs
        ]
    ]

constrMdItem :: ADTConstr -> RenderOut
constrMdItem PrefixC{..} = withAnchorTag ac_anchor $ \tag -> mconcat
    [ renderLineDep $ \env -> T.unwords
        $ "*"
        : (tag <> bold (escapeMd (unTypename ac_name)))
        : map (type2md env) ac_args
    , renderIndent 2 $ renderDocText ac_descr
    , renderLine ""
    ]
constrMdItem RecordC{..} = mconcat
    [ renderAnchorInfix "* " ac_anchor . bold . escapeMd $ unTypename ac_name
    , renderIndent 2 $ mconcat
        [ renderDocText ac_descr
        , fieldTable ac_fields
        ]
    , renderLine ""
    ]

-- | Render fields as a pipe-table, like this:
-- >  | Field    | Type/Description |
-- >  | :------- | :---------------
-- >  |`anA`     | `a`
-- >  |`another` | `a`
-- >  |          | another a
-- >  |`andText` | `Text`
-- >  |          | and text
-- >
fieldTable :: [FieldDoc] -> RenderOut
fieldTable []  = renderLine "(no fields)"
fieldTable fds = header <> fieldRows <> renderLine ""
  where
    header = renderLines
      [ "| " <> adjust fLen "Field"   <> " | Type/Description |"
      , "| :" <> T.replicate (fLen - 1) "-" <> " | :----------------"
      ]

    fieldRows = renderLinesDep $ \env -> concat
      [ ("| " <> adjust fLen (escapeMd $ unFieldname fd_name)
              <> " | " <> type2md env fd_type <> " |")
        : maybe []
              (\descr -> [col1Empty <> removeLineBreaks (unDocText descr) <> " |"])
              fd_descr
      | FieldDoc{..} <- fds ]

    -- Markdown does not support multi-row cells so we have to remove
    -- line breaks.
    removeLineBreaks = T.unwords . T.lines

    fLen = maximum $ 5 : map (T.length . escapeMd . unFieldname . fd_name) fds
      -- 5 = length of "Field" header

    col1Empty = "| " <> T.replicate fLen " " <> " | "

-- | Render a type. Nested type applications are put in parentheses.
type2md :: RenderEnv -> Type -> T.Text
type2md env = f 0
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
    f _ (TypeList t1) = "\\[" <> f 0 t1 <> "\\]"
    f _ (TypeTuple ts) = "(" <> T.intercalate ", " (map (f 0) ts) <>  ")"

    link :: Maybe Anchor -> Typename -> T.Text
    link Nothing n = escapeMd $ unTypename n
    link (Just anchor) n =
        case lookupAnchor env anchor of
            Nothing -> escapeMd $ unTypename n
            Just anchorLoc -> T.concat
                [ "["
                , escapeMd $ unTypename n
                , "]("
                , anchorRelativeHyperlink anchorLoc anchor
                , ")"
                ]

fct2md :: FunctionDoc -> RenderOut
fct2md FunctionDoc{..} = mconcat
    [ renderAnchorInfix "" fct_anchor $ T.concat
        [ "**", escapeMd $ unFieldname fct_name, "**  " ]
    , blockQuote $ mconcat
        [ renderLineDep $ \env -> T.concat
            [ ": "
            , maybe "" ((<> " => ") . type2md env) fct_context
            , type2md env fct_type
            ]
        , renderDocText fct_descr
        ]
    ]

blockQuote :: RenderOut -> RenderOut
blockQuote = renderPrefix "> "

------------------------------------------------------------

-- | Escape characters to prevent markdown interpretation.
escapeMd :: T.Text -> T.Text
escapeMd = T.pack . concatMap escapeChar . T.unpack
  where
    escapeChar c
        | shouldEscape c = ['\\', c]
        | otherwise = [c]

    shouldEscape = (`elem` ("[]*_~`<>\\&" :: String))
-}