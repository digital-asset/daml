-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.Doc.Render.Markdown
  ( renderSimpleMD
  ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render.Util
import DA.Daml.Doc.Render.Monoid

import           Data.Maybe
import qualified Data.Text as T
import Data.List.Extra


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

cls2md :: ClassDoc -> RenderOut
cls2md ClassDoc{..} = mconcat
    [ renderAnchorInfix "### " cl_anchor ("Class " <> escapeMd (unTypename cl_name))
    , renderLine ""
    , renderLineDep $ \env -> T.concat
        [ "**class "
        , maybe "" (\x -> type2md env x <> " => ") cl_super
        , escapeMd $ T.unwords (unTypename cl_name : cl_args)
        , " where**"
        ]
    , renderDocText cl_descr
    , renderPrefix "> " $ mconcatMap fct2md cl_functions
    ]

adt2md :: ADTDoc -> RenderOut
adt2md TypeSynDoc{..} = mconcat
    [ renderAnchorInfix "**type " ad_anchor $
        escapeMd (T.unwords (unTypename ad_name : ad_args)) <> "**  "
    , renderLineDep $ \env -> T.concat ["&nbsp; = ", type2md env ad_rhs]
    , renderDocText ad_descr
    ]

adt2md ADTDoc{..} = mconcat
    [ renderAnchorInfix "**data " ad_anchor $
        escapeMd (T.unwords (unTypename ad_name : ad_args)) <> "**"
    , renderDocText ad_descr
    , mconcatMap constrMdItem ad_constrs
    ]

constrMdItem :: ADTConstr -> RenderOut
constrMdItem PrefixC{..} = withAnchorTag ac_anchor $ \tag -> mconcat
    [ renderLineDep $ \env -> T.unwords
        ("*" : (tag <> escapeMd (unTypename ac_name)) : map (type2md env) ac_args)
    , renderIndent 2 $ renderDocText ac_descr
    , renderLine ""
    ]
constrMdItem RecordC{..} = mconcat
    [ renderAnchorInfix "* " ac_anchor . escapeMd $ unTypename ac_name
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
        if renderAnchorAvailable env anchor
            then T.concat ["[", escapeMd $ unTypename n, "](#", unAnchor anchor, ")"]
            else escapeMd $ unTypename n

fct2md :: FunctionDoc -> RenderOut
fct2md FunctionDoc{..} = mconcat
    [ renderAnchorInfix "" fct_anchor $ T.concat
        [ "**", escapeMd $ unFieldname fct_name, "**  " ]
    , renderLinesDep $ \env ->
        maybe [] (\t -> ["&nbsp; : " <> type2md env t]) fct_type
    , renderDocText fct_descr
    ]
------------------------------------------------------------

-- | Escape characters to prevent markdown interpretation.
escapeMd :: T.Text -> T.Text
escapeMd = T.pack . concatMap escapeChar . T.unpack
  where
    escapeChar c
        | shouldEscape c = ['\\', c]
        | otherwise = [c]

    shouldEscape = (`elem` ("[]*_~`<>\\&" :: String))
