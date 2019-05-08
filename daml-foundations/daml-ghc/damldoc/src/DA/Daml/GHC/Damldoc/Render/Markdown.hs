-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.GHC.Damldoc.Render.Markdown
  ( renderSimpleMD
  ) where

import DA.Daml.GHC.Damldoc.Types
import DA.Daml.GHC.Damldoc.Render.Util

import           Data.Maybe
import qualified Data.Text as T
import           Data.List (intersperse)

renderSimpleMD :: ModuleDoc -> T.Text
renderSimpleMD ModuleDoc{..}
  | null md_templates && null md_classes &&
    null md_adts && null md_functions &&
    isNothing md_descr = T.empty
renderSimpleMD ModuleDoc{..} = T.unlines $
  [ "# " <> "Module " <> md_name
  , ""
  , fromMaybe "" md_descr
  , "" ]
  <> concat
  [ if null md_templates then []
    else [ "## Templates"
         , ""
         , T.unlines $ map tmpl2md md_templates
         , "" ]
  , if null md_classes
    then []
    else [ "## Typeclasses"
         , ""
         , "Typeclasses in DAML are an interface that defines some behavior."
         , ""
         , T.unlines $ map cls2md md_classes
         , ""
         ]
  , if null md_adts then []
    else [ "## Data types"
         , ""
         , T.unlines $ map adt2md md_adts
         , "" ]
  , if null md_functions then []
    else [ "## Functions"
         , ""
         , T.unlines $ map fct2md md_functions
         ]
  ]


tmpl2md :: TemplateDoc -> T.Text
tmpl2md TemplateDoc{..} = T.unlines $
  "### Template " <> asCode td_name :
  maybe "" (T.cons '\n') td_descr :
  T.empty :
  fieldTable td_payload :
  "" :
  "  #### Choices" :
  "" :
  map choiceBullet td_choices -- ends by "\n" because of unlines above

  where
    choiceBullet :: ChoiceDoc -> T.Text
    choiceBullet ChoiceDoc{..} = T.unlines
      [ prefix "* " $ asCode cd_name
      , maybe T.empty (flip T.snoc '\n' . indent 2) cd_descr
      , indent 2 (fieldTable cd_fields)
      ]

cls2md :: ClassDoc -> T.Text
cls2md ClassDoc{..} = T.unlines $
  "### `class` " <> maybe "" (\x -> type2md x <> " => ") cl_super <> T.unwords (cl_name : cl_args) <> " where" :
  maybe T.empty (T.cons '\n' . indent 2) cl_descr :
  map (indent 2 . fct2md) cl_functions

adt2md :: ADTDoc -> T.Text
adt2md TypeSynDoc{..} = T.unlines $
  "### `type` " <> asCode (ad_name <> (T.concat $ map (T.cons ' ') ad_args)) :
  "    = " <> type2md ad_rhs :
  maybe [] ((:[]) . T.cons '\n' . indent 2) ad_descr
adt2md ADTDoc{..} = T.unlines $
  "### `data` " <> asCode (ad_name <> (T.concat $ map (T.cons ' ') ad_args)) :
  maybe T.empty (T.cons '\n' . indent 2) ad_descr :
  map constrMdItem ad_constrs


constrMdItem :: ADTConstr -> T.Text
constrMdItem PrefixC{..} =
  ("* " <> T.unwords (asCode ac_name : map type2md ac_args))
  <> maybe T.empty (T.cons '\n' . indent 2) ac_descr
constrMdItem RecordC{..} =
  ("* " <> asCode ac_name)
  <> maybe T.empty (T.cons '\n' . indent 2) ac_descr
  <> "\n\n"
  <> indent 2 (fieldTable ac_fields)


-- | Render fields as a pipe-table, like this:
-- >  | Field    | Type/Description |
-- >  | :------- | :---------------
-- >  |`anA`     | `a`
-- >  |`another` | `a`
-- >  |          | another a
-- >  |`andText` | `Text`
-- >  |          | and text
-- >
fieldTable :: [FieldDoc] -> T.Text
fieldTable []  = "(no fields)"
fieldTable fds = header <> fieldRows <> "\n"
  where
    header = T.unlines
      [ "| " <> adjust fLen "Field"   <> " | Type/Description |"
      , "| :" <> T.replicate (fLen - 1) "-" <> " | :----------------"
      ]

    fieldRows = T.unlines
      [ "| " <> adjust fLen (asCode fd_name) <> " | " <> type2md fd_type <> " |"
        <> maybe "" (\desc -> "\n" <> col1Empty <> removeLineBreaks desc <> " |") fd_descr
      | FieldDoc{..} <- fds ]

    -- Markdown does not support multi-row cells so we have to remove
    -- line breaks.
    removeLineBreaks = T.unwords . T.lines

    fLen = maximum $ 5 : map (T.length . asCode . fd_name) fds
      -- 5 = length of "Field" header

    col1Empty = "| " <> T.replicate fLen " " <> " | "

-- | Render a type. Nested type applications are put in parentheses.
type2md :: Type -> T.Text
type2md t = t2md id t
  where t2md f (TypeFun ts) = f $ T.intercalate " `->` " $ map (t2md id) ts
        t2md _ (TypeList t1) = "`[` " <> t2md id t1 <> " `]`"
        t2md _ (TypeTuple ts) = "`(` " <>
                            T.concat (intersperse ", " $ map (t2md id) ts) <>
                            " `)`"
        t2md _ (TypeApp n []) = asCode n
        t2md f (TypeApp name args) =
          f $ T.unwords ( asCode name : map (t2md codeParens) args)
        codeParens s = "`(` " <> s <> " `)`"



fct2md :: FunctionDoc -> T.Text
fct2md FunctionDoc{..} =
  "* " <> asCode fct_name <> maybe "" ((" : " <>) . type2md) fct_type
  <> maybe "" (("  \n" <>) . indent 2) fct_descr
  --             ^^ NB trailing whitespace to cause a line break

------------------------------------------------------------

asCode :: T.Text -> T.Text
asCode = enclosedIn "`"
