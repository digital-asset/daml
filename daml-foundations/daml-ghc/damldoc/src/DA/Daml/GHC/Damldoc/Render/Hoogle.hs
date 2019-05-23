-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.GHC.Damldoc.Render.Hoogle
  ( renderSimpleHoogle
  ) where

import DA.Daml.GHC.Damldoc.Types
import DA.Daml.GHC.Damldoc.Render.Anchor

import           Data.Maybe
import qualified Data.Text as T

-- | Convert a markdown comment into hoogle text.
hooglify :: Maybe Markdown -> [T.Text]
hooglify Nothing = []
hooglify (Just md) =
    case T.lines md of
        [] -> []
        (x:xs) -> ("-- | " <>  x)
            : map ("--   " <>) xs

urlTag :: Anchor -> T.Text
urlTag = ("@url https://docs.daml.com/daml/reference/base.html#" <>)

renderSimpleHoogle :: ModuleDoc -> T.Text
renderSimpleHoogle ModuleDoc{..}
  | null md_classes && null md_adts &&
    null md_functions && isNothing md_descr = T.empty
renderSimpleHoogle ModuleDoc{..} = T.unlines . concat $
  [ hooglify md_descr
  , [ urlTag (moduleAnchor md_name)
    , "module " <> md_name
    , "" ]
  -- , concatMap tmpl2hoogle md_templates    -- just ignore templates
  , concatMap (adt2hoogle md_name) md_adts
  , concatMap (cls2hoogle md_name) md_classes
  , concatMap (fct2hoogle md_name) md_functions
  ]

adt2hoogle :: Modulename -> ADTDoc -> [T.Text]
adt2hoogle md_name TypeSynDoc{..} = concat
    [ hooglify ad_descr
    , [ urlTag (typeAnchor md_name ad_name)
      , T.unwords ("type" : wrapOp ad_name : ad_args ++ ["=", type2hoogle ad_rhs])
      , "" ]
    ]
adt2hoogle md_name ADTDoc{..} = concat
    [ hooglify ad_descr
    , [ urlTag (dataAnchor md_name ad_name)
      , T.unwords ("data" : wrapOp ad_name : ad_args)
      , "" ]
    , concatMap (adtConstr2hoogle md_name ad_name) ad_constrs
    ]

adtConstr2hoogle :: Modulename -> Typename -> ADTConstr -> [T.Text]
adtConstr2hoogle md_name typename PrefixC{..} = concat
    [ hooglify ac_descr
    , [ urlTag (constrAnchor md_name ac_name)
      , T.unwords
            [ wrapOp ac_name
            , "::"
            , type2hoogle $ TypeFun (ac_args ++ [TypeApp typename []])
            ]
      , "" ]
    ]
adtConstr2hoogle md_name typename RecordC{..} = concat
    [ hooglify ac_descr
    , [ urlTag (constrAnchor md_name ac_name)
      , T.unwords
            [ wrapOp ac_name
            , "::"
            , type2hoogle $ TypeFun
                (map fd_type ac_fields ++ [TypeApp typename []])
            ]
      , "" ]
    , concatMap (fieldDoc2hoogle typename) ac_fields
    ]

fieldDoc2hoogle :: Typename -> FieldDoc -> [T.Text]
fieldDoc2hoogle typename FieldDoc{..} = concat
    [ hooglify fd_descr
    , [ T.unwords
            [ "[" <> wrapOp fd_name <> "]"
            , "::"
            , wrapOp typename
            , "->"
            , type2hoogle fd_type
            ]
      , "" ]
    ]


cls2hoogle :: Modulename -> ClassDoc -> [T.Text]
cls2hoogle md_name ClassDoc{..} = concat
    [ hooglify cl_descr
    , [ urlTag (classAnchor md_name cl_name)
      , T.unwords $ ["class"]
                 ++ maybe [] ((:["=>"]) . type2hoogle) cl_super
                 ++ wrapOp cl_name : cl_args
      , "" ]
    , concatMap (fct2hoogle md_name . addToContext) cl_functions
    ]
  where
    addToContext :: FunctionDoc -> FunctionDoc
    addToContext fndoc = fndoc { fct_context = newContext (fct_context fndoc) }

    newContext :: Maybe Type -> Maybe Type
    newContext = \case
        Nothing -> Just contextTy
        Just (TypeTuple ctx) -> Just (TypeTuple (contextTy : ctx))
        Just ctx -> Just (TypeTuple [contextTy, ctx])

    contextTy :: Type
    contextTy = TypeApp cl_name [TypeApp arg [] | arg <- cl_args]

fct2hoogle :: Modulename -> FunctionDoc -> [T.Text]
fct2hoogle md_name FunctionDoc{..} = concat
    [ hooglify fct_descr
    , [ urlTag (functionAnchor md_name fct_name fct_type)
      , T.unwords $ [wrapOp fct_name, "::"]
                 ++ maybe [] ((:["=>"]) . type2hoogle) fct_context
                 ++ maybe ["_"] ((:[]) . type2hoogle) fct_type -- FIXME(FM): what to do when missing type?
      , "" ]
    ]

-- | If name is an operator, wrap it.
wrapOp :: T.Text -> T.Text
wrapOp t =
    if T.null t || isIdChar (T.head t)
        then t
        else codeParens t
  where
    isIdChar :: Char -> Bool
    isIdChar c = ('A' <= c && c <= 'Z')
              || ('a' <= c && c <= 'z')
              || ('_' == c)

-- | Render a type. Nested types are put in parentheses where appropriate.
type2hoogle :: Type -> T.Text
type2hoogle = t2hg id id

-- | Render a type with 2 helper functions. The first is applied to
-- rendered function types. The second is applied to rendered
-- type applications with one or more arguments.
t2hg :: (T.Text -> T.Text) -> (T.Text -> T.Text) -> Type -> T.Text
t2hg f _ (TypeFun ts) = f $
    T.intercalate " -> " $ map (t2hg codeParens id) ts
t2hg _ _ (TypeList t1) =
    "[" <> t2hg id id t1 <> "]"
t2hg _ _ (TypeTuple ts) =
    "(" <> T.intercalate ", " (map (t2hg id id) ts) <> ")"
t2hg _ _ (TypeApp n []) = n
t2hg _ f (TypeApp name args) = f $
    T.unwords (wrapOp name : map (t2hg codeParens codeParens) args)

codeParens :: T.Text -> T.Text
codeParens s = "(" <> s <> ")"
