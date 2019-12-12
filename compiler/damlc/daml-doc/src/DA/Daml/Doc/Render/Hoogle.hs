-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Doc.Render.Hoogle
  ( renderSimpleHoogle
  ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render.Util

import           Data.Maybe
import qualified Data.Text as T

-- | Convert a markdown comment into hoogle text.
hooglify :: Maybe DocText -> [T.Text]
hooglify Nothing = []
hooglify (Just md) =
    case T.lines (unDocText md) of
        [] -> []
        (x:xs) -> ("-- | " <>  x)
            : map ("--   " <>) xs

urlTag :: Maybe Anchor -> T.Text
urlTag Nothing = ""
urlTag (Just (Anchor t)) = "@url https://docs.daml.com/daml/reference/base.html#" <> t

renderSimpleHoogle :: ModuleDoc -> T.Text
renderSimpleHoogle ModuleDoc{..}
  | null md_classes && null md_adts &&
    null md_functions && isNothing md_descr = T.empty
renderSimpleHoogle ModuleDoc{..} = T.unlines . concat $
  [ hooglify md_descr
  , [ urlTag md_anchor
    , "module " <> unModulename md_name
    , "" ]
  , concatMap adt2hoogle md_adts
  , concatMap cls2hoogle md_classes
  , concatMap fct2hoogle md_functions
  ]

adt2hoogle :: ADTDoc -> [T.Text]
adt2hoogle TypeSynDoc{..} = concat
    [ hooglify ad_descr
    , [ urlTag ad_anchor
      , T.unwords ("type" : wrapOp (unTypename ad_name) :
          ad_args ++ ["=", type2hoogle ad_rhs])
      , "" ]
    ]
adt2hoogle ADTDoc{..} = concat
    [ hooglify ad_descr
    , [ urlTag ad_anchor
      , T.unwords ("data" : wrapOp (unTypename ad_name) : ad_args)
      , "" ]
    , concatMap (adtConstr2hoogle ad_name) ad_constrs
    ]

adtConstr2hoogle :: Typename -> ADTConstr -> [T.Text]
adtConstr2hoogle typename PrefixC{..} = concat
    [ hooglify ac_descr
    , [ urlTag ac_anchor
      , T.unwords
            [ wrapOp (unTypename ac_name)
            , "::"
            , type2hoogle $ TypeFun (ac_args ++ [TypeApp Nothing typename []])
            ]
      , "" ]
    ]
adtConstr2hoogle typename RecordC{..} = concat
    [ hooglify ac_descr
    , [ urlTag ac_anchor
      , T.unwords
            [ wrapOp (unTypename ac_name)
            , "::"
            , type2hoogle $ TypeFun
                (map fd_type ac_fields ++ [TypeApp Nothing typename []])
            ]
      , "" ]
    , concatMap (fieldDoc2hoogle typename) ac_fields
    ]

fieldDoc2hoogle :: Typename -> FieldDoc -> [T.Text]
fieldDoc2hoogle typename FieldDoc{..} = concat
    [ hooglify fd_descr
    , [ T.unwords
            [ "[" <> wrapOp (unFieldname fd_name) <> "]"
            , "::"
            , wrapOp (unTypename typename)
            , "->"
            , type2hoogle fd_type
            ]
      , "" ]
    ]


cls2hoogle :: ClassDoc -> [T.Text]
cls2hoogle ClassDoc{..} = concat
    [ hooglify cl_descr
    , [ urlTag cl_anchor
      , T.unwords $ ["class"]
                 ++ maybe [] ((:["=>"]) . type2hoogle) cl_super
                 ++ wrapOp (unTypename cl_name) : cl_args
      , "" ]
    , concatMap classMethod2hoogle cl_methods
    ]

classMethod2hoogle :: ClassMethodDoc -> [T.Text]
classMethod2hoogle ClassMethodDoc{..} | cm_isDefault = [] -- hide default methods from hoogle search
classMethod2hoogle ClassMethodDoc{..} = concat
    [ hooglify cm_descr
    , [ urlTag cm_anchor
      , T.unwords . concat $
          [ [wrapOp (unFieldname cm_name), "::"]
          , maybe [] ((:["=>"]) . type2hoogle) cm_globalContext
          , [type2hoogle cm_type]
          ]
      , "" ]
    ]

fct2hoogle :: FunctionDoc -> [T.Text]
fct2hoogle FunctionDoc{..} = concat
    [ hooglify fct_descr
    , [ urlTag fct_anchor
      , T.unwords . concat $
          [ [wrapOp (unFieldname fct_name), "::"]
          , maybe [] ((:["=>"]) . type2hoogle) fct_context
          , [type2hoogle fct_type]
          ]
      , "" ]
    ]

-- | Render a type. Nested types are put in parentheses where appropriate.
type2hoogle :: Type -> T.Text
type2hoogle = t2hg id id

-- | Render a type with 2 helper functions. The first is applied to
-- rendered function types. The second is applied to rendered
-- type applications with one or more arguments.
t2hg :: (T.Text -> T.Text) -> (T.Text -> T.Text) -> Type -> T.Text
t2hg f _ (TypeFun ts) = f $
    T.intercalate " -> " $ map (t2hg inParens id) ts
t2hg _ _ (TypeList t1) =
    "[" <> t2hg id id t1 <> "]"
t2hg _ _ (TypeTuple ts) =
    "(" <> T.intercalate ", " (map (t2hg id id) ts) <> ")"
t2hg _ _ (TypeApp _ n []) = unTypename n
t2hg _ f (TypeApp _ name args) = f $
    T.unwords (wrapOp (unTypename name) : map (t2hg inParens inParens) args)
t2hg _ _ (TypeLit lit) = lit
