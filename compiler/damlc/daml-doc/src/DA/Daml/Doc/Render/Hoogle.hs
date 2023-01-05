-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Doc.Render.Hoogle
    ( HoogleEnv (..)
    , renderSimpleHoogle
    ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render.Util

import Data.Maybe
import qualified Data.HashMap.Strict as HMS
import qualified Data.Text as T

newtype HoogleEnv = HoogleEnv
    { he_anchorTable :: HMS.HashMap Anchor T.Text
    }

-- | Convert a markdown comment into hoogle text.
hooglify :: Maybe DocText -> [T.Text]
hooglify Nothing = []
hooglify (Just md) =
    case T.lines (unDocText md) of
        [] -> []
        (x:xs) -> ("-- | " <>  x)
            : map ("--   " <>) xs

urlTag :: HoogleEnv -> Maybe Anchor -> T.Text
urlTag env anchorM = fromMaybe "" $ do
    anchor <- anchorM
    url <- HMS.lookup anchor (he_anchorTable env)
    pure ("@url " <> url)

renderSimpleHoogle :: HoogleEnv -> ModuleDoc -> T.Text
renderSimpleHoogle _env ModuleDoc{..}
  | null md_classes && null md_adts &&
    null md_functions && isNothing md_descr = T.empty
renderSimpleHoogle env ModuleDoc{..} = T.unlines . concat $
  [ hooglify md_descr
  , [ urlTag env md_anchor
    , "module " <> unModulename md_name
    , "" ]
  , concatMap (adt2hoogle env) md_adts
  , concatMap (cls2hoogle env) md_classes
  , concatMap (fct2hoogle env) md_functions
  ]

adt2hoogle :: HoogleEnv ->  ADTDoc -> [T.Text]
adt2hoogle env TypeSynDoc{..} = concat
    [ hooglify ad_descr
    , [ urlTag env ad_anchor
      , T.unwords ("type" : wrapOp (unTypename ad_name) :
          ad_args ++ ["=", type2hoogle ad_rhs])
      , "" ]
    ]
adt2hoogle env ADTDoc{..} = concat
    [ hooglify ad_descr
    , [ urlTag env ad_anchor
      , T.unwords ("data" : wrapOp (unTypename ad_name) : ad_args)
      , "" ]
    , concatMap (adtConstr2hoogle env ad_name) ad_constrs
    ]

adtConstr2hoogle :: HoogleEnv -> Typename -> ADTConstr -> [T.Text]
adtConstr2hoogle env typename PrefixC{..} = concat
    [ hooglify ac_descr
    , [ urlTag env ac_anchor
      , T.unwords
            [ wrapOp (unTypename ac_name)
            , "::"
            , type2hoogle $ TypeFun (ac_args ++ [TypeApp Nothing typename []])
            ]
      , "" ]
    ]
adtConstr2hoogle env typename RecordC{..} = concat
    [ hooglify ac_descr
    , [ urlTag env ac_anchor
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


cls2hoogle :: HoogleEnv -> ClassDoc -> [T.Text]
cls2hoogle env ClassDoc{..} = concat
    [ hooglify cl_descr
    , [ urlTag env cl_anchor
      , T.unwords $ ["class"]
                 ++ contextToHoogle cl_super
                 ++ wrapOp (unTypename cl_name) : cl_args
      , "" ]
    , concatMap (classMethod2hoogle env) cl_methods
    ]

classMethod2hoogle :: HoogleEnv -> ClassMethodDoc -> [T.Text]
classMethod2hoogle _env ClassMethodDoc{..} | cm_isDefault = [] -- hide default methods from hoogle search
classMethod2hoogle env ClassMethodDoc{..} = concat
    [ hooglify cm_descr
    , [ urlTag env cm_anchor
      , T.unwords . concat $
          [ [wrapOp (unFieldname cm_name), "::"]
          , contextToHoogle cm_globalContext
          , [type2hoogle cm_type]
          ]
      , "" ]
    ]

fct2hoogle :: HoogleEnv -> FunctionDoc -> [T.Text]
fct2hoogle env FunctionDoc{..} = concat
    [ hooglify fct_descr
    , [ urlTag env fct_anchor
      , T.unwords . concat $
          [ [wrapOp (unFieldname fct_name), "::"]
          , contextToHoogle fct_context
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
t2hg _ _ (TypeTuple ts) = typeTupleToHoogle ts
t2hg _ _ (TypeApp _ n []) = unTypename n
t2hg _ f (TypeApp _ name args) = f $
    T.unwords (wrapOp (unTypename name) : map (t2hg inParens inParens) args)
t2hg _ _ (TypeLit lit) = lit

typeTupleToHoogle :: [Type] -> T.Text
typeTupleToHoogle ts = "(" <> T.intercalate ", " (map type2hoogle ts) <> ")"

contextToHoogle :: Context -> [T.Text]
contextToHoogle = \case
  Context [] -> []
  Context ts -> [typeTupleToHoogle ts, "=>"]
