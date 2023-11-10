-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Doc.Render.Hoogle
    ( HoogleEnv (..)
    , renderSimpleHoogle
    ) where

import DA.Daml.Doc.Render.Util
import DA.Daml.Doc.Types
import Data.HashMap.Strict qualified as HMS
import Data.Maybe
import Data.Text qualified as T

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
    null md_functions && isNothing md_descr &&
    null md_templates && null md_interfaces = T.empty
renderSimpleHoogle env ModuleDoc{..} = T.unlines . concat $
  [ hooglify md_descr
  , [ urlTag env md_anchor
    , "module " <> unModulename md_name
    , "" ]
  , concatMap (adt2hoogle env Nothing) md_adts
  , concatMap (cls2hoogle env) md_classes
  , concatMap (fct2hoogle env) md_functions
  , concatMap (template2hoogle env) md_templates
  , concatMap (interface2hoogle env) md_interfaces
  ]

adt2hoogle :: HoogleEnv -> Maybe T.Text -> ADTDoc -> [T.Text]
adt2hoogle env _ TypeSynDoc{..} = concat
    [ hooglify ad_descr
    , [ urlTag env ad_anchor
      , T.unwords ("type" : unwrapTypename ad_name :
          ad_args ++ ["=", type2hoogle ad_rhs])
      , "" ]
    ]
adt2hoogle env dataConstraints ADTDoc{..} = concat
    [ hooglify ad_descr
    , [ urlTag env ad_anchor
      , T.unwords ("data" : (constraintsText <> (unwrapTypename ad_name : ad_args)))
      , "" ]
    , concatMap (adtConstr2hoogle env ad_name) ad_constrs
    ]
  where
    constraintsText = maybe [] (\c -> ["(" <> c <> ")", "=>"]) dataConstraints

adtConstr2hoogle :: HoogleEnv -> Typename -> ADTConstr -> [T.Text]
adtConstr2hoogle env typename PrefixC{..} = concat
    [ hooglify ac_descr
    , [ urlTag env ac_anchor
      , T.unwords
            [ unwrapTypename ac_name
            , "::"
            , type2hoogle $ TypeFun (ac_args ++ [TypeApp Nothing typename []])
            ]
      , "" ]
    ]
adtConstr2hoogle env typename RecordC{..} = concat
    [ hooglify ac_descr
    , [ urlTag env ac_anchor
      , T.unwords
            [ unwrapTypename ac_name
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
            [ wrapOp (unFieldname fd_name)
            , "::"
            , unwrapTypename typename
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
                 ++ unwrapTypename cl_name : cl_args
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

template2hoogle :: HoogleEnv -> TemplateDoc -> [T.Text]
template2hoogle env TemplateDoc {..} = concat
    [ adt2hoogle env (Just $ "Template " <> typeName) adtDoc
    , concatMap (choice2hoogle env typeName) td_choices
    ]
  where
    adtDoc = ADTDoc
      td_anchor
      td_name
      td_descr
      []
      [RecordC td_anchor td_name td_descr td_payload]
      Nothing
    typeName = unwrapTypename td_name

interface2hoogle :: HoogleEnv -> InterfaceDoc -> [T.Text]
interface2hoogle env InterfaceDoc {..} = concat
    [ adt2hoogle env (Just $ type2hoogle constraintType) adtDoc
    , concatMap (choice2hoogle env typeName) if_choices
    ]
  where
    adtDoc = ADTDoc if_anchor if_name if_descr [] [] Nothing
    typeName = unwrapTypename if_name
    constraintType = TypeApp Nothing "HasInterfaceView" [TypeLit typeName, maybe (TypeTuple []) unInterfaceViewtypeDoc if_viewtype]

choice2hoogle :: HoogleEnv -> T.Text -> ChoiceDoc -> [T.Text]
choice2hoogle _ _ ChoiceDoc {..} | cd_name == "Archive" = []
choice2hoogle env templateName ChoiceDoc {..} =
    adt2hoogle env (Just $ type2hoogle constraintType) $ ADTDoc
      cd_anchor
      cd_name
      cd_descr
      []
      [RecordC cd_anchor cd_name cd_descr cd_fields]
      Nothing
  where
    choiceName = unwrapTypename cd_name
    constraintType = TypeApp Nothing "Choice" [TypeLit templateName, TypeLit choiceName, cd_type]

      
-- | Render a type. Nested types are put in parentheses where appropriate.
type2hoogle :: Type -> T.Text
type2hoogle = t2hg id id

unwrapTypename :: Typename -> T.Text
unwrapTypename = wrapOp . unTypename

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
    T.unwords (unwrapTypename name : map (t2hg inParens inParens) args)
t2hg _ _ (TypeLit lit) = lit

typeTupleToHoogle :: [Type] -> T.Text
typeTupleToHoogle ts = "(" <> T.intercalate ", " (map type2hoogle ts) <> ")"

contextToHoogle :: Context -> [T.Text]
contextToHoogle = \case
  Context [] -> []
  Context ts -> [typeTupleToHoogle ts, "=>"]
