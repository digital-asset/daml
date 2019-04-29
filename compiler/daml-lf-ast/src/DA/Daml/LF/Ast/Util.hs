-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module DA.Daml.LF.Ast.Util where

import DA.Prelude

import           Control.Lens
import           Control.Lens.Ast
import qualified Data.Graph as G
import           Data.List.Extra (nubSort)
import qualified Data.NameMap as NM

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Optics

dvalName :: DefValue -> ExprValName
dvalName = fst . dvalBinder

dvalType :: DefValue -> Type
dvalType = snd . dvalBinder

chcArgType :: TemplateChoice -> Type
chcArgType = snd . chcArgBinder

topoSortPackage :: Package -> Either [ModuleName] Package
topoSortPackage (Package version mods) = do
  let isLocal (pkgRef, modName) = case pkgRef of
        PRSelf -> Just modName
        PRImport{} -> Nothing
  let modDeps = nubSort . mapMaybe isLocal . toListOf moduleModuleRef
  let modNode mod0 = (mod0, moduleName mod0, modDeps mod0)
  let sccs = G.stronglyConnComp (map modNode (NM.toList mods))
  let isAcyclic = \case
        G.AcyclicSCC mod0 -> Right mod0
        -- NOTE(MH): A module referencing itself is not really a cycle.
        G.CyclicSCC [mod0] -> Right mod0
        G.CyclicSCC modCycle -> Left (map moduleName modCycle)
  Package version . NM.fromList <$> traverse isAcyclic sccs

-- | Unwind a left associative operation like an application node of an AST,
-- e.g., @unwindl ('matching' _EApp)@.
unwindl :: (e -> Either e (e, a)) -> e -> (e, [a])
unwindl p = go []
  where
    go as e0 = case p e0 of
      Right (e1, a) -> go (a:as) e1
      Left  e1      -> (e1, as)

-- | Unwind a right associative operation like an abstraction node of an AST,
-- e.g., @unwindl ('matching' _EAbs)@.
unwindr :: (e -> Either e (x, e)) -> e -> ([x], e)
unwindr p = go []
  where
    go xs e0 = case p e0 of
      Right (x, e1) -> go (x:xs) e1
      Left  e1      -> (reverse xs, e1)

data Arg
  = TmArg Expr
  | TyArg Type

mkEApp :: Expr -> Arg -> Expr
mkEApp e (TmArg a) = ETmApp e a
mkEApp e (TyArg t) = ETyApp e t

_EApp :: Prism' Expr (Expr, Arg)
_EApp = prism' inj proj
  where
    inj (f, a) = case a of
      TmArg e -> ETmApp f e
      TyArg t -> ETyApp f t
    proj = \case
      ETmApp f e -> Just (f, TmArg e)
      ETyApp f t -> Just (f, TyArg t)
      _          -> Nothing

_ETmApps :: Iso' Expr (Expr, [Expr])
_ETmApps = leftSpine _ETmApp

_ETyApps :: Iso' Expr (Expr, [Type])
_ETyApps = leftSpine _ETyApp

_EApps :: Iso' Expr (Expr, [Arg])
_EApps = leftSpine _EApp

_ETmLams :: Iso' Expr ([(ExprVarName, Type)], Expr)
_ETmLams = rightSpine _ETmLam

_ETyLams :: Iso' Expr ([(TypeVarName, Kind)], Expr)
_ETyLams = rightSpine _ETyLam

_ELets :: Iso' Expr ([Binding], Expr)
_ELets = rightSpine _ELet

mkETmApps :: Expr -> [Expr] -> Expr
mkETmApps = curry (review _ETmApps)

mkETyApps :: Expr -> [Type] -> Expr
mkETyApps = curry (review _ETyApps)

mkEApps :: Expr -> [Arg] -> Expr
mkEApps = curry (review _EApps)

mkETmLams :: [(ExprVarName, Type)] -> Expr -> Expr
mkETmLams = curry (review _ETmLams)

mkETyLams :: [(TypeVarName, Kind)] -> Expr -> Expr
mkETyLams = curry (review _ETyLams)

mkELets :: [Binding] -> Expr -> Expr
mkELets = curry (review _ELets)

mkEmptyText :: Expr
mkEmptyText = EBuiltin (BEText "")

mkIf :: Expr -> Expr -> Expr -> Expr
mkIf cond0 then0 else0 =
  ECase cond0
  [ CaseAlternative (CPEnumCon ECTrue ) then0
  , CaseAlternative (CPEnumCon ECFalse) else0
  ]

mkBoolEC :: Bool -> EnumCon
mkBoolEC = \case
  False -> ECFalse
  True  -> ECTrue

mkBool :: Bool -> Expr
mkBool = EBuiltin . BEEnumCon . mkBoolEC

pattern EUnit :: Expr
pattern EUnit = EBuiltin (BEEnumCon ECUnit)

pattern ETrue :: Expr
pattern ETrue = EBuiltin (BEEnumCon ECTrue)

pattern EFalse :: Expr
pattern EFalse = EBuiltin (BEEnumCon ECFalse)

mkNot :: Expr -> Expr
mkNot arg = mkIf arg (mkBool False) (mkBool True)

mkOr :: Expr -> Expr -> Expr
mkOr arg1 arg2 = mkIf arg1 (mkBool True) arg2

mkAnd :: Expr -> Expr -> Expr
mkAnd arg1 arg2 = mkIf arg1 arg2 (mkBool False)

mkAnds :: [Expr] -> Expr
mkAnds [] = mkBool True
mkAnds [x] = x
mkAnds (x:xs) = mkAnd x $ mkAnds xs


alpha, beta :: TypeVarName
-- NOTE(MH): We want to avoid shadowing variables in the environment. That's
-- what the weird names are for.
alpha = Tagged "::alpha::"
beta  = Tagged "::beta::"

tAlpha, tBeta :: Type
tAlpha = TVar alpha
tBeta  = TVar beta


infixr 1 :->

-- | Type constructor for function types.
pattern (:->) :: Type -> Type -> Type
pattern a :-> b = TArrow `TApp` a `TApp` b

pattern TUnit, TBool, TInt64, TDecimal, TText, TTimestamp, TParty, TDate, TArrow :: Type
pattern TUnit       = TBuiltin (BTEnum ETUnit)
pattern TBool       = TBuiltin (BTEnum ETBool)
pattern TInt64      = TBuiltin BTInt64
pattern TDecimal    = TBuiltin BTDecimal
pattern TText       = TBuiltin BTText
pattern TTimestamp  = TBuiltin BTTimestamp
pattern TParty      = TBuiltin BTParty
pattern TDate       = TBuiltin BTDate
pattern TArrow      = TBuiltin BTArrow

pattern TList, TOptional, TMap, TUpdate, TScenario, TContractId :: Type -> Type
pattern TList typ = TApp (TBuiltin BTList) typ
pattern TOptional typ = TApp (TBuiltin BTOptional) typ
pattern TMap typ = TApp (TBuiltin BTMap) typ
pattern TUpdate typ = TApp (TBuiltin BTUpdate) typ
pattern TScenario typ = TApp (TBuiltin BTScenario) typ
pattern TContractId typ = TApp (TBuiltin BTContractId) typ

pattern TMapEntry :: Type -> Type
pattern TMapEntry a = TTuple [(Tagged "key", TText), (Tagged "value", a)]

pattern TConApp :: Qualified TypeConName -> [Type] -> Type
pattern TConApp tcon targs <- (view (leftSpine _TApp) -> (TCon tcon, targs))
  where
    TConApp tcon targs = foldl TApp (TCon tcon) targs

_TList :: Prism' Type Type
_TList = prism' TList $ \case
  TList typ -> Just typ
  _ -> Nothing

_TOptional :: Prism' Type Type
_TOptional = prism' TOptional $ \case
  TOptional typ -> Just typ
  _ -> Nothing

_TUpdate :: Prism' Type Type
_TUpdate = prism' TList $ \case
  TUpdate typ -> Just typ
  _ -> Nothing

_TScenario :: Prism' Type Type
_TScenario = prism' TList $ \case
  TScenario typ -> Just typ
  _ -> Nothing

_TConApp :: Prism' Type (Qualified TypeConName, [Type])
_TConApp = prism' (uncurry TConApp) $ \case
  TConApp tcon targs -> Just (tcon, targs)
  _ -> Nothing

_TForalls :: Iso' Type ([(TypeVarName, Kind)], Type)
_TForalls = rightSpine _TForall

_TApps :: Iso' Type (Type, [Type])
_TApps = leftSpine _TApp

mkTForalls :: [(TypeVarName, Kind)] -> Type -> Type
mkTForalls = curry (review _TForalls)

mkTFuns :: [Type] -> Type -> Type
mkTFuns ts t = foldr (:->) t ts

mkTApps :: Type -> [Type] -> Type
mkTApps = curry (review _TApps)


typeConAppToType :: TypeConApp -> Type
typeConAppToType (TypeConApp tcon targs) = TConApp tcon targs

packListLit :: Type -> [Expr] -> Expr
packListLit t = foldr (ECons t) (ENil t)

unpackListLit :: Expr -> Maybe [Expr]
unpackListLit e0 = case unwindr p e0 of
  (es, ENil{}) -> Just es
  _ -> Nothing
  where
    p = \case
      ECons _ e es -> Right (e, es)
      e -> Left e

-- Compatibility type and functions

data Definition
  = DDataType DefDataType
  | DValue DefValue
  | DTemplate Template

moduleFromDefinitions :: ModuleName -> Maybe FilePath -> FeatureFlags -> [Definition] -> Module
moduleFromDefinitions name path flags defs =
  let (dats, vals, tpls) = partitionDefinitions defs
  in  Module name path flags (NM.fromList dats) (NM.fromList vals) (NM.fromList tpls)

partitionDefinitions :: [Definition] -> ([DefDataType], [DefValue], [Template])
partitionDefinitions = foldr f ([], [], [])
  where
    f = \case
      DDataType d -> over _1 (d:)
      DValue v    -> over _2 (v:)
      DTemplate t -> over _3 (t:)

moduleDefinitions :: Module -> [Definition]
moduleDefinitions (Module _name _path _flags dats vals tpls) =
  concat
  [ map DDataType (NM.toList dats)
  , map DValue (NM.toList vals)
  , map DTemplate (NM.toList tpls)
  ]

_moduleDefinitions :: Lens' Module [Definition]
_moduleDefinitions f mod0@(Module name path flags _ _ _) =
  moduleFromDefinitions name path flags <$> f (moduleDefinitions mod0)

makePrisms ''Definition
