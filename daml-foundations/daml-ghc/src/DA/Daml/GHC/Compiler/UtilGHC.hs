-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE PatternSynonyms #-}
{-# OPTIONS_GHC -Wno-missing-fields #-} -- to enable prettyPrint
{-# OPTIONS_GHC -Wno-orphans #-}

-- | GHC utility functions. Importantly, code using our GHC should never:
--
-- * Call runGhc, use runGhcFast instead. It's faster and doesn't require config we don't have.
--
-- * Call setSessionDynFlags, use modifyDynFlags instead. It's faster and avoids loading packages.
module DA.Daml.GHC.Compiler.UtilGHC where

import           "ghc-lib-parser" Class
import           "ghc-lib" GHC                         hiding (convertLit)
import           "ghc-lib" GhcPlugins                  as GHC hiding (PackageState, fst3, (<>))

import           Data.Generics.Uniplate.Data
import           Data.List
import qualified Data.Set as Set
import           Data.Tuple.Extra

----------------------------------------------------------------------
-- GHC utility functions

is :: NamedThing a => a -> String
is = getOccString

pattern Is :: NamedThing a => String -> a
pattern Is x <- (is -> x)

pattern VarIs :: String -> GHC.Expr Var
pattern VarIs x <- Var (Is x)

pattern TypeCon :: TyCon -> [GHC.Type] -> GHC.Type
pattern TypeCon c ts <- (splitTyConApp_maybe -> Just (c, ts))
  where TypeCon = mkTyConApp

pattern StrLitTy :: String -> Type
pattern StrLitTy x <- (fmap unpackFS . isStrLitTy -> Just x)

subst :: [(TyVar, GHC.Type)] -> GHC.Type -> GHC.Type
subst env = transform $ \t ->
    case getTyVar_maybe t of
        Just v | Just t <- lookup v env -> t
        _                               -> t

fromApps :: GHC.Expr b -> [Arg b]
fromApps (App x y) = fromApps x ++ [y]
fromApps x         = [x]

fromCast :: GHC.Expr a -> GHC.Expr a
fromCast (Cast x _) = x
fromCast x          = x

isType_maybe :: GHC.Expr b -> Maybe Type
isType_maybe = \case
    Type t -> Just t
    _ -> Nothing

isSingleConType :: TyCon -> Bool
isSingleConType t = case algTyConRhs t of
    DataTyCon [_] _ _ -> True
    NewTyCon{} -> True
    TupleTyCon{} -> True
    _ -> False

-- Pretty printing is very expensive, so clone the logic for when to add unique suffix
varPrettyPrint :: Var -> String
varPrettyPrint (varName -> x) = is x ++ (if isSystemName x then "_" ++ show (nameUnique x) else "")

defaultLast :: [Alt Var] -> [Alt Var]
defaultLast = uncurry (++) . partition ((/=) DEFAULT . fst3)

isLitAlt :: Alt Var -> Bool
isLitAlt (LitAlt{},_,_) = True
isLitAlt _              = False

defaultMethods :: CoreModule -> Set.Set Name
defaultMethods core = Set.fromList
  [ name
  | ATyCon (tyConClass_maybe -> Just class_) <- nameEnvElts (cm_types core)
  , (_id, Just (name, VanillaDM)) <- classOpItems class_
  ]

untick :: GHC.Expr b -> GHC.Expr b
untick = \case
    Tick _ e -> untick e
    e -> e

-- | An argument to a function together with the location information of the
-- _function_.
type LArg b = (Maybe RealSrcSpan, Arg b)

pattern LType :: Type -> LArg b
pattern LType x <- (_, Type x)

pattern LExpr :: GHC.Expr b -> LArg b
pattern LExpr x <- (_, x)
