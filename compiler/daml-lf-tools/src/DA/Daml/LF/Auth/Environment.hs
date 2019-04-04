-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | An environment giving fast access to templates, choices and values
module DA.Daml.LF.Auth.Environment(
    Environment,
    mkEnvironment,
    withTemplate
    ) where

import           DA.Daml.LF.Ast
import           DA.Daml.LF.Auth.Type
import qualified Data.HashMap.Strict as HMS
import qualified Data.NameMap as NM

data Environment = Environment
    {envTemplates :: HMS.HashMap (Qualified TypeConName) Template
    ,envValues :: HMS.HashMap (Qualified ExprValName) Expr
    }

instance Semigroup Environment where
    a <> b = Environment (envTemplates a <> envTemplates b) (envValues a <> envValues b)
instance Monoid Environment where
    mempty = Environment HMS.empty HMS.empty
    mappend = (<>)

mkEnvironment :: [(PackageId, Package)] -> Package -> Environment
mkEnvironment pkgs pkg = mconcat $ f PRSelf pkg : [f (PRImport a) b | (a,b) <- pkgs]
    where
        f pref (Package _version (NM.toList -> ms)) = Environment
            (HMS.fromList [(Qualified pref moduleName $ tplTypeCon t, t) | Module{..} <- ms, t <- NM.toList moduleTemplates])
            (HMS.fromList [(Qualified pref moduleName (dvalName v), dvalBody v) | Module{..} <- ms, v <- NM.toList moduleValues])


withTemplate :: Environment -> Qualified TypeConName -> (Template -> [Error]) -> [Error]
withTemplate env t f = case HMS.lookup t $ envTemplates env of
    Nothing -> [EUnknownTemplate t]
    Just x -> f x
