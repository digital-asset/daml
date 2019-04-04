-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Ast.Subst where

import Control.Lens (_2)
import Control.Monad.Error.Class (throwError)

import DA.Daml.LF.Ast.Base

-- | Apply a substitution. Fails if the type to subsitute in contains any of the
-- following:
--
-- * type variables that are not in the domain of the substitution
--
-- * forall quantifications
substAllRank1Type :: (TypeVarName -> Maybe Type) -> Type -> Either String Type
substAllRank1Type sigma = go
  where
    go = \case
      TVar v
        | Just t <- sigma v -> pure t
        | otherwise         -> throwError ("TVar " ++ show v)
      t@TCon{} -> pure t
      TApp tf ta -> TApp <$> go tf <*> go ta
      TBuiltin b -> pure (TBuiltin b)
      TForall{} -> throwError "TForall"
      TTuple fs -> TTuple <$> (traverse . _2) go fs
