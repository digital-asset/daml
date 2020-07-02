-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Term substitions for DAML LF static verification
module DA.Daml.LF.Verify.Subst
  ( InstPR(..)
  ) where

-- TODO: Replace this with the existing substitution library.

import Control.Lens hiding (Context)
import Data.Bifunctor
import DA.Daml.LF.Ast

-- | A class covering the data types containing package references which can be
-- instantiated..
class InstPR a where
  -- | Instantiate `PRSelf` with the given package reference.
  instPRSelf :: PackageRef
    -- ^ The package reference to substitute with.
    -> a
    -- ^ The data type to substitute in.
    -> a

instance InstPR (Qualified a) where
  instPRSelf pac qx@(Qualified pac' mod x) = case pac' of
    PRSelf -> Qualified pac mod x
    _ -> qx

instance InstPR Expr where
  instPRSelf pac = \case
    EVal val -> EVal (instPRSelf pac val)
    ERecCon t fs -> ERecCon (instPRSelf pac t) $ map (over _2 (instPRSelf pac)) fs
    ERecProj t f e -> ERecProj (instPRSelf pac t) f $ instPRSelf pac e
    ERecUpd t f e1 e2 -> ERecUpd (instPRSelf pac t) f (instPRSelf pac e1) (instPRSelf pac e2)
    EVariantCon t v e -> EVariantCon (instPRSelf pac t) v (instPRSelf pac e)
    EStructCon fs -> EStructCon $ map (over _2 (instPRSelf pac)) fs
    EStructProj f e -> EStructProj f (instPRSelf pac e)
    EStructUpd f e1 e2 -> EStructUpd f (instPRSelf pac e1) (instPRSelf pac e2)
    ETmApp e1 e2 -> ETmApp (instPRSelf pac e1) (instPRSelf pac e2)
    ETyApp e t -> ETyApp (instPRSelf pac e) t
    ETmLam b e -> ETmLam b (instPRSelf pac e)
    ETyLam b e -> ETyLam b (instPRSelf pac e)
    ECase e cs -> ECase (instPRSelf pac e)
      $ map (\CaseAlternative{..} -> CaseAlternative altPattern (instPRSelf pac altExpr)) cs
    ELet Binding{..} e -> ELet (Binding bindingBinder $ instPRSelf pac bindingBound)
      (instPRSelf pac e)
    ECons t e1 e2 -> ECons t (instPRSelf pac e1) (instPRSelf pac e2)
    ESome t e -> ESome t (instPRSelf pac e)
    EToAny t e -> EToAny t (instPRSelf pac e)
    EFromAny t e -> EFromAny t (instPRSelf pac e)
    EUpdate u -> EUpdate $ instPRSelf pac u
    ELocation l e -> ELocation l (instPRSelf pac e)
    e -> e

instance InstPR Type where
  instPRSelf pac = \case
    TCon tc -> TCon (instPRSelf pac tc)
    TSynApp tc ts -> TSynApp (instPRSelf pac tc) $ map (instPRSelf pac) ts
    TApp t1 t2 -> TApp (instPRSelf pac t1) (instPRSelf pac t2)
    TForall b t -> TForall b (instPRSelf pac t)
    TStruct fs -> TStruct $ map (second (instPRSelf pac)) fs
    t -> t

instance InstPR TypeConApp where
  instPRSelf pac (TypeConApp tc ts) = TypeConApp (instPRSelf pac tc)
    $ map (instPRSelf pac) ts
instance InstPR TypeConName where
  instPRSelf _ tc = tc
instance InstPR TypeSynName where
  instPRSelf _ tc = tc

instance InstPR Update where
  instPRSelf pac = \case
    UPure t e -> UPure t (instPRSelf pac e)
    UBind Binding{..} e -> UBind (Binding bindingBinder $ instPRSelf pac bindingBound)
      (instPRSelf pac e)
    UCreate tem arg -> UCreate (instPRSelf pac tem) (instPRSelf pac arg)
    UExercise tem ch cid act arg -> UExercise (instPRSelf pac tem) ch
      (instPRSelf pac cid) (instPRSelf pac <$> act) (instPRSelf pac arg)
    UFetch tem cid -> UFetch (instPRSelf pac tem) (instPRSelf pac cid)
    UEmbedExpr t e -> UEmbedExpr t (instPRSelf pac e)
    u -> u

instance InstPR DefDataType where
  instPRSelf pac dat@DefDataType{..} = dat{dataCons = instPRSelf pac dataCons}

instance InstPR DataCons where
  instPRSelf pac = \case
    DataRecord fs -> DataRecord $ map (second (instPRSelf pac)) fs
    DataVariant fs -> DataVariant $ map (second (instPRSelf pac)) fs
    DataEnum fs -> DataEnum fs
