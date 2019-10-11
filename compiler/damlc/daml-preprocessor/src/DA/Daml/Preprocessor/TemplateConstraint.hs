-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Every `template T a_1 a_n` gets desugared into a `class TInstance a_1 ... a_n`,
-- an instance
-- ```
-- (*)    instance TInstance a_1 ... a_n => Template (T a_1 ... a_n)
-- ```
-- and instances `instance TInstance a_1 ... a_n => Choice (T a_1 ... a_n) C R`
-- for each choice `C`.
--
-- Thus, a _generic_ exercise of a choice on `T t_1 ... t_n` needs to have the
-- `TInstance t_1 ... t_n` constraint in scope. However, we want to keep the
-- existence of the `TInstance` class an implementation detail and not expose
-- it to our users. Instead we want our users to add a `Template (T t_1 ... t_n)`
-- constraint when they want to perform the generic exercise.
--
-- Due to the (*) instance above, the constraint `Template (T t_1 ... t_n)`
-- is satisfied if and only if the constraint `TInstance t_1 ... t_n` is
-- satisfied. For the intent described above it would be necessary that GHC
-- conlcudes the latter from the former. Unfortunately, GHC's type system only
-- allows for concluding the former from the latter.
--
-- Thus, we add a preprocessing step which rewrites all constraints of
-- the form `Template (T t_1 ... t_n)` into `TInstance t_1 ... t_n`.
module DA.Daml.Preprocessor.TemplateConstraint (
    templateConstraintPreprocessor
    ) where

import "ghc-lib" GHC
import "ghc-lib-parser" BasicTypes
import "ghc-lib-parser" OccName
import "ghc-lib-parser" RdrName

import Data.Generics.Uniplate.Data

templateConstraintPreprocessor :: ParsedSource -> ParsedSource
templateConstraintPreprocessor = fmap onModule

onModule :: HsModule GhcPs -> HsModule GhcPs
onModule = transformBi onType . transformBi onTyClDecl

-- | The contexts of class and data definitions do not live inside an
-- `HsType` but in their respective AST nodes. That's why we need to chase
-- them down separately.
onTyClDecl :: TyClDecl GhcPs -> TyClDecl GhcPs
onTyClDecl decl = case decl of
    ClassDecl{tcdCtxt} -> decl{tcdCtxt = fmap onContext tcdCtxt}
    DataDecl{tcdDataDefn = defn@HsDataDefn{dd_ctxt}} -> decl{tcdDataDefn = defn{dd_ctxt = fmap onContext dd_ctxt}}
    _ -> decl

onType :: HsType GhcPs-> HsType GhcPs
onType = \case
    HsQualTy ext ctxt t -> HsQualTy ext (fmap onContext ctxt) (fmap onType t)
    t -> descend onType t

onContext :: HsContext GhcPs -> HsContext GhcPs
onContext = map (fmap onContext1)
  where
    onContext1 :: HsType GhcPs -> HsType GhcPs
    onContext1 = \case
        HsAppTy _ (L _ (HsTyVar _ _ (L _ (occNameString . rdrNameOcc -> "Template")))) (L _ t)
            | Just t' <- instantifyType t
            -> t'
        HsParTy ext t -> HsParTy ext (fmap onContext1 t)
        t -> t

-- | Check if the type is of the form `T t_1 ... t_n` for some type constructor
-- `T` and return the constraint `TInstance t_1 ... t_n` in case it is.
instantifyType :: HsType GhcPs -> Maybe (HsType GhcPs)
instantifyType = \case
    HsTyVar ext NotPromoted (L loc x) -> HsTyVar ext NotPromoted . L loc <$> instantifyRdrName x
    HsAppTy ext (L loc t1) t2 -> (\t1' -> HsAppTy ext (L loc t1') t2) <$> instantifyType t1
    HsParTy ext (L loc t) -> HsParTy ext . L loc <$> instantifyType t
    _ -> Nothing

instantifyRdrName :: RdrName -> Maybe RdrName
instantifyRdrName = \case
    Unqual occ -> Unqual <$> instantifyOccName occ
    Qual m occ -> Qual m <$> instantifyOccName occ
    Orig{}-> Nothing
    Exact{} -> Nothing

instantifyOccName :: OccName -> Maybe OccName
instantifyOccName occ
    | occNameSpace occ == tcName = Just $ mkOccNameFS clsName $ occNameFS occ <> "Instance"
    | otherwise = Nothing
