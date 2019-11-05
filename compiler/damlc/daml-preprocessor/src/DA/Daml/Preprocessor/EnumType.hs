
-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.Preprocessor.EnumType
    ( enumTypePreprocessor
    ) where

import "ghc-lib" GHC
import "ghc-lib-parser" BasicTypes
import "ghc-lib-parser" RdrName
import "ghc-lib-parser" OccName

-- | This preprocessor adds the @DamlEnum@ constraint to every data definition
-- of the form @data A = B@, so it becomes @data DamlEnum => A = B@, which gets
-- picked up during LF conversion and turned into an enum definition. This is
-- distinguishible from @data A = B {}@ at the core level. @DamlEnum@ is a
-- 0-parameter typeclass defined in GHC.Types and re-exported in Prelude.
enumTypePreprocessor :: ParsedSource -> ParsedSource
enumTypePreprocessor (L l src) = L l src
    { hsmodDecls =  map fixEnumTypeDecl (hsmodDecls src) }

fixEnumTypeDecl :: LHsDecl GhcPs -> LHsDecl GhcPs
fixEnumTypeDecl (L l decl)
    | TyClD xtcd tcd <- decl
    , DataDecl { tcdLName = lname, tcdTyVars = tyvars, tcdDataDefn = dd } <- tcd
    , HsQTvs _ [] <- tyvars -- enums cannot have type vars
    , HsDataDefn {dd_ctxt = (L lctx []), dd_cons = [con]} <- dd
    , PrefixCon [] <- con_args (unLoc con) -- no arguments to constructor
    = L l (TyClD xtcd tcd { tcdDataDefn = dd { dd_ctxt = L lctx (makeEnumCtx lname) } })

fixEnumTypeDecl ldecl = ldecl

makeEnumCtx :: Located RdrName -> [LHsType GhcPs]
makeEnumCtx (L loc _) =
    [L loc (HsTyVar noExt NotPromoted (L loc (mkUnqual tcName "DamlEnum")))]
        -- TODO: qualify with Prelude.
