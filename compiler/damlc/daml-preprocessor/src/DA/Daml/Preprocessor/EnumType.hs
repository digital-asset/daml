
-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Preprocessor.EnumType
    ( enumTypePreprocessor
    ) where

import "ghc-lib" GHC
import "ghc-lib-parser" BasicTypes
import "ghc-lib-parser" FastString
import "ghc-lib-parser" OccName
import "ghc-lib-parser" RdrName

-- | This preprocessor adds the @DamlEnum@ constraint to every data definition
-- of the form @data A = B@, so it becomes @data DamlEnum => A = B@, which gets
-- picked up during LF conversion and turned into an enum definition. This is
-- distinguishible from @data A = B {}@ at the core level. @DamlEnum@ is a
-- 0-parameter typeclass defined in GHC.Types and re-exported in Prelude.
--
-- This rule is parametrized by the module containing damlEnumMod since this is different for
-- data-dependencies where we want CurrentSdk.GHC.Types
enumTypePreprocessor :: FastString -> ParsedSource -> ParsedSource
enumTypePreprocessor damlEnumMod (L l src) = L l src
    { hsmodDecls =  map (fixEnumTypeDecl damlEnumMod) (hsmodDecls src) }

fixEnumTypeDecl :: FastString -> LHsDecl GhcPs -> LHsDecl GhcPs
fixEnumTypeDecl damlEnumMod (L l decl)
    | TyClD xtcd tcd <- decl
    , DataDecl { tcdLName = lname, tcdTyVars = tyvars, tcdDataDefn = dd } <- tcd
    , HsQTvs _ [] <- tyvars -- enums cannot have type vars
    , HsDataDefn {dd_ctxt = (L lctx []), dd_cons = [con]} <- dd
    , PrefixCon [] <- con_args (unLoc con) -- no arguments to constructor
    = L l (TyClD xtcd tcd { tcdDataDefn = dd { dd_ctxt = L lctx (makeEnumCtx damlEnumMod lname) } })

fixEnumTypeDecl _ ldecl = ldecl

makeEnumCtx :: FastString -> Located RdrName -> [LHsType GhcPs]
makeEnumCtx damlEnumMod (L loc _) =
    [L loc (HsTyVar noExt NotPromoted (L loc (mkQual tcName (damlEnumMod, "DamlEnum"))))]
        -- TODO: qualify with Prelude.
