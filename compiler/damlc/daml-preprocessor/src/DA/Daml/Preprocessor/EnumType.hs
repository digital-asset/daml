
{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.Preprocessor.EnumType
    ( enumTypePreprocessor
    ) where

-- import Development.IDE.Types.Options
-- import qualified "ghc-lib-parser" SrcLoc as GHC
-- import qualified "ghc-lib-parser" Module as GHC
-- import qualified "ghc-lib-parser" FastString as GHC
-- import Outputable

import "ghc-lib" GHC
import "ghc-lib-parser" BasicTypes
import "ghc-lib-parser" RdrName
import "ghc-lib-parser" OccName

-- import           Control.Monad.Extra
-- import           Data.List
-- import           Data.Maybe
-- import           System.FilePath (splitDirectories)

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
    [L loc (HsTyVar noExt NotPromoted (L loc (mkUnqual tcName "DamlEnum")))] -- TODO: qualify.
