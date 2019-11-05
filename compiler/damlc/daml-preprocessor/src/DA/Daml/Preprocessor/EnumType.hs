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
import "ghc-lib-parser" OccName

-- import           Control.Monad.Extra
-- import           Data.List
-- import           Data.Maybe
-- import           System.FilePath (splitDirectories)

enumTypePreprocessor :: ParsedSource -> ParsedSource
enumTypePreprocessor (L l src) =
    L l src { hsmodDecls =  concatMap fixEnumTypeDecl (hsmodDecls src) }

fixEnumTypeDecl :: LHsDecl GhcPs -> [LHsDecl GhcPs]
fixEnumTypeDecl (L l decl)
    | TyClD xtc (DataDecl xdd (L nameLoc name) dtyvars dfixity ddefn) <- decl
    , HsQTvs _ [] <- dtyvars
    , HsDataDefn {dd_cons = [con]} <- ddefn
    , PrefixCon [] <- con_args (unLoc con)
    , Unqual oname <- name
    =
    let newName = Unqual (mkOccName (occNameSpace oname) ("DamlEnum$" <> occNameString oname))
    in  [ L l (TyClD xtc (DataDecl xdd (L nameLoc newName) dtyvars dfixity ddefn))
        , L l (TyClD xtc (SynDecl noExt (L nameLoc name) dtyvars dfixity
            (L nameLoc (HsTyVar noExt NotPromoted (L nameLoc newName)))))
        ]

fixEnumTypeDecl ldecl = [ldecl]
