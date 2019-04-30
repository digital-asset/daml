-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import System.Environment
import System.Process
import           Data.ByteString.Lazy.Char8 (unpack)
import           Data.ByteString.UTF8
import           Network.HTTP.Simple
import           Text.XML.Light
import           Distribution.PackageDescription.Parsec
import           Distribution.Types.GenericPackageDescription
import           Distribution.Types.PackageDescription
import qualified Distribution.SPDX as SPDX
import qualified Distribution.License as DL
import           Distribution.Types.PackageId
import           Distribution.Types.PackageName (unPackageName)
import           Control.Monad
import           Data.Maybe
import Distribution.SPDX.LicenseExpression as LE
import Distribution.SPDX.License as SLE
import qualified Data.List as L



data LibInfo = LibInfo 
  { packageName :: String
  , licenseId :: String
  , licenseText :: String
  } deriving (Show)

data PacakageCabalFile = PacakageCabalFile 
  { pUrl :: String
  , cabalFile :: String
  } deriving (Show)


attrToValue :: [Attr] -> Maybe String
attrToValue [Attr qname1 "name" , Attr qname2 val] 
  | qName qname1 == "name" && qName qname2 == "value" = Just val
attrToValue _ = Nothing

packageNameFromContent :: [[Content]] -> [String]
packageNameFromContent xsc = mapMaybe (attrToValue . elAttribs) (onlyElems $ concat xsc)

hackageLink :: String -> String
hackageLink pname =
  "http://hackage.haskell.org/package/" ++ pname ++ "/src/" ++ pname ++ ".cabal"

licenceFileFromHackage :: String -> IO String
licenceFileFromHackage url = do
  req  <- parseRequest url
  resp <- httpLBS req
  return (unpack $ getResponseBody resp)


hackageLicenceFileLink :: String -> [FilePath] -> IO String
hackageLicenceFileLink pname (x : _) = licenceFileFromHackage
  ("http://hackage.haskell.org/package/" ++ pname ++ "/src/" ++ x)
hackageLicenceFileLink pname _ = return pname


packageList :: Element -> [String]
packageList doc =
  packageNameFromContent (map elContent (findElements (unqual "rule") doc))


cabalFileFromUrl :: String -> IO PacakageCabalFile
cabalFileFromUrl url = do
  req  <- parseRequest (hackageLink url)
  rest <- httpLBS req
  return $ PacakageCabalFile url $ unpack (getResponseBody rest)


licenseIdText :: Either SPDX.License DL.License -> String
licenseIdText m = case m of
  Right r -> show r
  Left  (SLE.License ( LE.ELicense( LE.ELicenseId lname) _)) -> show lname
  _ -> "Unknown license"

pkgInfo :: PacakageCabalFile -> IO LibInfo
pkgInfo pkg =
  case parseGenericPackageDescriptionMaybe (fromString $ cabalFile pkg) of
      Just parseResult -> do 
        let pd = packageDescription parseResult
        fullText <- hackageLicenceFileLink (pUrl pkg) $licenseFiles pd
        let pName = unPackageName (pkgName $ package pd)
        let licenseCode = licenseIdText (licenseRaw pd)
        return (LibInfo pName licenseCode fullText)
                      
      Nothing -> return (LibInfo (pUrl pkg) (cabalFile pkg) [])

ppLibInfo :: LibInfo -> String
ppLibInfo libInfo = pkName ++ replicate fittingLen ' '   ++ licenseId libInfo ++ "\n"
  where 
    pkName = packageName libInfo
    fittingLen = 40 - L.length pkName

ppFullLicense :: LibInfo -> String
ppFullLicense libFullText =  "\n\n" ++ replicate 80 '-' ++ "\n" ++ packageName libFullText ++ "\n" ++ licenseText libFullText

generateDepsFileFromBazel :: IO String
generateDepsFileFromBazel = do
  let cmd = "bazel query 'kind(\"(haskell_library|haskell_toolchain_library)\",filter(\"^((?!da-hs-|:daml-|:daml_).)*$\", deps(//...)))' --output xml"
  readCreateProcess (shell cmd) "" 
  

main :: IO ()
main = do 
  args <- getArgs
  case args of 
    [outputFilepath] -> do
        s <- generateDepsFileFromBazel
        case parseXMLDoc s of
          Nothing -> error "Failed to parse xml"
          Just doc -> do 
            files <- mapM cabalFileFromUrl (packageList doc) 
            allInfos <- mapM pkgInfo files
            let infos = filter (\info -> not $ L.isPrefixOf "Paths_" $ packageName info ) allInfos
            forM_ infos $ \info -> do 
                  appendFile outputFilepath (ppLibInfo info)
            forM_ infos $ \info -> do
                  appendFile outputFilepath (ppFullLicense info)
    _ -> error "Missing notices filepath to be produced, usage: notices-gen /path/notices.txt"
