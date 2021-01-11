-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell #-}

module Maven (
    generateAggregatePom,
    validateMavenArtifacts,
) where

import qualified Control.Exception.Safe as E
import           Control.Monad
import           Control.Monad.Logger
import           Control.Monad.IO.Class
import qualified Data.Maybe as Maybe
import qualified Data.Text as T
import           Data.Text (Text)
import           Path
import           Path.IO
import           System.Exit

import Types
import Util

validateMavenArtifacts :: MonadCI m => Path Abs Dir -> [(MavenCoords, Path Rel File)] -> m ()
validateMavenArtifacts releaseDir artifacts =
    forM_ artifacts $ \(_, file) -> do
        exists <- doesFileExist (releaseDir </> file)
        unless exists $ do
            $logError $ T.pack $ show file <> " is required for publishing to Maven"
            liftIO exitFailure

generateAggregatePom :: E.MonadThrow m => BazelLocations -> [Artifact PomData] -> m Text
generateAggregatePom BazelLocations{bazelBin} artifacts = do
    executions <- T.concat <$> mapM execution artifacts
    return (aggregatePomStart <> executions <> aggregatePomEnd)
    where
    execution :: E.MonadThrow m => Artifact PomData -> m Text
    execution artifact = do
        let (directoryText, name) = splitBazelTarget (artTarget artifact)
        directory <- parseRelDir (T.unpack directoryText)
        let prefix = bazelBin </> directory
        mainArtifactFile <- mainArtifactPath name artifact
        pomFile <- pomFilePath name
        javadocFile <- javadocJarPath artifact
        sourcesFile <- sourceJarPath artifact
        let configuration =
                map (\(name, value) -> (name, pathToText (prefix </> value))) $
                    Maybe.catMaybes
                        [ Just ("pomFile", pomFile)
                        , Just ("file", mainArtifactFile)
                        , ("javadoc", ) <$> javadocFile
                        , ("sources", ) <$> sourcesFile
                        ]
        return $ T.unlines $ map ("                    " <>) $
            [ "<execution>"
            , "    <id>" <> pomArtifactId (artMetadata artifact) <> "</id>"
            , "    <phase>initialize</phase>"
            , "    <goals>"
            , "        <goal>install-file</goal>"
            , "    </goals>"
            , "    <configuration>"
            ] ++
            map (\(name, value) -> "        <" <> name <> ">" <> value <> "</" <> name <> ">") configuration ++
            [ "    </configuration>"
            , "</execution>"
            ]
    aggregatePomStart :: Text
    aggregatePomStart =
        T.unlines
            [ "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            , "<project xmlns=\"http://maven.apache.org/POM/4.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd\">"
            , "  <modelVersion>4.0.0</modelVersion>"
            , "    <groupId>com.daml</groupId>"
            , "    <artifactId>aggregate</artifactId>"
            , "    <version>0.0.0</version>"
            , "    <packaging>pom</packaging>"
            , "    <build>"
            , "        <plugins>"
            , "            <plugin>"
            , "                <groupId>org.apache.maven.plugins</groupId>"
            , "                <artifactId>maven-install-plugin</artifactId>"
            , "                <version>3.0.0-M1</version>"
            , "                <executions>"
            ]
    aggregatePomEnd :: Text
    aggregatePomEnd =
        T.unlines
            [ "                </executions>"
            , "            </plugin>"
            , "        </plugins>"
            , "    </build>"
            , "</project>"
            ]

