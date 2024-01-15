// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const fs = require('fs');
const path = require('path');
const webpack = require("webpack");
const ESLintPlugin = require('eslint-webpack-plugin');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const TsConfigPathsPlugin = require('tsconfig-paths-webpack-plugin');
const CaseSensitivePathsPlugin = require('case-sensitive-paths-webpack-plugin');

const APP_NAME = 'Navigator';

/**
 * By default, navigator build tools (webpack, webpack plugins, typescript)
 * expect the following directory structure:
 *
 * project directory (ledger-tools/navigator/frontend)
 * ├── node_modules
 * │   └── (dependencies installed by yarn)
 * ├── src
 * │   └── (typescript source files)
 * ├── tsconfig.json
 * ├── .eslintrc.js
 * ├── .modernizrrc
 * ├── webpack.config.json
 * └── package.json
 *
 * With Bazel, there are several issues:
 * - Paths contain symlinks
 * - The node_modules folder is outside of the project directory
 * - Webpack is not called from the project directory
 *
 * It is not trivial (or possibly impossible) to configure the build tools to run
 * in the Bazel environment. Therefore, all input files are *copied* into a temporary
 * directory according to the above directory structure, and then webpack is run in there.
 * If you want to try running webpack without copying files, have a look at the following settings:
 *
 * webpack.config.js
 * - context: https://webpack.js.org/configuration/entry-context#context
 * - rule.include: https://webpack.js.org/configuration/module#condition
 * - resolve.alias: https://webpack.js.org/configuration/resolve#resolvealias
 * - resolve.modules: https://webpack.js.org/configuration/resolve#resolvemodules
 * - resolve.symlinks: https://webpack.js.org/configuration/resolve/#resolvesymlinks
 * - tsconfig-paths-webpack-plugin.baseUrl: https://github.com/dividab/tsconfig-paths-webpack-plugin#baseurl-string-defaultundefined
 * - ts-loader.context: https://github.com/TypeStrong/ts-loader#context-string-defaultundefined
 *
 * tsconfig.json
 * - baseUrl: https://www.typescriptlang.org/docs/handbook/compiler-options.html
 * - preserveSymlinks: https://www.typescriptlang.org/docs/handbook/compiler-options.html
 * - paths: https://www.typescriptlang.org/docs/handbook/compiler-options.html
 */
module.exports = (env) => {
  const paths_case_check = env && env.paths_case_check  || 'true';
  const in_dir           = env && env.bazel_in_dir  || __dirname;
  const out_dir          = env && env.bazel_out_dir || path.join(__dirname, 'dist');
  const build_version    = env && env.bazel_version_file ? fs.readFileSync(env.bazel_version_file, 'utf8').trim() : 'HEAD';
  const build_commit     = env && env.bazel_commit_file  ? fs.readFileSync(env.bazel_commit_file, 'utf8').trim()  : 'HEAD';
  const isProduction     = env ? (!!env.prod || !!env.production) : false;
  console.log(isProduction ? 'PRODUCTION' : 'DEVELOPMENT');

  const modernizr_config  = path.join(in_dir, '.modernizrrc');
  const typescript_config = path.join(in_dir, 'tsconfig.json');

  console.log(`============================== Webpack environment =============================`);
  console.log(`  isProduction:      ${isProduction}`);
  console.log(`  in_dir:            ${in_dir}`);
  console.log(`  out_dir:           ${out_dir}`);
  console.log(`  modernizr_config:  ${modernizr_config}`);
  console.log(`  typescript_config: ${typescript_config}`);
  console.log(`  paths_case_check:  ${paths_case_check}`);
  console.log(`============================== Webpack environment =============================`);

  var plugins = [
    new HtmlWebpackPlugin({
      title: APP_NAME,
      template: 'src/index.html',
      favicon: './src/images/favicon.png',
    }),
    new ESLintPlugin({emitError: true, extensions: ["js", "ts", "tsx"]}),
  ];

  if (paths_case_check === 'true') {
    plugins.push(new CaseSensitivePathsPlugin())
  }

  return {
    mode: isProduction ? 'production' : 'development',
    entry: {
      browsercheck: './src/browsercheck.ts',
      bundle: './src/index.tsx',
    },
    context: in_dir,
    output: {
      path: out_dir,
      filename: '[name]-[fullhash].js',
      // In production, we serve static assets under /assets and publicPath
      // makes WebPack set file references relative to this and because this is
      // a root path, file references will all be relative to the root, which is
      // important when using the History API for doing routing inside the SPA.
      // In development, we serve files on / since that's the easiest thing to
      // get to work with HtmlWebpackPlugin. For production, we build all
      // assets, including index.html, to a single folder. The backend serving
      // these assets then need to take care of serving the index.html on all
      // routes the frontend might use for its routing in order for reloading
      // the frontend to work as expected.
      publicPath: isProduction ? '/assets' : '/'
    },
    devtool: 'source-map',
    module: {
      rules: [
        {
          test: /\.modernizrrc$/,
          use: [{
            loader: 'val-loader',
            options: {
              executableFile: path.resolve(__dirname, "modernizr.js")
            }
          }]
        },
        {
          test: /\.tsx?$/,
          exclude: /node_modules/,
          loader: 'ts-loader',
          options: {
            configFile: typescript_config,
            context: in_dir,
          }
        },
        {
          test: /\.css$/,
          use: ['style-loader', 'css-loader']
        },
        { test: /\.(png|jpg)$/, use: { loader: 'url-loader', options: { limit: 8192 } } },
        {
          test: /\.(woff|woff2)$/, use: {
            loader: 'url-loader',
            options: {
              name: 'fonts/[contenthash].[ext]',
              limit: 5000,
              mimetype: 'application/font-woff'
            }
          }
        },
        {
          test: /\.(ttf|eot|svg)$/, use: {
            loader: 'file-loader',
            options: {
              name: 'fonts/[contenthash].[ext]'
            }
          }
        }
      ],
    },
    resolve: {
      extensions: ['.ts', '.tsx', '.js', '.jsx'],
      plugins: [
        // This is necessary to get the baseUrl and paths options in the
        // TypeScript config to work.
        new TsConfigPathsPlugin({
          configFile: typescript_config,
          baseUrl: in_dir,
        }),
      ],
      alias: {
        modernizr$: modernizr_config,
      }
    },
    plugins: plugins,
    devServer: {
      port: 8000,
      // host: '0.0.0.0', // enable to allow remote computers to connect
      // disableHostCheck: true, // enable to allow remote computers to connect
      static: out_dir,
      historyApiFallback: { index: '/' },
      compress: true,
      proxy: {
        '/api': 'http://localhost:4000/'
      }
    }
  };
};
