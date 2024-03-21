// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

module.exports = function(grunt) {

  // load all grunt tasks
  ["banner",
   "browserify",
   "contrib-clean",
   "contrib-connect",
   "contrib-copy",
   "contrib-sass",
   "contrib-uglify",
   "contrib-watch",
   "exec",
   "open"].forEach(name => grunt.loadNpmTasks(`grunt-${name}`));

  grunt.initConfig({
    // Read package.json
    pkg: grunt.file.readJSON("package.json"),

    open : {
      dev: {
        path: 'http://localhost:1919'
      }
    },

    connect: {
      server: {
        options: {
          port: 1919,
          base: 'docs/build',
          livereload: true
        }
      }
    },
    copy: {
      skeleton: {
        files: [
          {
            expand: true,
            cwd: 'da_theme_skeleton',
            src: ['**'],
            dest: 'da_theme/'
          }
        ]
      },
      fonts: {
        files: [
          {
            expand: true,
            flatten: true,
            src: ['bower_components_static/font-awesome/fonts/*'],
            dest: 'da_theme/static/fonts/',
            filter: 'isFile'
          }
        ]
      },
      versions: {
          files: [
              {
                expand: true,
                cwd: 'docs/build',
                src: ['**'],
                dest: 'docs/build/0.11.0/'
              },
              {
                expand: true,
                cwd: 'docs/build/',
                src: ['**'],
                dest: 'docs/build/0.10.9/'
              },
              {
                expand: true,
                cwd: 'docs/build/',
                src: ['**'],
                dest: 'docs/build/0.7.13/'
              },
              {
                expand: true,
                cwd: 'docs/',
                src: ['versions.json'],
                dest: 'docs/build/'
              }
          ]
      }
    },
    sass: {
      dev: {
        options: {
          style: 'expanded',
          loadPath: ['bower_components_static/bourbon/dist', 'bower_components_static/neat/app/assets/stylesheets', 'bower_components_static/font-awesome/scss', 'bower_components_static/wyrm/sass']
        },
        files: [{
          expand: true,
          cwd: 'sass',
          src: ['*.sass'],
          dest: 'da_theme/static/css',
          ext: '.css'
        }]
      }
    },

    browserify: {
      dev: {
        options: {
          external: ['jquery'],
          alias: {
            'da_theme': './js/theme.js'
          }
        },
        src: ['js/*.js'],
        dest: 'da_theme/static/js/theme.js'
      },
      build: {
        options: {
          external: ['jquery'],
          alias: {
            'da_theme': './js/theme.js'
          }
        },
        src: ['js/*.js'],
        dest: 'da_theme/static/js/theme.js'
      }
    },
    uglify: {
      dist: {
        options: {
          sourceMap: false,
          mangle: {
            reserved: ['jQuery'] // Leave 'jQuery' identifier unchanged
          },
          ie8: true // compliance with IE 6-8 quirks
        },
        files: [{
          expand: true,
          src: ['da_theme/static/js/*.js', '!da_theme/static/js/*.min.js'],
          dest: 'da_theme/static/js/',
          rename: function (dst, src) {
            // Use unminified file name for minified file
            return src;
          }
        }]
      }
    },
    usebanner: {
      dist: {
        options: {
          position: 'top',
          banner: '/* <%= pkg.name %> version <%= pkg.version %> | MIT license */\n',
          linebreak: true
        },
        files: {
          src: [ 'da_theme/static/js/theme.js', 'da_theme/static/css/theme.css' ]
        }
      }
    },
    exec: {
      build_sphinx: {
        cmd: 'sphinx-build -c docs/ ../source docs/build'
      }
    },
    clean: {
      build: ["docs/build"],
      fonts: ["da_theme/static/fonts"],
      js: ["da_theme/static/js/*", "!da_theme/static/js/modernizr.min.js"]
    },

    watch: {
      /* Compile sass changes into theme directory */
      sass: {
        files: ['sass/**/*.sass', 'bower_components_static/**/*.sass'],
        tasks: ['sass:dev']
      },
      /* Static */
      browserify: {
        files: ['da_theme_skeleton/**/*'],
        tasks: ['clean','copy:skeleton','sass:dev','browserify:dev']
      },
      /* Changes in theme dir rebuild sphinx */
      sphinx: {
        files: ['da_theme/**/*', 'README.rst', 'docs/**/*.rst', 'docs/**/*.py'],
        tasks: ['clean:build','exec:build_sphinx']
      },
      /* JavaScript */
      browserify: {
        files: ['js/*.js'],
        tasks: ['browserify:dev']
      },
      /* live-reload the docs if sphinx re-builds */
      livereload: {
        files: ['docs/build/**/*'],
        options: { livereload: true }
      }
    }

  });

  grunt.loadNpmTasks('grunt-exec');
  grunt.loadNpmTasks('grunt-contrib-connect');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-contrib-sass');
  grunt.loadNpmTasks('grunt-contrib-clean');
  grunt.loadNpmTasks('grunt-contrib-copy');
  grunt.loadNpmTasks('grunt-open');
  grunt.loadNpmTasks('grunt-browserify');

  grunt.registerTask('default', ['clean','copy:skeleton','copy:fonts','sass:dev','browserify:dev','usebanner','exec:build_sphinx', 'copy:versions','connect','open','watch']);
  grunt.registerTask('build', ['clean','copy:skeleton','copy:fonts','browserify:build','uglify','usebanner']);
}
