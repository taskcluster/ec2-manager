var lint = require('mocha-eslint');

var paths = [
  'lib/*.js',
  'test/*.js',
];

lint(paths);
