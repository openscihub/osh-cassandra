function blanks(n) {
  var _blanks = '(?';
  var i = 0;
  while (++i < n) {
    _blanks += ', ?';
  }
  return _blanks + ')';
}

module.exports = blanks;
