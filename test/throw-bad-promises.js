process.on('unhandledRejection', err => {
  console.log(err.stack || err);
  throw err;
});
