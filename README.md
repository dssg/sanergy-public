# Sanergy: Sanitation in Nairobi, Kenya
Repository for 2016 DSSG project with [Sanergy](http://saner.gy/)

# Setup
You need the following software:
* [drake](https://github.com/Factual/drake) 
* [SCIP](http://scip.zib.de/)
* `davfs2` (can mount data source directly through WebDAV)

Build directory structure:

1. We use an alias to point to the data directory on the file server. This makes references from inside the repository shorter and simpler. The alias is `data`, a file in the root repository directory.  
2. drake uses timestamps on input and output files to determine whether it needs to run steps again. But that doesn't help with postgres steps because we don't have a timestamp for postgres. We created a psql directory with files that we can touch to track those updates.
3. GitHub ignores our data/ and psql/ directories. When you create the repository, run `build_directories_schemas.sh`. It parses `input/Drakefile` to figure out how to build those data and psql directories on the file server and schema on the database server (if they don't exist).  

Details
========

Setting up SCIP
-------------
1. Download the [SCIP Optimization suite](http://scip.zib.de/download.php?fname=scipoptsuite-3.2.1.tgz)
2. Follow the [suite installation instruction](http://scip.zib.de/doc/html/MAKE.php) , steps 1-3
3. In 3. ,before compiling, replace the Makefile.doit file with [the correct version](http://scip.zib.de/download/bugfixes/scip-3.2.1/Makefile.doit)
4. Follow the instructions to [set up the Python interface](http://scip.zib.de/doc/html/PYTHON_INTERFACE.php). When running `python setup.py install`, consider running it locally with the `--user` option.
5. Include `import pyscipopt` in you code and you are good to go! If you want to understand the syntax better, a good place to start is with [their examples](https://github.com/SCIP-Interfaces/PySCIPOpt/tree/master/examples/finished). This assumes some basic knowledge of linear and integer programming.
