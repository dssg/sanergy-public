# Sanergy: Sanitation in Nairobi, Kenya
Repository for 2016 DSSG project with [Sanergy](http://saner.gy/)

# Introduction
Founded in 2011, Sanergy is a franchise based sanitation service, licensing pay-per-use and non-commercial toilets to local entrepreneurs. Entrepreneurs apply to operate a Sanergy, Fresh Life toilet within the informal settlements, at markets, bus stops and schools, and Sanergy contracts to collect and treat the human waste on a regular basis. This economic model expands access to clean sanitation for those who dwell within the informal settlements, while helping Fresh Life owner-operators to grow their franchise. Currently Sanergy operates a network of about 700 toilets---in raw terms, Sanergy collected over ~1,500 tonnes of feces and about as much urine in 2015 alone.

As the first and primary objective of our DSSG summer efforts, we focused on improving the collection operations and particularly their collection schedule. In the past, Sanergy had collected from each toilet every day. While this was possible when there were a few hundred toilets, the scale of Sanergy’s network has quickly outpaced its collection strategy. Additionally, not every toilet needs to be collected every day. For example, when schools are closed on a weekend or for a holiday, the toilet will not be used. As some residential toilets are shared by only a few families, they fill at a slower rate than toilets at bus stops or open markets, and could be collected less often than the later examples.

Recently Sanergy has started to experiment with different scheduling schemes. To support such flexible collection schedules, we have implemented a model to construct weekly collection schedules in a data-driven way, such that Sanergy picks up low-fill toilets only occasionally. At the same time, this *collection model* ensures that toilets do not exceed its capacity too often and that they are collected frequently---within at most three days---so as not to smell. The collection model uses as an input the waste model, which predicts how much feces and urine get accumulated in each toilet every day.

We further implemented a data-driven *staffing model* (crew scheduling). This model produces a spreadsheet that suggests how many workers Sanergy will need for every route in the coming week on basis of the collection model and the waste model. We expects that these changes will help Sanergy break even on their logistics operations and expand more rapidly to provide more slum-dwellers with toilet access.



# Documentation
You can find most of the documentation in the Technical Report.

# Setup
You need the following software:
* [drake](https://github.com/Factual/drake) 
* [SCIP](http://scip.zib.de/)
  * Building SCIP is a bit complicated and requires a number of dependencies; see below
* Python packages listed in [requirements.txt](requirements.txt)
  * `geopandas` requires `apt-get install libgdal-dev`
  * `pyscipopt` is installed via SCIP
* At least one of the ETL data processing steps requires R: `apt-get install r-base`
* `davfs2` (can mount data source directly through WebDAV)

### Configuration

There are two required configuration files for each of the database connections:

* `./default_profile`, which contains Bash-like assignments to the variables
  `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, and `PGPASSWORD` assigned with Bash-like
  syntax.
* `./mssql_profile`, similarly contains assignments to host, port, database,
  user, and password variables.

### Build directory structure:

1. We use an alias to point to the data directory on the file server. This makes references from inside the repository shorter and simpler. The alias is `data`, a file in the root repository directory.  
2. drake uses timestamps on input and output files to determine whether it needs to run steps again. But that doesn't help with postgres steps because we don't have a timestamp for postgres. We created a psql directory with files that we can touch to track those updates.
3. GitHub ignores our data/ and psql/ directories. When you create the repository, run `build_directories_schemas.sh`. It parses `input/Drakefile` to figure out how to build those data and psql directories on the file server and schema on the database server (if they don't exist).  

Setup Details
========

Setting up SCIP
-------------
0. Install required dependencies: `apt-get install -y libgmp3-dev libreadline6 libreadline6-dev zlib1g-dev libncurses5-dev bison flex`
1. Download the [SCIP Optimization suite](http://scip.zib.de/download.php?fname=scipoptsuite-3.2.1.tgz)
2. Follow the [suite installation instruction](http://scip.zib.de/doc/html/MAKE.php), steps 1-3
  * In 3, before compiling, replace the Makefile.doit file with [the correct version](http://scip.zib.de/download/bugfixes/scip-3.2.1/Makefile.doit)
  * Build as a shared library
4. Follow the instructions to [set up the Python interface](http://scip.zib.de/doc/html/PYTHON_INTERFACE.php). When running `python setup.py install`, consider running it locally with the `--user` option.
5. Include `import pyscipopt` in you code and you are good to go! If you want to understand the syntax better, a good place to start is with [their examples](https://github.com/SCIP-Interfaces/PySCIPOpt/tree/master/examples/finished). This assumes some basic knowledge of linear and integer programming.

# Contributors
Brian McInnis, Ivana Petrovic, Jan Vlachy, Joe Walsh, Jen Helsby, Kevin Lo
