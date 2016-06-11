# Sanergy: Sanitation in Nairobi, Kenya
Repository for 2016 DSSG project with [Sanergy](http://saner.gy/)

# Setup
You need the following software:
* [drake](https://github.com/Factual/drake) 
* `davfs2` (can mount data source directly through WebDAV)

Build directory structure:

1. We use an alias to point to the data directory on the file server. This makes references from inside the repository shorter and simpler. The alias is `data`, a file in the root repository directory.  
2. drake uses timestamps on input and output files to determine whether it needs to run steps again. But that doesn't help with postgres steps because we don't have a timestamp for postgres. We created a psql directory with files that we can touch to track those updates.
3. GitHub ignores our data/ and psql/ directories. When you create the repository, run `build_directories_schemas.sh`. It parses `input/Drakefile` to figure out how to build those data and psql directories on the file server and schema on the database server (if they don't exist).  

