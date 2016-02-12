# Lingual integration with Scalding

This library makes working with Lingual a bit easier by using Scalding to do some of the heavy lifting. 
It abstracts away the need to work directly with Cascading and allows for one job to run both Scalding and Lingual.

Some examples of how to use the library are in the Tutorial directory.

## Building
There is a script (called sbt) in the root that loads the correct sbt version to build:

1. ./sbt update (takes 2 minutes or more)
2. ./sbt test
3. ./sbt assembly (needed to make the jar used by the scripts)

## TSql
Included in this project is a small CLI that allows for quick queries to be run on single text files in either hadoop or locally.

The main script can be found in scripts/tsql.  Running the command will give the options.

The script parses the text file given the delimiter which defaults to tab and counts how many columns it sees.  It then names the columns A-Z.

The name of the table is always FILE.

### Examples

Locally you can run at the root of the project but on the cluster you will need to upload the TSV.

Notice that local runs are giving a -l option while hadoop runs are not.

#### Local

Count the number of lines.
```shell
scripts/tsql -l -i src/main/resources/tutorial/docBOW.tsv -o /tmp/output.tsv -q "select count(A) as cnt from FILE"
```

Count the number rows per object in column A.
```shell
scripts/tsql -l -i src/main/resources/tutorial/docBOW.tsv -o /tmp/output.tsv -q "select A, count(B) as cnt from FILE group by A"
```
#### Hadoop 

Similar to Local but instead uploads a file to host machine and builds a hadoop command.  Here the output and input should be files on hdfs.

Count the number of lines.
```shell
scripts/tsql -i /user/scaldual/docBOW.tsv -o /user/scaldual/output.tsv -q "select count(A) as cnt from FILE"
```

#### Head

Another option is output the first n rows at the end of a run. For example in the above example you could output the rows to stdout.
```shell
scripts/tsql -l -i src/main/resources/tutorial/docBOW.tsv -o /tmp/output.tsv -q "select count(A) as cnt from FILE" -n 1
```

#### Multiple Files

The tool also allows for multiple input files.
You can do it by quoting and passing in a list of files to -i.  
What happens then is each file after the first is appended with a index(for example File1 below) and the columns in that file get the index appended also.

```shell
tsql -l -i "src/main/resources/tutorial/docBOW.tsv src/main/resources/tutorial/docBOW.tsv" -o /tmp/output.tsv -q "select A, A1 from FILE, FILE1 where A = A1"
```