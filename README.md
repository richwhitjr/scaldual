# Lingual integration with Scalding

This library makes working with Lingual a bit easier by using Scalding to do some of the heavy lifting. 
It abstracts away the need to work directly with Cascading and allows for one job to run both Scalding and Lingual.

Some examples of how to use the library in the Tutorial directory.

## Building
There is a script (called sbt) in the root that loads the correct sbt version to build:

1. ./sbt update (takes 2 minutes or more)
2. ./sbt test
3. ./sbt assembly (needed to make the jar used by the scald.rb script)