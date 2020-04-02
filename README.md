## Sbt: Scala build tools

### Runs in two modes:
1. Shell/ Interactive: 
    * Here, type "sbt" on command line and it opens up the shell. 
    * can run commands like: clean/ compile/ run

2. Batch mode:
    * Here, type "sbt clean", it runs as a batch job and exits when done.
    * Slower, as sbt' JVM spins up each time.
    * Can run many commands at once, "sbt clean compile run"
    
### Commands:
1. clean: deletes generated files in target directory
2. compile
3. test
4. run
5. package: creates a JAR from resources and sources in target folder at project root level

### To use Spark to execute the jar:

1. Install spark, go to https://spark.apache.org/downloads.html
2. Run
`./bin/spark-submit --master local --class "HelloSpark" <jar_path>` 

### Spark and Scala VERSION compatibility errors
Once we start writing spark code, we need to import spark-core and spark-sql in project.

If right versions are not installed, code compilation or runtime fails with missing definitions. 

To choose the right version of spark and scala libraries:
1. See the spark version installed via 
`spark-shell`
2. Notice the scala version there.
3. In build.sbt, mention the same spark-core and spark-sql library version via 
`libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"`
4. In build.sbt, mention scala version via `scalaVersion := "2.11.12"`

Again to run code, 
> sbt package
>
> ./bin/spark-submit --class HelloSpark <jar_path>