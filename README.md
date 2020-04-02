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

1. Install spark
2. Run
`./bin/spark-submit  --master local --class "HelloSpark"  <jar_path>` 



