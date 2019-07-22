# PL2

Simple Java to parse [MATHia](https://www.carnegielearning.com/products/software-platform/mathia-learning-software/) csv output from Carnegie Learning and generate tab-delimited [DataShop](https://pslcdatashop.web.cmu.edu) import.

Jar files in lib directory are needed to build and run.

`javac -classpath ".:./lib/opencsv-4.5.jar:./lib/common-lang3.jar" ParseMATHia.java`

Command-line args: `-i inputFileName -o outputFileName`

If -o not specified, output written to output.txt. Debugging written to debug.log.

`java -classpath ".:/./lib/opencsv-4.5.jar:./lib/common-lang3.jar" ParseMATHia -i input_file.csv -o output_file.txt`
