
# microbench

This is [Google Caliper][caliper] project for benchmarking the
performance of different encoding schemes available for building HBase
applications. It depends on a HEAD checkout of Caliper and code from
the [data types patch][8089] vs HBase.

## dependencies

Install Caliper snapshot

    $ cd ~/tmp
    $ git clone https://code.google.com/p/caliper/
    $ cd caliper/caliper
    $ mvn clean install

Install HBase snapshot

    $ cd ~/tmp
    $ wget https://github.com/ndimiduk/hbase/archive/8693-datatypes-api.zip
    $ unzip 8693-datatypes-api.zip
    $ cd hbase-8693-datatypes-api
    $ mvn clean install -DskipTests

TODO: build Phoenix from snapshot

Build the benchmarks

    $ cd ~/tmp
    $ git clone https://github.com/ndimiduk/encoding-microbench.git
    $ cd encoding-microbench
    $ mvn clean compile

## usage

Verify launch script, classpath, print Caliper help message

    $ ./bin/microbench --help

Print list of available benchmarks

    $ ./bin/microbench list

Verify the benchmarks will run

    $ ./bin/microbench list | xargs -n1 ./bin/microbench --dry-run

At this point you have a working environment for collecting
benchmarks. Caliper by default post your benchmark results to a
[webapp][webapp] for analysis. You probably want to create an account
and configure your local machine with your API key before running any
thorough tests.

Once you're ready, run all of the things, gather many trials

    $ ./bin/microbench list | xargs -n1 ./bin/microbench -t 5

[caliper]: https://code.google.com/p/caliper/
[webapp]: https://microbenchmarks.appspot.com/
[8089]: https://issues.apache.org/jira/browse/HBASE-8089
