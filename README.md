HTTP Shared Storage
===================

Simple shared storage for web applications. Especially useful in closed intranet environments.

- Multiple clients can share data by using REST protocol
- [Optimistic locking](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) 
  mechanism to sync clients (using `ETag`)
- Arbitrary mime type (not only JSON) support
- Use long polling for immediate change notification
- Stores all data in memory (lost on restart), which can be improved in future releases

Sample Angular application using this as a backend: 
[Planning Poker](https://github.com/yarosla/poker).

Quick start
-----------

    gradle clean fatJar
    java -jar build/libs/http-shared-storage-1.0.jar

Options
-------

    --host, -H
      Set http host to listen on
      Default: 0.0.0.0
    --port, -P
      Set http port to listen to
      Default: 8080
    --cors, -c
      Allow cross-origin requests
      Default: false
    --limit, -l
      Memory limit in megabytes
      Default: 1000
    --static, -s
      Serve static content from this directory
    --debug, -v
      Show debug log
      Default: false
    --help, -h
      Display help

Statistics
----------

Inspect statistics (in JSON format):

    curl http://localhost:8080/stats

Technologies
------------

- Spring 5 WebFlux + Netty
- Caffeine Cache

Author
------

Yaroslav Stavnichiy <yarosla@gmail.com>
