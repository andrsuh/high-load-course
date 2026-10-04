# Template for the HighLoad course
This project is based on [Tiny Event Sourcing library](https://github.com/andrsuh/tiny-event-sourcing)

### Run PostgreSql
This example uses Postgres as an implementation of the Event store. You can see it in `pom.xml`:

```
<dependency>
    <groupId>ru.quipy</groupId>
    <artifactId>tiny-postgres-event-store-spring-boot-starter</artifactId>
    <version>${tiny.es.version}</version>
</dependency>
```

Thus, you have to run Postgres in order to test this example. Postgres service is included in  `docker-compose` file that we have in the root of the project.

# More comprehensive information about the course, project, how to run tests is here:

https://andrsuh.notion.site/2595d535059281d8a815c2cb3875c376?source=copy_link

https://andrsuh.notion.site/2625d5350592801aaf88c7c95302d10c?source=copy_link

### Run the infrastructure
Set of the services you need to start developing and testing process is following:
- Bombardier - service that is in charge of emulation the store's clients activity (creates the incoming load). Also serves as a third-party payment system.
- Postgres DBMS
- Prometheus + Grafana - metrics collection and visualization services

You can run all beforementioned services by the following command:
```
docker compose -f docker-compose.yml up
```

### Run the application
To make the application run you can start the main class `OnlineShopApplication`. It is not being launched as a docker contained to simplify and speed up the devevopment process as it is easier for you to refactor the application and re-run it immediately in the IDE.


### If you want to pull changes from the main repository into your fork

The command ```git remote -v``` should include the following lines:

```
upstream        https://github.com/andrsuh/high-load-course.git (fetch)
upstream        https://github.com/andrsuh/high-load-course.git (push)
```

If not, add the upstream remote:
```git remote add upstream https://github.com/andrsuh/high-load-course.git```

To pull changes from the main repository, run the following commands:

```
git fetch upstream
# switch to the main branch of your fork. Make sure the branch has no uncommitted changes to avoid conflicts
git checkout main 
# merge changes from the main repository into your main branch
git merge upstream/main 
```

### Case 2 (`hw2`)

Use account `acc-5` with `ratePerSecond=2`, `testCount=100`, and
`processingTimeMillis=60000`. The local provider reports 3 requests/second,
5 concurrent requests, average processing time 4.9 seconds, and cost 30 per call.

The concurrency limit is the bottleneck: `min(3, 5 / 4.9) = 1.02` payments/second.
Incoming load is higher, so payments wait in the existing bounded queue. For this
finite test, estimated waiting plus processing for the last payments is about
53 seconds. This estimate uses the average response time; the test must verify
the actual response times and deadlines.

The adapter reserves a concurrent request slot **before** obtaining a rate
permit. Otherwise, a permit can expire while a request waits for concurrency,
and several waiting requests can then be submitted together above the rate
limit. Both waits stop at `deadline - averageProcessingTime`, followed by a
deadline check immediately before HTTP submission. The semaphore uses fair
ordering, and its slot is released on rejection before enqueue or in the HTTP
callback after enqueue. Payment submission and processing events remain recorded.

Start the application with the same team credentials as case 1 and
`PAYMENT_ACCOUNTS=acc-5`. Run [test-case2-local.http](test-case2-local.http) locally.
For the course server, [test-case2-remote.http](test-case2-remote.http) selects
branch `hw2` and account `acc-5`; publish the branch before using that request.

Build and run the automated limiter tests with `mvn package`.

Local validation on 2026-10-03 with the case parameters:

| Metric | Existing `hw1` application | `hw2` |
| --- | --- | --- |
| Successful tests | 96 / 100 | 100 / 100 |
| Rate-limit violations | 4 | 0 |
| Concurrent-request violations | 0 | 0 |
| Maximum payment time including queue | Not measured | 53.661 seconds |
| Calls / total provider cost | 100 / 3000 | 100 / 3000 |

All four automated limiter tests pass. Provider metric counters were measured
as differences before and after each run. The maximum payment time was checked
against the 100 matching persisted payment events, including time in the queue.
These are local results; the course server has not been tested with `hw2` yet.

The `hw2` run generated successful order income of 37194, giving income/expenses
shares of 92.536% / 7.464% using the dashboard formula
`income / (income + providerCost)`. Order amounts are randomly generated, so
this percentage can differ between runs even with all payments successful and
exactly one provider call per order. This run does not establish a 93% income
share if that threshold is required for case 2.
