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
`processingTimeMillis=60000`. The provider reports 3 requests/second,
5 concurrent requests, average processing time 4.9 seconds, and cost 30 per call.
The existing payment executor still has 16 workers and a bounded queue.

The adapter keeps the order **rate limiter -> concurrency semaphore -> HTTP**.
A rate permit is reserved first and remains occupied while waiting for a free
concurrency slot. Its one-second sliding window starts immediately before HTTP
submission. Thus, old permits cannot expire while requests wait for the
semaphore and later allow a burst of submissions above the rate limit.
If a deadline is missed before submission, the rate reservation is returned
without making a provider call. Once submitted, it is returned after one second,
independently of the response. The concurrency slot is returned in the callback.
Both waits stop at `deadline - averageProcessingTime`, followed by a deadline
check immediately before HTTP submission. Both semaphores use fair waiting.

The concurrency limit is the bottleneck: `min(3, 5 / 4.9) = 1.02` payments/second.
Incoming load is higher, so payments wait in the existing bounded queue. For this
finite test, estimated waiting plus processing for the last payments is about
53 seconds. The estimate uses average processing time, so actual deadlines must
be checked in a complete load test.

Start the application with the same team credentials as case 1 and
`PAYMENT_ACCOUNTS=acc-5`. Run [test-case2-local.http](test-case2-local.http) locally.
For the course server, [test-case2-remote.http](test-case2-remote.http) selects
branch `hw2` and account `acc-5`; push the branch before using that request.
Build and run the seven automated limiter tests with `mvn package`.

Income/expenses shares use `income / (income + providerCost)`, as in Grafana.
With exactly 100 calls costing 30 each, reaching 93% income share requires total
successful order income of at least 39857.143. Random order amounts affect this
percentage even with all payments successful and exactly one call per order.

Local validation on 2026-10-04 of this rate limiter -> semaphore implementation:

| Metric | Result |
| --- | --- |
| Successful tests | 100 / 100 |
| Rate-limit violations | 0 |
| Provider calls / total cost | 100 / 3000 |
| Maximum payment time including queue | 56.286 seconds (< 60 seconds) |
| Income / expenses shares | 92.895% / 7.105% |

All seven automated tests pass, including a reservation waiting behind an
occupied semaphore longer than one rate window, reservation cancellation,
and preventing early release after submission. Provider outcomes were all
SUCCESS. Counts were measured as differences before and after the load test;
maximum payment time was checked against all 100 matching persisted events.
Generated successful order income was 39225. Financial share is below
the general 93% target in this sample despite all payments succeeding and
one provider call per order. No claim of meeting that target or of passing the
course server is made for this version; the remote run is still required.
