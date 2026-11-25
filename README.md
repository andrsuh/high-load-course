# Template for the HighLoad course
This project is based on [Tiny Event Sourcing library](https://github.com/andrsuh/tiny-event-sourcing)

### Case 7: local Windows run

Requires Docker Desktop and Java 17+. Maven runs in Docker.
Credentials are read from the existing `.env`.
On a fresh checkout, copy `.env.example` to `.env` and fill in
`PAYMENT_SERVICE_NAME` and `PAYMENT_TOKEN`. The local `.env` is ignored by Git.

```powershell
./scripts/start-local.ps1
# Wait for http://localhost:8081/actuator/health to report UP.
./scripts/test-local.ps1
```

The application uses `acc-7`, 5 requests/s, 1000 tests and a 6000 ms processing budget.
Grafana: http://localhost:3000/d/highload-case7 (admin / quipy).
Application logs: `local-app.log`.
To change the default 2500 ms HTTP timeout, stop the application PID printed by
start-local and run `./scripts/start-local.ps1 -SkipBuild -HttpTimeoutMs 2000`.
Choose the final timeout from measured latency and test outcomes.
The actual HTTP call timeout is also bounded by the remaining payment deadline.

Metrics: `payment_retries_total` counts extra attempts;
`payment_http_duration_seconds` exposes p50/p80/p95/p99 per account and status,
including `status="timeout"`. HTTP timing excludes semaphore/rate-limiter waiting
and event-store updates. These durations use seconds.
A timed-out measurement does not reveal how long the provider would have taken
to finish. Compare successful throughput and cost as well.

Observed local case 7 result at 2500 ms: 997 successful operations and 3 failures
out of 1000. Outcomes vary between runs.

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
