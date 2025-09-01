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

Thus, you have to run Postgres in order to test this example. Postgres service is included in each `docker-compose` file that we have in the root of the project.

### Run the infrastructure
Set of the services you need to start developing and testing process is following:
- Bombardier - service that is in charge of emulation the store's clients activity (creates the incoming load). Also serves as a third-party payment system.
- Postgres DBMS
- Prometheus + Grafana - metrics collection and visualization services

You can run all beforementioned services by the following command:
```
docker-compose -f docker-conpose-local.yml up
```

### Run the application
To make the application run you can start the main class `OnlineShopApplication`. It is not being launched as a docker contained to simplify and speed up the devevopment process as it is easier for you to refactor the application and re-run it immediately in the IDE.


### Если вы хотите подтянуть изменения главного репозитория в свой форк

Команда ```git remote -v``` должна содержать следующие строки:

```
upstream        https://github.com/andrsuh/high-load-course.git (fetch)
upstream        https://github.com/andrsuh/high-load-course.git (push)
```

Если нет, то добавьте upstream:

```git remote add upstream https://github.com/andrsuh/high-load-course.git```

Чтобы подтянуть изменения из главного репозитория, выполните следующие команды:

```
git fetch upstream
// переключаемся на главную ветку вашего форка. Убедитесь что ветка не содержит изменений, чтобы не решать конфликты
git checkout main 
// сливаем изменения из главного репозитория в вашу главную ветку
git merge upstream/main 
```