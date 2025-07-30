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

Thus, you have to run Postgres in order to test this example. We have `docker-compose` file in the root. Run following command to start the database:

```
docker-compose up
```

### Run the application
To make the application run you can start the main class `OnlineShopApplication`.


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