# Tiny event sourcing library
This project is based on [Tiny Event Sourcing library](https://github.com/andrsuh/tiny-event-sourcing)

### Run MongoDb
This example uses MongoDb as an implementation of the Event store. You can see it in `pom.xml`:

```
<dependency>
    <groupId>ru.quipy</groupId>
    <artifactId>tiny-mongo-event-store-spring-boot-starter</artifactId>
    <version>${tiny.es.version}</version>
</dependency>
```

Thus, you have to run MongoDb in order to test this example. We have `docker-compose` file in the root. Run following command to start the database:

```
docker-compose up
```

### Run the application
To make the application run you can start the main class `OnlineShopApplication`.