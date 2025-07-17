# Setup Guide:

`Maven` is used to build and manage the Java-based project. (see https://kotlinlang.org/docs/maven.html)

## Pre-requirements:

### Docker installation:
Docker will be needed to build the containers required by our projects.

Follow docker guide https://docs.docker.com/engine/install/ubuntu/

```shell
# Add Docker's official GPG key:
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
```

```shell
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

### Fork and clone repositories:

Repositories:
* https://github.com/andrsuh/high-load-course - `Shop Application`
* https://github.com/andrsuh/bombardier - `Testing System` (aka clients) && `ExternalPaymentService`

**How to fork?** - See https://docs.github.com/ru/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo <br />
**How to clone?** - See https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository

### Launch docker containers:

Command bellow deploys services and creates new containers from a Docker image, as well as networks, volumes,
and all configurations specified in the `docker-compose.yml`.
By adding the -d flag, you run the command in a separate or background mode, while maintaining control over the terminal.

```shell
docker compose up -d
```

#### high-load-course:

After command above, our database `Postgres` will be up and running.

* `Postgres` - database. (see https://www.postgresql.org/)

#### bombardier:

After command above, `Prometheus` and `Grafana` will be up and running.

* `Prometheus` - collects data. (see https://prometheus.io/)
* `Grafana` - visualizes data, metrics. (see https://grafana.com/docs/grafana/latest/)
    + Dashboards will be available at http://localhost:3000/dashboards (check `Grafana` service's `ports` in `docker-compose.yml`)

## Launch projects via `IDE`

**NB**: Before launch `high-load-course` and `bombardier` projects you must up docker containers! <br />
**NB**: Firstly you need to launch the `bombardier` and then `high-load-course` and not the other way around!

### `IntelliJ IDEA`

The first time you open the project in `IDEA`, dependencies will be automatically downloaded: jdk, etc. <br />
**NB**: It can take some time, so just wait till the end.

`Maven` is IDEA built-in, so thanks JetBrains :)

To launch the `high-load-course` project, just go to `src/main/kotlin/ru/quipy/OnlineShopApplication.kt` and run `main` function. <br />

![](/doc/images/idea_run_high_load_course.png)

To launch the `bombardier` project, just go to `src/main/kotlin/com/itmo/microservices/demo/DemoServiceApplication.kt` and run `main` function.

![](/doc/images/idea_run_bombardier.png)

### `VSCode and others`

Here everything needs to be installed **manually**:

`Ubuntu / WSL`:

```shell
sudo su
apt update
apt install default-jdk
apt install maven
```

`Java` version:
```shell
java -version
```

`Maven` version:
```shell
mvn -version
```

`Maven` commands (see https://maven.apache.org/run.html):

* Cleans the Maven project by deleting the `target` directory
    ```shell
    mvn clean
    ```
* Builds the `Maven` project and installs the project files (JAR, WAR, pom.xml, etc.) to the local repository
    ```shell
    mvn install
    ```
* Runs the test cases of the project
    ```shell
    mvn test
    ```

* More info:
    ```shell
    mvn -help
    ```

**To install dependencies and build the project:**
```shell
mvn clean install
```
or using `-DskipTests` to skip tests
```shell
mvn clean install -DskipTests
```

**To launch the project:**

**including all stages** from `pom.xml`.
```shell
mvn spring-boot:run 
```

or

run an executable `.jar` file
```shell
java -jar target/<artifactId>-<version>.jar
```

#### Summary

To **run** project use:

```shell
mvn spring-boot:run 
```

or

```shell
mvn clean install &&
java -jar target/<artifactId>-<version>.jar
```

**NB**: `<artifactId>` and `<version>` can be found in `Maven` project configuration file `pom.xml`.

##### Launch bombardier
![](/doc/images/bombardier_1.png)
![](/doc/images/bombardier_2.png)

##### Launch high-load-course
![](/doc/images/shop_1.png)
![](/doc/images/shop_2.png)

**Example**:

The part of `pom.xml`
```xml
...
<groupId>ru.quipy</groupId>
<artifactId>demo</artifactId>
<version>0.0.1-SNAPSHOT</version>
<name>OnlineShop</name>
<description>Application for resilience and highly-loaded applications course</description>
...
```

So to launch the project:

```shell
java -jar target/demo-0.0.1-SNAPSHOT.jar
```