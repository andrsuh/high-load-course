.PHONY: local-test
local-test:
	curl -v -X POST http://localhost:1234/test/run \
		-H "Content-Type: application/json" \
		-d '{"serviceName":"$(serviceName)","token":"$(token)","ratePerSecond":1,"testCount":100,"processingTimeMillis":80000}'

.PHONY: infra run logs

infra:
	docker compose -f docker-compose.yml up

logs:
	docker compose logs --tail=400 -f $(CONTAINER)

run:
	mvn spring-boot:run


.PHONY: run-local
run-local:
	PAYMENT_SERVICE_NAME=$(serviceName) PAYMENT_TOKEN=$(token) mvn spring-boot:run


.PHONY: remote-test remote-stop

# Defaults for remote testing (can be overridden on CLI)
branch ?= hw-5
accounts ?= acc-23
ratePerSecond ?= 15
testCount ?= 3000 
processingTimeMillis ?= 2500 

# accounts ?= acc-23
# ratePerSecond ?= 11 
# testCount ?= 2200 
# processingTimeMillis ?= 13000 
# profile ?= "s_0.7_60"

# accounts ?= acc-23
# ratePerSecond ?= 3 
# testCount ?= 1050 
# processingTimeMillis ?= 26000 
# runits ?= 90

remote-test:
	curl -v -X POST http://77.234.215.138:34321/run \
		-H "Content-Type: application/json" \
		-d '{"serviceName":"$(PAYMENT_SERVICE_NAME)","token":"$(PAYMENT_TOKEN)","branch":"$(branch)","accounts":"$(accounts)","ratePerSecond":$(ratePerSecond),"testCount":$(testCount),"processingTimeMillis":$(processingTimeMillis),"profile":"$(profile)",onPremises":true}'

remote-stop:
	curl -X POST http://77.234.215.138:31234/test/stop/$(PAYMENT_SERVICE_NAME)

