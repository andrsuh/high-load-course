package ru.quipy.config

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.mongo.MongoClientSettingsBuilderCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.delivery.api.DeliveryAggregate
import ru.quipy.delivery.logic.DeliveryAggregateState
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.logic.OrderAggregateState
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.streams.AggregateEventStreamManager
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.warehouse.api.BookingAggregate
import ru.quipy.warehouse.api.ProductAggregate
import ru.quipy.warehouse.logic.BookingAggregateState
import ru.quipy.warehouse.logic.ProductAggregateState
import java.util.*
import javax.annotation.PostConstruct


/**
 * This files contains some configurations that you might want to have in your project. Some configurations are
 * made in for the sake of demonstration and not required for the library functioning. Usually you can have even
 * more minimalistic config
 *
 * Take into consideration that we autoscan files searching for Aggregates, Events and StateTransition functions.
 * Autoscan enabled via [event.sourcing.auto-scan-enabled] property.
 *
 * But you can always disable it and register all the classes manually like this
 * ```
 * @Autowired
 * private lateinit var aggregateRegistry: AggregateRegistry
 *
 * aggregateRegistry.register(ProjectAggregate::class, ProjectAggregateState::class) {
 *     registerStateTransition(TagCreatedEvent::class, ProjectAggregateState::tagCreatedApply)
 *     registerStateTransition(TaskCreatedEvent::class, ProjectAggregateState::taskCreatedApply)
 *     registerStateTransition(TagAssignedToTaskEvent::class, ProjectAggregateState::tagAssignedApply)
 * }
 * ```
 */
@Configuration
class EventSourcingLibConfiguration {

    private val logger = LoggerFactory.getLogger(EventSourcingLibConfiguration::class.java)

    @Autowired
    private lateinit var eventSourcingServiceFactory: EventSourcingServiceFactory

    @Autowired
    private lateinit var eventStreamManager: AggregateEventStreamManager

    /**
     * Use this object to create/update the aggregate
     */
    @Bean
    fun productEsService() = eventSourcingServiceFactory.create<UUID, ProductAggregate, ProductAggregateState>()

    @Bean
    fun bookingEsService() = eventSourcingServiceFactory.create<UUID, BookingAggregate, BookingAggregateState>()

    @Bean
    fun ordersEsService() = eventSourcingServiceFactory.create<UUID, OrderAggregate, OrderAggregateState>()

    @Bean
    fun paymentsEsService() = eventSourcingServiceFactory.create<UUID, PaymentAggregate, PaymentAggregateState>()

    @Bean
    fun deliveryEsService() = eventSourcingServiceFactory.create<UUID, DeliveryAggregate, DeliveryAggregateState>()

    @PostConstruct
    fun init() {
        // Demonstrates how you can set up the listeners to the event stream
        eventStreamManager.maintenance {
            onRecordHandledSuccessfully { streamName, eventName ->
                logger.debug("Stream $streamName successfully processed record of $eventName")
            }

            onBatchRead { streamName, batchSize ->
                logger.debug("Stream $streamName read batch size: $batchSize")
            }
        }
    }
}