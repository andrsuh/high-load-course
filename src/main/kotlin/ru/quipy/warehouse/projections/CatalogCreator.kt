package ru.quipy.warehouse.projections

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository
import org.springframework.stereotype.Service
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import ru.quipy.warehouse.api.ProductAggregate
import ru.quipy.warehouse.api.ProductCreatedEvent
import java.util.*
import javax.annotation.PostConstruct

@Service
class CatalogCreator {

    val logger: Logger = LoggerFactory.getLogger(CatalogCreator::class.java)

    @Autowired
    lateinit var productRepository: ProductRepository

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(ProductAggregate::class, "wh-catalog-creator-subscriber", retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)) {
            `when`(ProductCreatedEvent::class) { event ->
                logger.info("Product created: {}", event.title)
                productRepository.save(Product(event.id, event.title))
            }
        }
    }

    @Document
    class Product(
        @Id
        val id: UUID,
        val title: String
    )
}

@Repository
interface ProductRepository : MongoRepository<CatalogCreator.Product, UUID>
