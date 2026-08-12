---
layout: home

hero:
  name: Kafka Bus
  text: PHP client for Apache Kafka
  tagline: Typed messages, a middleware pipeline, and convenient testing without the boilerplate
  actions:
    - theme: brand
      text: Quick Start
      link: /docs/installation
    - theme: alt
      text: GitHub
      link: https://github.com/kafka-bus/kafka-bus

features:
  - icon: 🚌
    title: kafka-bus/core
    details: The core of the ecosystem. A Bus facade with support for multiple connections, topic routing, a middleware pipeline, and built-in fakes for testing.
    link: /docs/installation
    linkText: Core Documentation

  - icon: 🟥
    title: kafka-bus/laravel-bridge
    details: Laravel integration — package auto-discovery, ready-made config, Artisan commands for managing workers, and KafkaBus::fake() for tests.
    link: /docs/laravel/installation
    linkText: Laravel Documentation

  - icon: 📨
    title: kafka-bus/messages
    details: Typed Payload, JsonMessage, and DomainMessage with automatic field casting, create/update/delete event support, and test factories.
    link: /docs/components/messages
    linkText: Messages Documentation

  - icon: ✅
    title: kafka-bus/commiter
    details: Idempotent message processing via middleware. Tracks processed messages, skips duplicates, and limits the number of retry attempts.
    link: /docs/components/commiter
    linkText: Commiter Documentation
---

<div class="home-features-section">
<h2 class="home-features-title">Features</h2>
<p class="home-features-subtitle">Everything you need for reliable Kafka integration in PHP</p>

<FeatureCard icon="🗂️" title="Topic Registry and Routing" details="Maps short logical keys to physical topic names — a single source of truth for the whole application. ConsumerRoutesBuilder routes incoming messages to handlers by key; PublisherRoutesBuilder automatically resolves the target topic from the message class." link="/docs/topics">

::: code-group

```php [Topics]
$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1',         'products'))
    ->add(new Topic('production.fact.orders.1',           'orders'))
    ->add(new Topic('production.event.user-registered.1', 'user-registered'));
```

```php [Consumer]
// Logical key → handler mapping
$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->add(new RouteInfo('orders',   new OrderHandler()))
    ->build();

// Start listening — topic names are resolved from the registry
$bus->listener('default')->listen();
```

```php [Producer]
// Message class → logical key mapping
$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductCreatedMessage::class, 'products')
    ->add(OrderCreatedMessage::class,   'orders')
    ->build();

// Publishing — the topic is resolved automatically from the class
$bus->publish(new ProductCreatedMessage($product));
$bus->publish(new OrderCreatedMessage($order));
```

:::

</FeatureCard>

<FeatureCard icon="⛓️" title="Middleware Pipeline" details="Attach middleware globally for a worker — every message on every topic passes through the chain. Each middleware receives the pipeline and decides whether to pass control to the next link." link="/docs/consumer/pipeline">

::: code-group

```php [Consumer]
use KafkaBus\Core\Consumers\Pipelines\ConsumerPipelineHandler;
use KafkaBus\Core\Consumers\Pipelines\ConsumerPipelineMiddleware;
use KafkaBus\Core\Interfaces\Pipelines\PipelineInterface;

class LoggingMiddleware implements ConsumerPipelineMiddleware
{
    /**
     * @param PipelineInterface<ConsumerPipelineHandler> $pipeline
     * @return PipelineInterface<ConsumerPipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $start = hrtime(true);
        
        $pipeline->continue();
        
        $elapsed = (hrtime(true) - $start) / 1e6;
        echo "Processed in {$elapsed}ms: " . $pipeline->handler()->target()->topicName();
        
        return $pipeline;
    }
}
```

```php [Producer]
use KafkaBus\Commiter\Interfaces\HasIdempotency;
use KafkaBus\Commiter\Repositories\IdempotencyMessageRepository;
use KafkaBus\Core\Interfaces\Pipelines\PipelineInterface;
use KafkaBus\Core\Producers\Pipelines\ProducerPipelineHandler;
use KafkaBus\Core\Producers\Pipelines\ProducerPipelineMiddleware;

final readonly class ProducerIdempotencyMiddleware implements ProducerPipelineMiddleware
{
    /**
     * @param PipelineInterface<ProducerPipelineHandler> $pipeline
     * @return PipelineInterface<ProducerPipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $message = $pipeline->handler()
            ->target();

        if ($message instanceof HasIdempotency) {
            $pipeline->handler()
                ->withHeader(IdempotencyMessageRepository::HEADER_NAME, $message->getIdempotencyKey());
        }

        return $pipeline->continue();
    }
}
```

:::

</FeatureCard>

<FeatureCard icon="📝" title="Typed Messages" details="Declare typed Payload classes with automatic field casting — int, float, datetime, nested objects, and collections. DomainMessage adds a create/update/delete event type and a list of changed (dirty) fields." link="/docs/components/messages">

```php
class ProductPayload extends Payload
{
    protected function definitionCasters(): array
    {
        return [
            'id'         => new IntegerCaster(),
            'price'      => new FloatCaster(),
            'published'  => new DateTimeCaster('Y-m-d'),
            'category'   => new PayloadCaster(CategoryPayload::class),
            'attributes' => new CollectionCaster(new PayloadCaster(AttributePayload::class)),
        ];
    }
}

$product = ProductPayload::from([
    'id' => '42', 'price' => '9.99',
    'category' => ['id' => 1, 'name' => 'Electronics'],
]);

echo $product->id;             // int(42)
echo $product->price;          // float(9.99)
echo $product->category->name; // "Electronics"
```

</FeatureCard>

<FeatureCard icon="🔑" title="Message Delivery Contracts" details="Implement HasKey, HasHeaders, or HasPartition on any message class to control its delivery. All three interfaces are independent and can be freely combined." link="/docs/producer/producing">

::: code-group

```php [HasKey]
use KafkaBus\Core\Interfaces\Producers\Messages\HasKey;

final readonly class OrderCreatedMessage implements ProducerMessageInterface, HasKey
{
    public function getKey(): ?string
    {
        return (string) $this->orderId; // all events for one order go to the same partition
    }
}
```

```php [HasHeaders]
use KafkaBus\Core\Interfaces\Producers\Messages\HasHeaders;

final readonly class OrderCreatedMessage implements ProducerMessageInterface, HasHeaders
{
    public function getHeaders(): array
    {
        return [
            'event'   => 'order.created',
            'version' => '2',
            'source'  => 'order-service',
        ];
    }
}
```

```php [HasPartition]
use KafkaBus\Core\Interfaces\Producers\Messages\HasPartition;

final readonly class OrderCreatedMessage implements ProducerMessageInterface, HasPartition
{
    public function getPartition(): int
    {
        return 3; // always deliver to partition 3
    }
}
```

:::

</FeatureCard>

<FeatureCard icon="🎯" title="Flexible Handler Typing" details="Declare the type you need — the bus delivers it. Handlers can receive ConsumerMessageInterface for full access, string for the raw payload, array for decoded JSON, or RdKafka\Message for metadata. The #[MessageFactory] attribute hydrates typed objects automatically." link="/docs/consumer/consuming">

::: code-group

```php [DomainMessage]
class ProductHandler
{
    #[MessageFactory(new DomainMessageFactory(ProductMessage::class))]
    public function __invoke(ProductMessage $message): void
    {
        echo $message->getEvent()->value; // 'create' | 'update' | 'delete'
        echo $message->id;   // int — auto-cast
        echo $message->name; // string

        if (in_array('price', $message->getDirty())) {
            $this->updatePriceIndex($message->id, $message->price);
        }
    }
}
```

```php [RdKafka\Message]
class AuditHandler
{
    public function __invoke(RdKafka\Message $msg): void
    {
        echo $msg->key;       // Kafka partition key
        echo $msg->offset;    // current offset
        echo $msg->partition; // partition number
        echo $msg->timestamp; // message timestamp
    }
}
```

:::

</FeatureCard>

<FeatureCard icon="🛡️" title="Idempotent Processing" details="ConsumerCommiterMiddleware deduplicates messages by Kafka msgId or a stable x-idempotency-key header. Configurable attempt limit. Any storage — implement RepositorySourceInterface for a database, Redis, or anything else." link="/docs/components/commiter">

::: code-group

```php [Consumer]
// Attach to the worker's pipeline
new ConsumerCommiterMiddleware(
    repository: new IdempotencyMessageRepository(
        new DatabaseRepositorySource()
    ),
    logger:     $logger, // PSR-3
    maxAttempt: 3,       // -1 for unlimited attempts
)
```

```php [Producer]
// Implement HasIdempotency on the message
final readonly class ProductCreatedMessage implements HasIdempotency
{
    public function getIdempotencyKey(): string
    {
        return "product-{$this->id}-v{$this->version}";
    }
}

// The middleware automatically adds the x-idempotency-key header
new ProducerIdempotencyMiddleware()
```

:::

</FeatureCard>

<FeatureCard icon="🔀" title="Multiple Connections" details="Register any number of named connections to different Kafka clusters. The bus uses the default connection; switch to another one via onConnection() for a specific call." link="/docs/configuration">

```php
$connectionRegistry = new ConnectionRegistry(
    driverRegistry: $driverRegistry,
    connections: [
        'main' => new KafkaConnectionConfig([
            'metadata.broker.list' => 'kafka-main:9092',
        ]),
        'analytics' => new KafkaConnectionConfig([
            'metadata.broker.list' => 'kafka-analytics:9092',
            'security.protocol'    => 'SASL_SSL',
        ]),
    ],
    defaultConnectionName: 'main',
);

// Publish to a specific cluster
$bus->publish(new ProductCreatedMessage($product));           // main
$bus->onConnection('analytics')->publish(new PageViewEvent()); // analytics
```

</FeatureCard>

<FeatureCard icon="🟥" title="Laravel Integration" details="Install kafka-bus/laravel-bridge for package auto-discovery, ready-made config, and Artisan commands. KafkaBus::fake() intercepts all bus calls — no real broker needed. Assert published messages, feed the fake consumer, and check commits." link="/docs/laravel/installation">

::: code-group

```php [Setup]
// config/kafka-bus.php — ready to use after publishing
'connections' => [
    'kafka' => [
        'driver'  => 'kafka',
        'options' => ['metadata.broker.list' => env('KAFKA_BROKERS', 'localhost:9092')],
    ],
],

// Manage workers via Artisan
// php artisan kafka:consume products
// php artisan kafka:consume products orders

// In production
KafkaBus::publish(new ProductCreatedMessage($product));
KafkaBus::onConnection('analytics')->publish(new PageViewEvent());

// In tests
KafkaBus::fake();
KafkaBus::assertPublished(ProductCreatedMessage::class);
```

```php [Producer Tests]
it('publishes a message when a product is created', function () {
    KafkaBus::fake();

    app(CreateProductAction::class)->execute(id: 1, name: 'Laptop');

    KafkaBus::assertPublished(
        ProductMessage::class,
        fn($msg) => str_contains($msg->payload, '"id":1')
                 && isset($msg->headers['x-idempotency-key'])
    );
    KafkaBus::assertPublishedTimes(ProductMessage::class, 1);
    KafkaBus::assertNotPublished(OrderMessage::class);
});
```

```php [Consumer Tests]
it('stores a product when the message is received', function () {
    KafkaBus::fake();

    KafkaBus::addMessage(
        MessageFactory::for()
            ->withTopicKey('products')
            ->make('{"id":42,"name":"Laptop"}')
    );
    KafkaBus::listen('products');

    expect(Product::find(42)->name)->toBe('Laptop');
    KafkaBus::assertCommitted('products');
});
```

:::

</FeatureCard>

<FeatureCard icon="📤" title="Batch Publishing" details="Publish multiple messages in a single bus call via MessageBatch. Every message in the batch goes through the full pipeline — middleware, routing, and serialization — with no manual loop." link="/docs/producer/producing">

```php
$messages = array_map(
    fn(Product $product) => new ProductCreatedMessage(
        id:       $product->id,
        name:     $product->name,
        category: $product->category,
    ),
    $products,
);

$bus->publishBatch(MessageBatch::fromArray($messages));

// Batch to a specific connection
$bus->onConnection('analytics')
    ->publishBatch(MessageBatch::fromArray($events));
```

</FeatureCard>

</div>

<style>
.home-features-section {
  max-width: 1152px;
  margin: 0 auto;
  padding: 64px 24px 80px;
}

.home-features-title {
  font-size: 32px !important;
  font-weight: 700;
  text-align: center;
  margin: 0 0 10px !important;
  padding: 0 !important;
  border: none !important;
}

.home-features-subtitle {
  font-size: 15px;
  color: var(--vp-c-text-2);
  text-align: center;
  margin: 0 0 56px !important;
}
</style>
