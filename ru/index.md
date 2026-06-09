---
layout: home

hero:
  name: Kafka Bus
  text: PHP клиент для Apache Kafka
  tagline: Типизированные сообщения, middleware pipeline и удобное тестирование без лишнего кода
  actions:
    - theme: brand
      text: Быстрый старт
      link: /ru/docs/installation
    - theme: alt
      text: GitHub
      link: https://github.com/kafka-bus/kafka-bus

features:
  - icon: 🚌
    title: kafka-bus/core
    details: Ядро экосистемы. Фасад Bus с поддержкой множественных подключений, маршрутизацией топиков, middleware pipeline и встроенными fake-заглушками для тестирования.
    link: /ru/docs/installation
    linkText: Документация Core

  - icon: 🟥
    title: kafka-bus/laravel-bridge
    details: Интеграция с Laravel — автодискавери, готовый конфиг, Artisan команды для управления воркерами и KafkaBus::fake() для тестов.
    link: /ru/docs/laravel/installation
    linkText: Документация Laravel

  - icon: 📨
    title: kafka-bus/messages
    details: Типизированные Payload, JsonMessage и DomainMessage с автоматическим кастингом полей, поддержкой событий create/update/delete и фабриками для тестов.
    link: /ru/docs/components/messages
    linkText: Документация Messages

  - icon: ✅
    title: kafka-bus/commiter
    details: Идемпотентная обработка сообщений через middleware. Отслеживает обработанные сообщения, пропускает дубликаты и ограничивает число попыток.
    link: /ru/docs/components/commiter
    linkText: Документация Commiter
---

<div class="home-features-section">
<h2 class="home-features-title">Возможности</h2>
<p class="home-features-subtitle">Всё необходимое для надёжной работы с Kafka на PHP</p>

<FeatureCard icon="🎯" title="Умный резолвинг обработчиков" details="Объявите нужный тип — шина его доставит. Обработчики получают ConsumerMessageInterface для полного доступа, string для сырого payload, array для декодированного JSON или RdKafka\Message для метаданных. Атрибут #[MessageFactory] для автоматической гидратации типизированных объектов." link="/ru/docs/consumer/consuming">

::: code-group

```php [DomainMessage]
class ProductHandler
{
    #[MessageFactory(new DomainMessageFactory(ProductMessage::class))]
    public function __invoke(ProductMessage $message): void
    {
        echo $message->getEvent()->value; // 'create' | 'update' | 'delete'
        echo $message->id;   // int — автокаст
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
        echo $msg->key;       // ключ партиции Kafka
        echo $msg->offset;    // текущий offset
        echo $msg->partition; // номер партиции
        echo $msg->timestamp; // временная метка сообщения
    }
}
```

:::

</FeatureCard>

<FeatureCard icon="🗂️" title="Topic Registry и маршрутизация" details="Маппинг коротких логических ключей на физические имена топиков — единый источник истины для всего приложения. ConsumerRoutesBuilder маршрутизирует входящие сообщения к обработчикам по ключу; PublisherRoutesBuilder автоматически определяет целевой топик по классу сообщения." link="/ru/docs/topics">

::: code-group

```php [Topics]
$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1',         'products'))
    ->add(new Topic('production.fact.orders.1',           'orders'))
    ->add(new Topic('production.event.user-registered.1', 'user-registered'));
```

```php [Consumer]
// Маппинг логического ключа → обработчик
$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->add(new RouteInfo('orders',   new OrderHandler()))
    ->build();

// Запуск — имена топиков берутся из реестра
$bus->listener('default')->listen();
```

```php [Producer]
// Маппинг класса сообщения → логический ключ
$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductCreatedMessage::class, 'products')
    ->add(OrderCreatedMessage::class,   'orders')
    ->build();

// Публикация — топик определяется автоматически по классу
$bus->publish(new ProductCreatedMessage($product));
$bus->publish(new OrderCreatedMessage($order));
```

:::

</FeatureCard>

<FeatureCard icon="⛓️" title="Middleware Pipeline" details="Подключайте middleware глобально для воркера — каждое сообщение в каждом топике проходит через цепочку. Каждый middleware получает pipeline и решает, передавать ли управление следующему звену." link="/ru/docs/consumer/pipeline">

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
        $start   = hrtime(true);
        $result  = $pipeline->continue();
        $elapsed = (hrtime(true) - $start) / 1e6;
        echo "Обработано за {$elapsed}мс: " . $pipeline->handler()->target()->topicName();
        return $result;
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

<FeatureCard icon="📝" title="Типизированные сообщения" details="Объявляйте типизированные классы Payload с автоматическим кастингом полей — int, float, datetime, вложенные объекты и коллекции. DomainMessage добавляет тип события create/update/delete и список изменённых полей (dirty)." link="/ru/docs/components/messages">

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
    'category' => ['id' => 1, 'name' => 'Электроника'],
]);

echo $product->id;             // int(42)
echo $product->price;          // float(9.99)
echo $product->category->name; // "Электроника"
```

</FeatureCard>

<FeatureCard icon="🛡️" title="Идемпотентная обработка" details="ConsumerCommiterMiddleware дедуплицирует сообщения по Kafka msgId или стабильному заголовку x-idempotency-key. Настраиваемый лимит попыток. Любое хранилище — реализуйте RepositorySourceInterface для базы данных, Redis или другого." link="/ru/docs/components/commiter">

::: code-group

```php [Consumer]
// Подключение к pipeline воркера
new ConsumerCommiterMiddleware(
    repository: new IdempotencyMessageRepository(
        new DatabaseRepositorySource()
    ),
    logger:     $logger, // PSR-3
    maxAttempt: 3,       // -1 для безлимитных попыток
)
```

```php [Producer]
// Реализация HasIdempotency на сообщении
final readonly class ProductCreatedMessage implements HasIdempotency
{
    public function getIdempotencyKey(): string
    {
        return "product-{$this->id}-v{$this->version}";
    }
}

// Middleware автоматически добавляет заголовок x-idempotency-key
new ProducerIdempotencyMiddleware()
```

:::

</FeatureCard>

<FeatureCard icon="🧪" title="Тестирование первого класса" details="KafkaBus::fake() перехватывает все вызовы шины — реальный брокер не нужен. Проверяйте какие сообщения были опубликованы, сколько раз и с каким payload. Подавайте сообщения в фейковый consumer и проверяйте коммиты. Только в kafka-bus/laravel-bridge." link="/ru/docs/laravel/testing">

::: code-group

```php [Producer]
it('публикует сообщение при создании продукта', function () {
    KafkaBus::fake();

    app(CreateProductAction::class)->execute(id: 1, name: 'Ноутбук');

    KafkaBus::assertPublished(
        ProductMessage::class,
        fn($msg) => str_contains($msg->payload, '"id":1')
                 && isset($msg->headers['x-idempotency-key'])
    );
    KafkaBus::assertPublishedTimes(ProductMessage::class, 1);
    KafkaBus::assertNotPublished(OrderMessage::class);
});
```

```php [Consumer]
it('сохраняет продукт при получении сообщения', function () {
    KafkaBus::fake();

    KafkaBus::addMessage(
        MessageFactory::for()
            ->withTopicKey('products')
            ->make('{"id":42,"name":"Ноутбук"}')
    );
    KafkaBus::listen('products');

    expect(Product::find(42)->name)->toBe('Ноутбук');
    KafkaBus::assertCommitted('products');
});
```

:::

</FeatureCard>

<FeatureCard icon="🔀" title="Множественные подключения" details="Регистрируйте любое количество именованных подключений к разным кластерам Kafka. Шина использует подключение по умолчанию; переключайтесь на другое через onConnection() в конкретном вызове." link="/ru/docs/configuration">

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

// Публикация в конкретный кластер
$bus->publish(new ProductCreatedMessage($product));           // main
$bus->onConnection('analytics')->publish(new PageViewEvent()); // analytics
```

</FeatureCard>

<FeatureCard icon="🟥" title="Интеграция с Laravel" details="Установите kafka-bus/laravel-bridge для автодискавери, готового конфига и Artisan команд. Фасад KafkaBus зеркалит API ядра и добавляет метод fake(), работающий как Event::fake() в тестах." link="/ru/docs/laravel/installation">

```php
// config/kafka-bus.php — готов к использованию после публикации
'connections' => [
    'kafka' => [
        'driver'  => 'kafka',
        'options' => ['metadata.broker.list' => env('KAFKA_BROKERS', 'localhost:9092')],
    ],
],

// Управление воркерами через Artisan
// php artisan kafka:consume products
// php artisan kafka:consume products orders

// В продакшне
KafkaBus::publish(new ProductCreatedMessage($product));
KafkaBus::onConnection('analytics')->publish(new PageViewEvent());

// В тестах
KafkaBus::fake();
KafkaBus::assertPublished(ProductCreatedMessage::class);
```

</FeatureCard>

<FeatureCard icon="📤" title="Пакетная публикация" details="Публикуйте несколько сообщений за один вызов шины через MessageBatch. Все сообщения в батче проходят через полный pipeline — middleware, маршрутизацию и сериализацию — без ручного цикла." link="/ru/docs/producer/producing">

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

// Батч в конкретное подключение
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
  border-top: 1px solid var(--vp-c-divider);
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
