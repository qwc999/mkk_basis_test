# Асинхронный сервис платежей

Сервис принимает платёж, сохраняет его в PostgreSQL, эмулирует обработку и отправляет
результат на webhook клиента. Реализованы обязательные требования задания: защищённые
эндпоинты создания и просмотра, идемпотентность, транзакционный Outbox, один consumer,
три попытки с экспоненциальной задержкой и Dead Letter Queue.

Стек: Python 3.13, FastAPI, Pydantic v2, асинхронный SQLAlchemy 2.0 / asyncpg,
PostgreSQL, Alembic, RabbitMQ / FastStream, httpx, Docker Compose.
Версии прямых и транзитивных зависимостей зафиксированы в `requirements*.txt`.

```text
POST API → payments + outbox (одна транзакция)
         → отправитель Outbox → RabbitMQ payments.new
         → consumer → результат в БД → webhook → отметка доставки → ACK
```

Отправитель Outbox и один подписчик работают в процессе `consumer`. Эмуляция занимает
2–5 секунд: 90% результатов — `succeeded`, 10% — `failed`. Оба результата требуют webhook.

## Запуск

Нужны Docker с работающим Linux-движком и Docker Compose v2. Команды выполняются из корня
репозитория. Python на хосте нужен только для локальных проверок и smoke-сценария.

1. Скопируйте настройки: `cp .env.example .env` в Bash или
   `Copy-Item .env.example .env` в PowerShell.
2. Замените демонстрационные `API_KEY`, `POSTGRES_PASSWORD`, `RABBITMQ_PASSWORD` и согласуйте
   их с `DATABASE_URL` и `RABBITMQ_URL`. Специальные символы паролей в URL кодируйте
   percent-encoding. Внутри Compose хосты — `postgres` и `rabbitmq`.
3. Соберите образы, дождитесь БД и брокера, примените миграции и запустите приложение:

```sh
docker compose build api consumer
docker compose up -d --wait postgres rabbitmq
docker compose run --rm api python -m alembic upgrade head
docker compose up -d api consumer
docker compose ps
docker compose logs -f api consumer
```

API доступен по `http://localhost:8000`. Миграции применяются явно перед запуском API;
повторный `upgrade head` безопасен. Проверка соответствия ORM и схемы:

```sh
docker compose run --rm api python -m alembic check
```

`docker compose down` останавливает сервисы и сохраняет данные в `postgres_data` и
`rabbitmq_data`. Рабочие PostgreSQL и RabbitMQ не публикуют порты на хост;
порт API привязан к `127.0.0.1`. `.env` не хранится в Git.

### Переменные окружения

| Переменная | Назначение |
| --- | --- |
| `API_KEY` | Непустой статический ключ API |
| `DATABASE_URL` | Подключение `postgresql+asyncpg://...`; нужно API, consumer и Alembic |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Инициализация PostgreSQL; должны соответствовать DATABASE_URL |
| `RABBITMQ_URL` | Подключение consumer, схема `amqp` или `amqps` |
| `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_VHOST` | Инициализация RabbitMQ; должны соответствовать RABBITMQ_URL |
| `API_PORT` | Порт API на хосте, по умолчанию `8000` |
| `ENABLE_DOCS` | Локальный Swagger, по умолчанию `false` |
| `OUTBOX_POLL_INTERVAL` | Пауза пустого Outbox, по умолчанию `1` секунда |
| `OUTBOX_PUBLISH_TIMEOUT` | Лимит публикации и подтверждения Outbox/retry/DLQ, по умолчанию `5` секунд |

Оба интервала — конечные числа в диапазоне `(0, 60]`. API работает без RabbitMQ и при его
недоступности продолжает сохранять платежи и Outbox. Consumer не использует API_KEY;
его отдельная команда запуска — `python -m app.worker`. Для запуска вне Compose нужны
адреса БД и брокера, доступные из соответствующего процесса.

## API

Платёжные эндпоинты требуют `X-API-Key`; отсутствие или неверное значение даёт `401`.
Swagger и `/openapi.json` по умолчанию отключены, ReDoc отключён всегда.
Для локальной проверки задайте `ENABLE_DOCS=true` в `.env`, пересоберите и перезапустите
API: `docker compose up -d --build --no-deps api`. Откройте `http://localhost:8000/docs`,
нажмите **Authorize** и введите значение `API_KEY` для запросов к платежам.
В этом режиме сама документация и схема доступны без ключа; используйте его только локально.
Примеры ниже — для Bash; в PowerShell
используйте `curl.exe` или `Invoke-RestMethod` с теми же заголовками.

Задайте свой ключ из `.env` и URL получателя, доступный **из consumer-контейнера**:

```sh
export API_KEY='your-local-api-key'
export WEBHOOK_URL='https://your-receiver.example/payments'
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: order-123' \
  -H 'Content-Type: application/json' \
  --data "{\"amount\":\"1500.00\",\"currency\":\"RUB\",\"description\":\"Order payment\",\"metadata\":{\"order_id\":\"order-123\"},\"webhook_url\":\"$WEBHOOK_URL\"}"
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "created_at": "2026-10-19T12:00:00Z"
}
```

Получение платежа — с UUID из ответа:

```sh
curl -i http://localhost:8000/api/v1/payments/550e8400-e29b-41d4-a716-446655440000 \
  -H "X-API-Key: $API_KEY"
```

Ответ `200 OK` после обработки:

```json
{
  "payment_id": "550e8400-e29b-41d4-a716-446655440000",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order payment",
  "metadata": {"order_id": "order-123"},
  "status": "succeeded",
  "webhook_url": "https://your-receiver.example/payments",
  "created_at": "2026-10-19T12:00:00Z",
  "processed_at": "2026-10-19T12:00:03Z"
}
```

У нового платежа `processed_at = null`. Повторяйте GET до `succeeded` или `failed`;
ожидание включает polling Outbox и предыдущие платежи. Неизвестный UUID даёт `404`,
некорректный UUID или тело запроса — `422`.

Сумма — положительный Decimal от `0.01` до `9999999999999999.99`, без округления;
передавайте её строкой, чтобы клиент не терял точность. Валюты: `RUB`, `USD`, `EUR`.
Описание — непустая строка до 1000 символов, webhook — HTTP(S) URL.
`metadata` — JSON-объект, по умолчанию `{}`; остальные поля обязательны.
Неизвестные поля отклоняются.

### Идемпотентность

`Idempotency-Key` обязателен для POST: непустая строка до 255 символов.
Новый ключ создаёт один Payment и одно событие Outbox в одной транзакции.
Тот же ключ с теми же нормализованными данными возвращает `202`, исходные UUID и created_at,
текущий статус; новые записи и события не создаются. Другие данные с тем же ключом дают `409`.
Правило действует и при конкурентных запросах; `1500.0` и `1500.00` эквивалентны.

## Webhook

POST на сохранённый `webhook_url`, заголовки `Content-Type: application/json` и
`Idempotency-Key: <UUID платежа>`. Клиентский ключ создания платежа в уведомление не передаётся.

```json
{
  "payment_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "processed_at": "2026-10-19T12:00:03+00:00"
}
```

Все поля берутся из сохранённого результата; статус — `succeeded` или `failed`, сумма —
строка с двумя знаками, время — UTC. Успех — любой HTTP 2xx. Остальные статусы, включая 3xx,
сетевые ошибки и таймауты вызывают retry. Таймаут — 5 секунд, редиректы отключены.
У worker один переиспользуемый HTTP-клиент; транзакция и блокировка БД во время HTTP не удерживаются.

После успеха отдельная транзакция сохраняет `webhook_delivered_at`, затем подтверждается
сообщение. При заполненной отметке повторный HTTP не выполняется. Поле служебное и в GET
не включается: окончательный статус в API ещё не означает успешную доставку уведомления.
Ошибка webhook не меняет статус и processed_at платежа.

### Retry и DLQ

| Попытка | Когда выполняется | При технической ошибке |
| --- | --- | --- |
| 1 | Первоначальное сообщение | Retry через 1 секунду |
| 2 | После очереди `payments.retry.1s` | Retry через 2 секунды |
| 3 | После очереди `payments.retry.2s` | Отправка в `payments.dlq` |

Durable direct exchange `payments` направляет события в `payments.new`, `payments.retry` —
в две durable TTL-очереди с возвратом в `payments.new`, `payments.dlx` — в `payments.dlq`.
Счётчик — целочисленный `x-attempt` от 1 до 3; отсутствующий заголовок означает 1.
Повтор уже завершённого платежа пропускает эмуляцию и доставляет только webhook.

`invalid_message` и `payment_not_found` отправляются сразу в DLQ;
`processing_error` и `webhook_error` — после исчерпания попыток.
Категория хранится в заголовке `x-error-category` сообщения DLQ.
При передаче сохраняются тело и message_id; сообщения persistent, с mandatory routing
и положительным publisher confirm. ACK исходного сообщения разрешён только после
подтверждённой передачи. Сбой передачи останавливает worker без ACK; Compose перезапускает его.

Посмотреть очереди можно командой (vhost из `.env.example`; замените при другой настройке):

```sh
docker compose exec rabbitmq rabbitmqctl list_queues -p payments name messages_ready messages_unacknowledged
```

## Проверки

Полный набор на выделенных PostgreSQL и RabbitMQ, без рабочего `.env`:

```sh
docker compose -p mkk-payments-test -f compose.test.yaml up --build --abort-on-container-exit --exit-code-from tests
docker compose -p mkk-payments-test -f compose.test.yaml run --rm --no-deps tests python -m ruff check --no-cache app tests migrations scripts
docker compose -p mkk-payments-test -f compose.test.yaml run --rm --no-deps tests python -m ruff format --check --no-cache app tests migrations scripts
docker compose -p mkk-payments-test -f compose.test.yaml down -v
```

Первая команда возвращает код тестов. Последняя удаляет **только тестовую** инфраструктуру
указанного Compose-проекта. Тестовая PostgreSQL использует tmpfs, БД и пользователя
`payments_test`; RabbitMQ — отдельный volume, пользователя и vhost `payments_test`.
До миграций и очистки тесты проверяют имена БД и пользователя. Не запускайте параллельные
наборы против одного тестового проекта. HTTP в интеграционных тестах заменён MockTransport;
транзакции, TTL и DLQ проверяются на реальных PostgreSQL и RabbitMQ.

Сквозная проверка сборки и запуска с чистыми volume, миграций и реального HTTP
(нужны Python 3.13 и Docker, без установки Python-зависимостей):

```sh
python scripts/smoke_processing.py
```

Существующий сценарий создаёт случайный Compose-проект и временные настройки вне checkout,
проверяет успешный webhook, повтор POST без дублей, три неуспешных HTTP-вызова с задержками
и DLQ, сохранность сообщения при перезапуске брокера и штатную остановку.
Временный HTTP-получатель запускается внутри тестового API-контейнера.
Сценарий удаляет только свои контейнеры и volume, в том числе при ошибке.

Для запуска инструментов на хосте: `python -m venv .venv`, активируйте окружение и выполните
`python -m pip install -r requirements-dev.txt`. Команды проверок:
`python -m ruff check app tests migrations scripts` и
`python -m ruff format --check app tests migrations scripts`.
Локальному `python -m pytest -q` нужны `TEST_DATABASE_URL`, `TEST_RABBITMQ_URL` и
`TEST_RABBITMQ_MANAGEMENT_URL`, указывающие на выделенные тестовые сервисы с именами выше.

## Решения и ограничения

- Payment и Outbox создаются атомарно. Отправитель выбирает записи через
  `FOR UPDATE SKIP LOCKED` и фиксирует published_at только после подтверждения брокера.
- Гарантия — **at least once**: авария между HTTP 2xx и сохранением отметки или между
  публикацией и ACK может дать дубль. Получатель должен дедуплицировать уведомления по UUID
  в Idempotency-Key. Три попытки ограничивают обычную цепочку, а не число HTTP-вызовов при авариях.
- Один consumer, `prefetch=1`; блокировка Payment удерживается во время эмуляции.
  Масштабирование webhook на несколько consumer не реализовано.
- Ключ создания глобален для сервиса, срок хранения не ограничен. Outbox и DLQ автоматически
  не очищаются; API восстановления из DLQ и рассылки по старым платежам нет.

Подробнее о транзакциях, топологии, восстановлении и остановке:
[docs/architecture.md](docs/architecture.md).
