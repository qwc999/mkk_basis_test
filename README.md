# Сервис платежей

Асинхронный API на Python 3.13, FastAPI, Pydantic v2 и SQLAlchemy 2.0 с PostgreSQL.
API создаёт и возвращает платежи, а worker через транзакционный Outbox и RabbitMQ
автоматически переводит их из `pending` в `succeeded` или `failed`.
Реализованы идемпотентность API и обработки, publisher confirms и ручной ACK после commit.
HTTP-доставка webhook, три ограниченные попытки, экспоненциальные retry-очереди и DLQ
пока не реализованы.

## Запуск через Docker Compose

Требуются Docker с запущенным Linux-движком и Docker Compose v2.

1. Скопируйте `.env.example` в `.env` (`cp .env.example .env` в Bash или
   `Copy-Item .env.example .env` в PowerShell).
2. Замените демонстрационные `API_KEY`, `POSTGRES_PASSWORD` и `RABBITMQ_PASSWORD`.
   В `DATABASE_URL` и `RABBITMQ_URL` укажите соответствующие пароли, пользователей,
   имя БД и vhost. Специальные символы в URL нужно percent-encode.
   Имена хостов внутри Compose — `postgres` и `rabbitmq`.
3. Из корня проекта выполните:

```sh
docker compose build api consumer
docker compose up -d --wait postgres rabbitmq
docker compose run --rm api python -m alembic upgrade head
docker compose up -d api consumer
docker compose logs -f api consumer rabbitmq
```

Миграции применяются **явно**, перед запуском API. Таблицы через `create_all` не создаются.
Повторная команда `alembic upgrade head` безопасна для уже обновлённой схемы.
API доступен по `http://localhost:8000`; порт можно изменить через `API_PORT`.
PostgreSQL не публикует порт на хост и хранит данные в volume `postgres_data`.
RabbitMQ хранит durable-топологию и persistent-сообщения в volume `rabbitmq_data`.
Для остановки используйте `docker compose down` — оба рабочих volume сохраняются.

Обязательные настройки приложения: `DATABASE_URL` (схема `postgresql+asyncpg`) и непустой
`API_KEY`. API зависит только от PostgreSQL, к RabbitMQ не подключается.
Настройки worker: `DATABASE_URL` и `RABBITMQ_URL` (схема `amqp` или `amqps`),
`OUTBOX_POLL_INTERVAL` (по умолчанию 1 секунда), `OUTBOX_PUBLISH_TIMEOUT` (5 секунд).
Оба интервала должны быть положительными конечными числами, не более 60 секунд.
Worker не требует `API_KEY`; Alembic требует только `DATABASE_URL`.
Ошибки обязательных настроек показывают их имена без значений.
`RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_VHOST` нужны Compose для инициализации брокера.

Отдельная команда запуска worker — `python -m app.worker`; Compose запускает её в сервисе
`consumer`. При запуске вне Docker нужны URL доступных с хоста PostgreSQL и RabbitMQ.
Отправитель Outbox работает фоновой задачей этого же процесса. Дополнительного контейнера нет.

## Примеры API

Оба эндпоинта требуют `X-API-Key` из вашей конфигурации. Отсутствующий или неверный ключ
даёт `401`. Swagger, ReDoc и `/openapi.json` отключены.

Примеры ниже для Bash; в Windows можно использовать WSL/Git Bash или `Invoke-RestMethod`.
В переменную `API_KEY` передайте своё локальное значение (не коммитьте его).

```sh
export API_KEY='your-local-api-key'
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: order-123' \
  -H 'Content-Type: application/json' \
  --data '{"amount":"1500.00","currency":"RUB","description":"Оплата заказа","metadata":{"order_id":"order-123"},"webhook_url":"https://example.com/webhooks/payments"}'
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "created_at": "2026-10-19T12:00:00Z"
}
```

Подставьте полученный UUID:

```sh
curl -i http://localhost:8000/api/v1/payments/550e8400-e29b-41d4-a716-446655440000 \
  -H "X-API-Key: $API_KEY"
```

Ответ `200 OK` содержит `payment_id`, `amount` (строка с двумя знаками после точки),
`currency`, `description`, `metadata`, `status`, `webhook_url`, `created_at`, `processed_at`.
Ключ идемпотентности в ответ не включается. Неизвестный UUID даёт `404`,
некорректный UUID — `422`. После создания повторяйте GET до `succeeded` или `failed`:
вместе с окончательным статусом появится `processed_at` в UTC. Эмуляция занимает 2–5 секунд;
к этому времени добавляется ожидание Outbox и предыдущих платежей в очереди.

Например, после POST задайте `PAYMENT_ID` из ответа и дождитесь результата:

```sh
export PAYMENT_ID='550e8400-e29b-41d4-a716-446655440000'
python - <<'PY'
import json, os, time, urllib.request
url = 'http://localhost:8000/api/v1/payments/' + os.environ['PAYMENT_ID']
for _ in range(30):
    request = urllib.request.Request(url, headers={'X-API-Key': os.environ['API_KEY']})
    with urllib.request.urlopen(request, timeout=5) as response:
        payment = json.load(response)
    if payment['status'] in ('succeeded', 'failed'):
        print(json.dumps(payment, ensure_ascii=False))
        break
    time.sleep(1)
else:
    raise SystemExit('Payment is still pending; inspect consumer logs')
PY
```

Сумму передавайте десятичной строкой, чтобы JSON-клиент не терял точность больших чисел.
Допустимый диапазон — от `0.01` до `9999999999999999.99`.
Ненулевые цифры после второго десятичного знака отклоняются (`422`), округления нет.
Валюта — только `RUB`, `USD`, `EUR` в верхнем регистре.
Описание — непустая строка до 1000 символов; webhook — HTTP(S) URL.
`metadata` — JSON-объект; при отсутствии используется `{}`.
Все остальные поля обязательны, неизвестные поля отклоняются.

### Повторные запросы

`Idempotency-Key` обязателен: непустая строка до 255 символов, глобальная для сервиса.
При отсутствии или некорректном значении возвращается `422`.

- Новый ключ создаёт ровно один платёж и одно событие `payments.new` в Outbox.
- Тот же ключ с теми же данными возвращает `202`, исходные UUID и `created_at`,
  текущий сохранённый статус. Нового события нет.
- Тот же ключ с другими данными возвращает `409` и JSON с полем `detail`.
- При одновременных одинаковых запросах все получают один и тот же платёж.

Сравнение выполняется после валидации: `1500.0` и `1500.00` эквивалентны,
порядок ключей вложенных JSON-объектов не влияет, пропущенный `metadata` равен `{}`.
URL сравниваются в нормализованном виде Pydantic, который сохраняется в БД
(например, `https://EXAMPLE.com:443` становится `https://example.com/`).
Описание и ключ сохраняются без удаления пробелов по краям; строки только из пробелов запрещены.

## Восстановление и ограничения

При недоступном RabbitMQ POST продолжает сохранять Payment и Outbox в PostgreSQL.
События с `published_at = null` отправляются после восстановления брокера.
Для ручной проверки остановите только RabbitMQ, создайте платёж, затем восстановите брокер:

```sh
docker compose stop rabbitmq
# Выполните POST из примера: платёж останется pending.
docker compose start rabbitmq
docker compose logs -f consumer
```

Worker восстанавливает соединение и продолжает обработку. Неожиданная остановка фонового
отправителя завершает процесс с ошибкой, Compose перезапускает consumer.
При штатной остановке worker отменяет отправитель, даёт обработчику до 10 секунд
на завершение и закрывает соединения. Неподтверждённое сообщение возвращается брокеру.

Публикация имеет гарантию **at least once**: после принятия сообщения брокером и до commit
`published_at` возможна повторная публикация. После commit платежа и до ACK возможна
повторная доставка. В обоих случаях сохранённый результат платежа не пересчитывается.
Записи Outbox после публикации сохраняются.

`failed` — окончательный бизнес-результат, а не техническая ошибка.
Технический сбой приводит к NACK с requeue после паузы 2 секунды, без успешного ACK.
Пока число повторов не ограничено. `prefetch=1` не допускает быстрой параллельной
перечитки ошибочного сообщения. Некорректное сообщение или неизвестный UUID платежа
отклоняются без requeue и **отбрасываются**, поскольку DLQ ещё отсутствует.
Webhook URL сохраняется, но HTTP-запросы к нему пока не выполняются.

## Тесты на отдельных PostgreSQL и RabbitMQ

Основной способ — отдельный Compose-проект, которому не нужен `.env` приложения:

```sh
docker compose -p mkk-payments-test -f compose.test.yaml up --build --abort-on-container-exit --exit-code-from tests
docker compose -p mkk-payments-test -f compose.test.yaml down -v
```

Первую команду используйте для проверки результата: её exit code равен коду тестов.
Вторая команда очищает тестовые контейнеры и сеть.
Тестовая БД находится в `tmpfs` отдельного контейнера, не имеет опубликованного порта
и не использует рабочий volume. Имя БД и пользователь — `payments_test`.
Тестовый RabbitMQ имеет отдельный volume, пользователя и vhost `payments_test`;
`down -v` удаляет только volume этого тестового проекта. В тестовом Compose нет
постоянного consumer: тесты сами запускают и останавливают worker.
Миграции применяются тестами через Alembic. Тесты очищают **только эту тестовую БД**.
Транзакции и блокировки проверяются на PostgreSQL, маршрутизация, confirms и доставка —
на настоящем RabbitMQ. Тесты эмуляции детерминированы и не ждут случайные 2–5 секунд.
Не запускайте несколько наборов тестов одновременно против одной тестовой инфраструктуры.

Для локального запуска нужен Python 3.13 и отдельная PostgreSQL с БД и пользователем
`payments_test`, отдельный RabbitMQ с management plugin, пользователем и vhost
`payments_test`. Не предоставляйте тестовым пользователям доступ к рабочим данным.
Явно укажите тестовые адреса:

```sh
python -m venv .venv
# Bash: source .venv/bin/activate
# PowerShell: .\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
export TEST_DATABASE_URL='postgresql+asyncpg://payments_test:test-only@localhost:5432/payments_test'
export TEST_RABBITMQ_URL='amqp://payments_test:test-only@localhost:5672/payments_test'
export TEST_RABBITMQ_MANAGEMENT_URL='http://localhost:15672'
# PowerShell: $env:TEST_DATABASE_URL = 'postgresql+asyncpg://payments_test:test-only@localhost:5432/payments_test'
python -m pytest -q
python -m ruff check app tests migrations scripts
python -m ruff format --check app tests migrations scripts
```

Защита тестов проверяет имена БД, пользователя и RabbitMQ vhost **до миграций и очистки**.
Management URL должен указывать на тот же тестовый RabbitMQ. Его HTTP API используется
для проверки восстановления после принудительного разрыва AMQP-соединений.
Рабочие `DATABASE_URL` и `API_KEY` не используются для настройки тестового приложения.

Отдельный приёмочный сценарий с реальными задержками и перезапуском RabbitMQ:

```sh
python scripts/smoke_processing.py
```

Нужны только Python 3.13 и запущенный Docker. Скрипт создаёт отдельный Compose-проект
со случайным именем, временный env-файл вне checkout и собственные volume; рабочие
настройки и данные не использует. Проверяет обновление схемы 0001 → 0002 с сохранением
платежа, обработку через API, недоступность/восстановление брокера без перезапуска worker,
сохранение уже опубликованного сообщения после перезапуска RabbitMQ и штатную остановку.
В конце, в том числе при ошибке, удаляет только свои контейнеры и volume.

Файлы `requirements.txt` и `requirements-dev.txt` фиксируют прямые и транзитивные зависимости.
Первый используется в runtime-образе, второй — для разработки и тестов.
Для намеренного обновления: установите `pip-tools==7.6.2` и выполните
`python -m piptools compile --strip-extras --no-header --no-annotate -o requirements.txt requirements.in`,
затем `python -m piptools compile --strip-extras --no-header --no-annotate -c requirements.txt -o requirements-dev.txt requirements-dev.in`.

Краткое описание решений: [docs/architecture.md](docs/architecture.md).
