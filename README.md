# Сервис платежей

Асинхронный API на Python 3.13, FastAPI, Pydantic v2 и SQLAlchemy 2.0 с PostgreSQL.
Текущая фича: создание и просмотр платежей, транзакционный Outbox и идемпотентность.
Платежи остаются `pending`, `processed_at` равен `null`, события Outbox ещё не публикуются.
RabbitMQ, consumer и доставка webhook в этой версии отсутствуют.

## Запуск через Docker Compose

Требуются Docker с запущенным Linux-движком и Docker Compose v2.

1. Скопируйте `.env.example` в `.env` (`cp .env.example .env` в Bash или
   `Copy-Item .env.example .env` в PowerShell).
2. Замените демонстрационные значения `API_KEY` и `POSTGRES_PASSWORD`.
   В `DATABASE_URL` укажите тот же пароль, пользователя и имя БД.
   Специальные символы пароля в URL нужно percent-encode. Имя хоста внутри Compose — `postgres`.
3. Из корня проекта выполните:

```sh
docker compose build api
docker compose up -d --wait postgres
docker compose run --rm api python -m alembic upgrade head
docker compose up -d api
docker compose logs api
```

Миграции применяются **явно**, перед запуском API. Таблицы через `create_all` не создаются.
Повторная команда `alembic upgrade head` безопасна для уже обновлённой схемы.
API доступен по `http://localhost:8000`; порт можно изменить через `API_PORT`.
PostgreSQL не публикует порт на хост и хранит данные в volume `postgres_data`.
Для остановки используйте `docker compose down` — volume при этом сохраняется.

Обязательные настройки приложения: `DATABASE_URL` (схема `postgresql+asyncpg`) и непустой
`API_KEY`. При их отсутствии или некорректном значении приложение завершится с ошибкой,
указывающей имена настроек без их значений. Compose также проверяет наличие параметров БД.

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
некорректный UUID — `422`.

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

## Тесты на отдельной PostgreSQL

Основной способ — отдельный Compose-проект, которому не нужен `.env` приложения:

```sh
docker compose -p mkk-payments-test -f compose.test.yaml up --build --abort-on-container-exit --exit-code-from tests
docker compose -p mkk-payments-test -f compose.test.yaml down
```

Первую команду используйте для проверки результата: её exit code равен коду тестов.
Вторая команда очищает тестовые контейнеры и сеть.
Тестовая БД находится в `tmpfs` отдельного контейнера, не имеет опубликованного порта
и не использует рабочий volume. Имя БД и пользователь — `payments_test`.
Миграции применяются тестами через Alembic. Тесты очищают **только эту тестовую БД**.
Транзакции, ограничения и конкурентные запросы проверяются на PostgreSQL, SQLite не используется.
Не запускайте несколько наборов тестов одновременно против одной тестовой БД.

Для локального запуска нужен Python 3.13 и отдельная PostgreSQL с БД и пользователем
`payments_test`. Не предоставляйте этому пользователю доступ к рабочей БД.
В `TEST_DATABASE_URL` явно укажите тестовый адрес:

```sh
python -m venv .venv
# Bash: source .venv/bin/activate
# PowerShell: .\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
export TEST_DATABASE_URL='postgresql+asyncpg://payments_test:test-only@localhost:5432/payments_test'
# PowerShell: $env:TEST_DATABASE_URL = 'postgresql+asyncpg://payments_test:test-only@localhost:5432/payments_test'
python -m pytest -q
python -m ruff check app tests migrations
python -m ruff format --check app tests migrations
```

Защита тестов отклоняет URL с другим именем БД или пользователем **до миграций и очистки**.
Рабочие `DATABASE_URL` и `API_KEY` не используются для настройки тестового приложения.

Файлы `requirements.txt` и `requirements-dev.txt` фиксируют прямые и транзитивные зависимости.
Первый используется в runtime-образе, второй — для разработки и тестов.
Для намеренного обновления: установите `pip-tools==7.6.2` и выполните
`python -m piptools compile --strip-extras --no-header --no-annotate -o requirements.txt requirements.in`,
затем `python -m piptools compile --strip-extras --no-header --no-annotate -c requirements.txt -o requirements-dev.txt requirements-dev.in`.

Краткое описание решений: [docs/architecture.md](docs/architecture.md).
