# Ежедневный парсинг сайта korzinavdom.kz

## Описание проекта

Цель проекта — автоматизация парсинга интернет-магазина [korzinavdom.kz/catalog](https://korzinavdom.kz/catalog), с сохранением данных в ClickHouse и управлением процессом через Apache Airflow. Проект реализован в виде DAG, запускаемого ежедневно.

---

## Используемые технологии

- **Python 3.12**
- **Apache Airflow** — оркестрация DAG
- **Selenium + Firefox Remote** — парсинг динамического контента
- **ClickHouse** — хранилище данных
- **clickhouse-connect** — библиотека вставки данных
- **pandas** — формирование таблиц
- **Docker (Selenium Grid)** — поддержка headless-браузера

---

## Периодичность запуска

- DAG запускается **ежедневно в 00:00 (полночь)**
- Настроен через `schedule_interval='@daily'` в Airflow

---

## Что делает DAG

### Шаг 1: `fetch_data`

- Открывает сайт https://korzinavdom.kz/catalog
- Скроллит до конца страницы, чтобы подгрузить все товары
- Обходит **все карточки товаров**
- Извлекает данные:
  - `product_name`
  - `article` (артикул из карточки)
  - `price` (в тенге)
  - `product_url` (прямая ссылка на карточку)
  - `parsed_at` (время сбора данных)
- Обрабатывает модальное окно 21+ (если открывается)
- Сохраняет результат во временный `.csv` файл `/tmp/tmp_parsed_data.csv`

### Шаг 2: `transform_data`

- Читает файл с результатами
- Очищает цену от символов (`₸`, пробелы), преобразует к `int`
- Подключается к ClickHouse через `BaseHook`
- Создаёт таблицу `products` при необходимости
- Выполняет `INSERT INTO products` для текущей выборки

---

## Структура таблицы в ClickHouse

```sql
CREATE TABLE IF NOT EXISTS products (
    product_name String,
    article      UInt64,
    price        UInt64,
    product_url  String,
    parsed_at    DateTime
)
ENGINE = MergeTree
ORDER BY (article)