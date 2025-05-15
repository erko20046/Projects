# Ежедневный парсинг сайта korzinavdom.kz с загрузкой в ClickHouse через Apache Airflow

## Описание проекта  
Цель проекта — автоматизировать парсинг интернет-магазина [korzinavdom.kz/catalog](https://korzinavdom.kz/catalog), сохраняя данные в ClickHouse и управляя процессом через Apache Airflow. Проект реализован как DAG и запускается ежедневно.

---

## Используемые технологии

- **Python 3.12**  
- **Apache Airflow** — управление DAG  
- **Selenium + Firefox Remote** — парсинг динамического контента  
- **ClickHouse** — база данных  
- **clickhouse-connect** — вставка данных  
- **Pandas** — формирование таблиц  
- **Docker + Selenium Grid** — headless браузер в контейнере  

---

## Периодичность запуска

- DAG запускается **ежедневно в полночь**
- Настроено через `schedule_interval='@daily'` в Airflow

---

## Что делает DAG

### Шаг 1: `fetch_data`
- Открывает сайт [korzinavdom.kz/catalog](https://korzinavdom.kz/catalog)
- Скроллит страницу до конца, чтобы подгрузить **все товары**
- Обходит **все карточки товаров** и извлекает:
  - `product_name` — название товара  
  - `article` — артикул (внутри карточки)  
  - `price` — цена в тенге  
  - `product_url` — прямая ссылка на товар  
  - `parsed_at` — текущая дата/время сбора  
- Обрабатывает модальное окно 21+, если появляется
- Сохраняет данные во временный CSV:  
  `/tmp/tmp_parsed_data.csv`

### Шаг 2: `transform_data`
- Загружает данные из CSV
- Очищает цену от символов `₸` и пробелов
- Преобразует цену в целое число (`int`)
- Подключается к ClickHouse через `BaseHook`
- Создаёт таблицу `products`, если она ещё не создана
- Выполняет `INSERT INTO` с текущими строками

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