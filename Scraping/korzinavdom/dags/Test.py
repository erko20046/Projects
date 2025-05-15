from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
import clickhouse_connect
from airflow.hooks.base import BaseHook
import time

from selenium.webdriver import Remote
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

# Настройки 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'korzinavdom_selenium_parsing_test',
    default_args=default_args,
    description='Парсинг товаров с korzinavdom.kz с использованием Selenium',
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 15),
    catchup=False
)


# Функции 

def fetch_data():
    log = logging.getLogger("airflow.task")

    options = Options()
    driver = Remote(
        command_executor='http://selenium:4444/wd/hub',
        options=options
    )

    log.info("Открытие страницы...")
    driver.get("https://korzinavdom.kz/catalog")
    driver.set_window_size(1920, 1080)


    wait = WebDriverWait(driver, 10)
    data = []
    product_index = 0
    max_items = 8

    while product_index < max_items:
        try:
            cards_xpath = (
                "//div[@class='search-product__items ng-star-inserted']"
                "/div[@class='card ng-star-inserted']"
            )
            wait.until(EC.presence_of_all_elements_located((By.XPATH, cards_xpath)))
            product_elements = driver.find_elements(By.XPATH, cards_xpath)

            if product_index >= len(product_elements):
                break

            try:
                price_xpath = f"(//p[@class='card__cost-sale'])[{product_index + 1}]"
                price = driver.find_element(By.XPATH, price_xpath).text.strip()
            except NoSuchElementException:
                price = 'N/A'

            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", product_elements[product_index])
            wait.until(EC.element_to_be_clickable(product_elements[product_index]))
            product_elements[product_index].click()

            try:
                warning = driver.find_element(
                    By.XPATH,
                    "//h2[@class='confirm-old__headline']"
                ).text.strip()
            except NoSuchElementException:
                warning = None

            if warning == 'Данная категория товара предназначена для лиц, достигших 21 года.':
                try:
                    driver.find_element(
                        By.XPATH,
                        "//button[contains(@class,'modal-card__close-btn')]"
                    ).click()
                except Exception:
                    pass
                name = "Alcohol Product"
                article = 0
            else:
                try:
                    name = driver.find_element(
                        By.XPATH,
                        "//p[@class='description-block__name']"
                    ).text.strip()
                except NoSuchElementException:
                    name = 'N/A'

                try:
                    art_text = driver.find_element(
                        By.XPATH,
                        "//p[@class='tab-content__article-item']"
                    ).text
                    raw_article = art_text.split(":", 1)[1].strip()
                    article = str(int(''.join(filter(str.isdigit, raw_article)))).zfill(8)
                except Exception:
                    article = 0

                data.append({
                'product_name': name,
                'article': article,
                'price': price,
                'product_url': driver.current_url,
                'parsed_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

            driver.back()
            wait.until(EC.presence_of_all_elements_located((By.XPATH, cards_xpath)))


            product_index += 1

        except Exception as e:
            log.error(f"Ошибка при парсинге товара {product_index}: {e}")
            product_index += 1
    
    driver.quit()

    df = pd.DataFrame(data)
    df.to_csv("/tmp/tmp_parsed_data.csv", index=False, encoding='utf-8')
    log.info(f"Сохранено {len(df)} записей во временный CSV")


def transform_data():
    log = logging.getLogger("airflow.task")
    try:
        df = pd.read_csv("/tmp/tmp_parsed_data.csv")

        df['parsed_at'] = pd.to_datetime(df['parsed_at'])
        df['price'] = df['price'].str.replace('₸', '', regex=False).str.replace(' ', '', regex=False)
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0).astype(int)

        conn = BaseHook.get_connection("clickhouse_default")

        client = clickhouse_connect.get_client(
            host=conn.host,
            port=conn.port,
            username=conn.login,
            password=conn.password
        )

        client.command('''
                        CREATE TABLE IF NOT EXISTS products
                                            (
                                                product_name String,
                                                article      UInt64,
                                                price        UInt64,
                                                product_url  String,
                                                parsed_at    DateTime
                                            )
                                            ENGINE = MergeTree
                                            ORDER BY (article);
                    ''')

        client.insert_df('products', df)
        log.info("Data Inserted to Clickhouse")
    except Exception as e:
        log.error(f"Ошибка в transform_data: {e}")
        raise


# ---------- Операторы ----------

fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

# ---------- Зависимости ----------
fetch_task >> transform_task
