# Проект: анализ регионов для расширения банка

Цель: собрать данные Банка России по ставкам, объемам, задолженности, просрочке и количеству кредитов в региональном разрезе, сформировать единый DataFrame и при необходимости загрузить его в Postgres для последующей аналитики.

**Что внутри**
- `cbr_extractor.py` — парсер API ЦБ РФ, выгрузка CSV и нормализация.
- `main.py` — единая точка входа: запускает выгрузку и (опционально) грузит в БД.
- `connector.py` — подключение к Postgres (не трогать, используется как есть).
- `data/` — локальные данные (игнорируются гитом).

**Зависимости**
- Python 3.10+
- `pandas`
- `requests`
- `psycopg2` (если нужна загрузка в Postgres)

Установка:
```bash
pip install pandas requests psycopg2-binary
```

**Как работать с данными**

1) Сначала выгружаем из API ЦБ:
```bash
python cbr_extractor.py extract
```

2) Затем нормализуем все CSV в одну таблицу:
```bash
python cbr_extractor.py normalize
```

Результат появится в:
```
data/cbr/products/normalized_for_db.csv
```

**Единая точка входа**

Запуск выгрузки и нормализации:
```bash
python main.py --out-dir data/cbr
```

Запуск без обращения к API (использовать уже готовые CSV в `data/cbr/products/`):
```bash
python main.py --out-dir data/cbr --skip-extract
```

Запуск с загрузкой в Postgres (таблицы должны быть созданы заранее через SQL-скрипты):
```bash
python main.py --out-dir data/cbr --load-db
```

При загрузке в БД создаются отдельные таблицы по каждому `product_key`:
```
<table_prefix><product_key><team_suffix>
```
Например: `hr_final_projects.team_6_mortgage`, `hr_final_projects.team_6_retail_loans_volume`.

Для основных таблиц загрузка идет по годам в партиции:
```
hr_final_projects.team_6_<product_key>_<год>
```
Например: `hr_final_projects.team_6_mortgage_2022`.

По умолчанию `main.py` грузит также справочные таблицы:
- `hr_final_projects.team_6_publications`
- `hr_final_projects.team_6_datasets_catalog`
- `hr_final_projects.team_6_datasets_bank_shortlist`
- `hr_final_projects.team_6_measures`
- `hr_final_projects.team_6_years`

Если их грузить не нужно:
```bash
python main.py --out-dir data/cbr --load-db --skip-spare
```

**Как устроены данные**

Основная таблица (`normalized_for_db.csv`) содержит:
- `product_key` — код продукта/метрики (ставки, объемы, задолженность и т.д.)
- `product_description` — человекочитаемое описание метрики
- `obs_date` — дата наблюдения (месячная периодичность)
- `periodicity` — периодичность (`month`)
- `value` — значение метрики
- `unit_name` — единицы измерения (`% годовых`, `млн руб.`, `единиц`, `месяцев`)
- `measure_id` — идентификатор региона/ФО
- `measure_name` — название региона/ФО
- `element_id` — идентификатор сегмента внутри метрики
- `element_name` — сегмент (например, срок или валюта)
- `unit_id` — идентификатор единицы измерения
- `colId`, `rowId` — технические идентификаторы из API ЦБ
- `dt` — текстовая дата из API (например, `Январь 2019`)
- `digits` — число знаков после запятой в исходной публикации

Ключ для аналитики:
```
product_key + measure_name + element_name + obs_date
```

**CSV в `data/cbr/products/`**

После `extract` в папке `data/cbr/products/` появляются CSV по каждому `product_key`.
Это сырые выгрузки по конкретной метрике/продукту (ставки, объемы, задолженность и т.д.).

Примеры файлов:
- `mortgage.csv` — ставки по ипотеке
- `retail_loans.csv` — ставки по кредитам физлицам
- `corp_sme_loans.csv` — ставки по кредитам юрлицам и ИП (МСП)
- `deposits.csv` — ставки по вкладам физлиц
- `mortgage_count.csv` — количество ипотечных кредитов
- `mortgage_volume.csv` — объем ипотечных кредитов (млн руб.)
- `mortgage_debt.csv` — задолженность по ипотеке (млн руб.)
- `mortgage_overdue.csv` — просроченная задолженность по ипотеке (млн руб.)
- `mortgage_term.csv` — средневзвешенный срок по ипотеке (месяцы)
- `retail_loans_volume.csv` — объем кредитов физлицам (млн руб.)
- `retail_loans_debt.csv` — задолженность по кредитам физлицам (млн руб.)
- `retail_loans_overdue.csv` — просроченная задолженность по кредитам физлицам (млн руб.)
- `corp_loans_volume.csv` — объем кредитов юрлицам и ИП (млн руб.)
- `corp_loans_debt.csv` — задолженность по кредитам юрлицам и ИП (млн руб.)
- `corp_loans_overdue.csv` — просроченная задолженность по кредитам юрлицам и ИП (млн руб.)
- `sme_loans_volume.csv` — объем кредитов МСП (млн руб.)
- `sme_loans_debt.csv` — задолженность по кредитам МСП (млн руб.)
- `sme_loans_overdue.csv` — просроченная задолженность по кредитам МСП (млн руб.)

Далее команда `normalize` объединяет все эти CSV в один файл `normalized_for_db.csv`.

**Настройка БД**

Данные для подключения указываются в `connector.py` в словаре `pg_creds`.

**Примечания**
- Папка `data/` не коммитится.
- Для новых метрик достаточно добавить их в `PRODUCT_CONFIG` в `cbr_extractor.py`.
