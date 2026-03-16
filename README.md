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

Запуск с загрузкой в Postgres (таблица должна быть создана заранее):
```bash
python main.py --out-dir data/cbr --load-db --table cbr_data
```

**Как устроены данные**

Основная таблица (`normalized_for_db.csv`) содержит:
- `product_key` — продукт/метрика (ставки, объемы, задолженность и т.д.)
- `measure_name` — регион/ФО
- `element_name` — сегмент (например, срок или валюта)
- `obs_date` — дата наблюдения
- `value` и `unit_name` — значение и единицы

Ключ для аналитики:
```
product_key + measure_name + element_name + obs_date
```

**Настройка БД**

Данные для подключения указываются в `connector.py` в словаре `pg_creds`.

**Примечания**
- Папка `data/` не коммитится.
- Для новых метрик достаточно добавить их в `PRODUCT_CONFIG` в `cbr_extractor.py`.
