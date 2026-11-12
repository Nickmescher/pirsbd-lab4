# NBA Analytics — pirsbd-lab4

Apache Spark приложение для анализа эффективности игроков NBA относительно их зарплат (1984–2018).

## Задача

Для каждого сезона найти топ-5 самых выгодных игроков — тех, у кого наименьшая стоимость за единицу эффективности:

```
efficiency          = PTS + TRB + AST          (сезонные суммы)
cost_per_efficiency = salary / efficiency
```

## Датасеты

| Файл | Описание | Ключевые колонки |
|------|----------|-----------------|
| `data/cleaned/players.csv` | Справочник игроков | `_id`, `name`, `birthDate` |
| `data/cleaned/salaries_1985to2018.csv` | Зарплаты по сезонам | `player_id`, `season_start`, `salary`, `team` |
| `data/cleaned/Seasons_Stats.csv` | Статистика по сезонам | `Year`, `Player`, `Age`, `Pos`, `PTS`, `TRB`, `AST` |

Данные в `data/cleaned/` — очищенная версия исходных датасетов.

## Архитектура

```
src/main/java/org/example/
├── NbaApp.java              — точка входа
├── NbaDataLoader.java       — интерфейс загрузки CSV
├── NbaAnalyzer.java         — интерфейс аналитики
├── NbaDataCleaner.java      — интерфейс очистки данных
├── NbaResultWriter.java     — интерфейс записи результатов
└── impl/
    ├── CsvNbaDataLoader.java    — чтение CSV через Spark, применяет NbaDataCleaner
    ├── NbaDataCleanerImpl.java  — фильтрация невалидных строк, нормализация типов
    ├── NbaAnalyzerImpl.java     — расчёт efficiency и CPE, дедупликация тёзок
    └── CsvNbaResultWriter.java  — запись CSV
```

## Логика расчёта

### Шаг 1 — очистка данных (`NbaDataCleanerImpl`)

Перед анализом каждый датасет очищается:
- **players**: отбрасываются строки без `_id`, нормализуются имена (убираются `*` и лишние пробелы)
- **salaries**: фильтруются нулевые/отрицательные зарплаты, строки без `player_id` и `season_end`, только лига NBA
- **stats**: фильтруются строки с null/отрицательными PTS/TRB/AST, дедупликация по `(Year, Player, Tm)`

### Шаг 2 — сопоставление игроков по имени и дате рождения

В датасете есть тёзки (например, Charles Jones — 3 разных игрока). Простого джойна по имени недостаточно.

Решение: для каждой пары `(player, year, age)` из Stats вычисляется опорная дата `1 мая (year - age)`. Затем из players выбирается кандидат с `birthDate`, ближайшей к этой дате. Window-функция `row_number()` ранжирует кандидатов по `abs(datediff(birth_date, reference_birth_date))`, берётся rank = 1.

### Шаг 3 — джойн зарплат со статистикой

После сопоставления Stats → `player_id` джойн со stats идёт по `(player_id, year)`. Зарплаты привязаны к `season_end`, stats — к `Year`, они совпадают.

### Шаг 4 — топ-N по годам

Window-функция `rank()` с партиционированием по `year` и сортировкой по `cost_per_efficiency` по возрастанию.

## Результат

- Parquet с партиционированием по `year`: `output/cost_per_efficiency/`
- CSV с топ-5 игроков по каждому году: `output/top_cheapest_by_year/`

Колонки результата: `year`, `player_name`, `pos`, `team`, `salary`, `PTS`, `TRB`, `AST`, `efficiency`, `cost_per_efficiency`, `rank`

## Запуск

### Требования

- Docker и Docker Compose
- Maven (для сборки jar)
- Java 17

### Сборка

```bash
mvn package
```

Создаёт `target/pirsbd-lab4.jar`.

### Запуск через Docker Compose

```bash
docker compose up --abort-on-container-exit
```

Поднимает три контейнера:
- `spark-master` — Spark Master (UI: http://localhost:8090)
- `spark-worker` — один воркер (UI: http://localhost:8091)
- `spark-submit` — отправляет задачу и завершается

Результаты появляются в папке `output/` на хосте.

### Параметры запуска (spark-submit)

```
NbaApp <playersPath> <salariesPath> <statsPath> <outputDir> [topN]
```

По умолчанию `topN = 5`.

## Технологии

- Apache Spark 3.5.9
- Scala 2.12
- Java 17
- Maven (shade-plugin для fat jar)
- Docker / Docker Compose (`apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu`)
