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
├── NbaResultWriter.java     — интерфейс записи результатов
└── impl/
    ├── CsvNbaDataLoader.java    — чтение CSV через Spark
    ├── NbaAnalyzerImpl.java     — расчёт efficiency и CPE
    └── CsvNbaResultWriter.java  — запись CSV
```

## Логика расчёта

### Шаг 1 — обогащение зарплат именем игрока

`players._id = salaries.player_id` — salaries хранит числовой ID, имя берём из справочника players. Также извлекаем год рождения из `birthDate` для последующей дедупликации.

### Шаг 2 — джойн зарплат со статистикой

Три условия:
1. `year` совпадает
2. `player_name = Player` (имя)
3. `birth_year BETWEEN (year - age) AND (year - age + 1)` — защита от тёзок

Третье условие необходимо, потому что в датасете есть игроки с одинаковыми именами (например, Charles Jones встречается 3 раза). Год рождения из `birthDate` + возраст из Stats однозначно идентифицируют конкретного человека. Диапазон ±1 год учитывает то, что возраст фиксируется на 1 мая сезона: игрок с ДР после 1 мая имеет `age` на 1 больше.

### Шаг 3 — топ-N по годам

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
