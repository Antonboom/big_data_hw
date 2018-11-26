### Быстрый старт
1. Подправить env.sh в нужном модуле
2. Запустить **run_on_hdfs.sh** с указанием модуля:
```bash
PACKAGE=task_2 ./run_on_hdfs.sh
```

#### Задача 1. Средний рейтинг
Реализуйте подсчет среднего рейтинга продуктов. Результат сохранить в HDFS в файле "avg_rating.csv".
Формат каждой записи: ProdId,Rating

Исходные данные
- data/reviews_sample_100.json (выборка из 100 отзывов)
- http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Electronics_5.json.gz (полный набор данных)


#### Задача 2. Добавление наименования продукта
Напишите программу, которая каждому ProdId из "avg_rating.csv" ставит в соответстие названием продукта.
Результат сохранить в HDFS в файле "prodname_avg_rating.csv": ProdId,Name,Rating

Исходные данные
- data/sample_100_meta.json (выборка мета-данных продуктов из reviews_sample_100.json)
- avg_rating.csv (данные, полученные из задачи 1 для полного набора данных)
http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Electronics.json.gz (полный набор данных)

#### Задача 3. Поиск среднего рейтинга по названию продукта
Напишите программу, которая выводит средний рейтинги всех продуктов из "prodname_avg_rating.csv",
в названии которых встречается введенное при запуске слово: ProdId,Name,Rating

Исходные данные
- prodname_avg_rating.csv (данные, полученные из задачи 2 для полного набора данных)
