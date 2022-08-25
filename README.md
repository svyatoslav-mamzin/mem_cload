# memcload
Загрузка логов в memcache

memc_load.py - переписано на многопроцессную версию

Для запуска скрипта с параметрами по умолчанию:
```sh
python memc_load.py
```
Путь до архива с логом указывается через параметр --pattern  
Без параметров скрипт ищет архив с логов в корне проекта в директории /data  
Все возможные параметры можно подсмотреть:
```python
 op = OptionParser()
 op.add_option("-t", "--test", action="store_true", default=False)
 op.add_option("-l", "--log", action="store", default=None)
 op.add_option("--dry", action="store_true", default=False)
 op.add_option("--pattern", action="store", default="data/*.tsv.gz")
 op.add_option("--idfa", action="store", default="127.0.0.1:33013")
 op.add_option("--gaid", action="store", default="127.0.0.1:33014")
 op.add_option("--adid", action="store", default="127.0.0.1:33015")
 op.add_option("--dvid", action="store", default="127.0.0.1:33016")
```
тесты:
```sh
python -m pytest
```

## Обработка одного файла логов
- Однопоточнная версия ~ 5м 12с
- Работа одного воркера ~ 4м 32с
- Работа двух воркеров ~ 2м 39с
- Работа трех воркеров ~ 2м 24с
- Работа четырех воркеров ~ 2м 19с
