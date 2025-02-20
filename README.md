# gar2

Синхронизация адресного справочника ГАР с локальной базой регионов.

## Getting started

Для работы потребуються несколько библиотек Perl. Работа с исходными фалами выгрузки происходит исключительно через PIPE без распаковки архива на диск или создания временных файлов. Существенно экономится место и повышаеться скорость работы.

Установка для pacman:
```shell
pacman -S extra/perl-json-parse
pacman -S extra/perl-data-dump
pacman -S extra/perl-config-simple
pacman -S extra/perl-dbd-pg
install from AUR https://aur.archlinux.org/packages/perl-dbix-runsql
```
## Подготовка базы данных

Рекомендуем создать пользователя Postgres gar от имени которого будут выполняться все действия.
Так-же справочник ГАР лучше разместить в отдельной схеме в БД, что-бы небыло пересечений пространства имен таблиц.

Отредактируйте файл с конфигурацией gar.cfg

Создайте пользователя и схему. Имя схемы может быть любым. При работе с БД все действия выполняються в CURRENT_SCHEMA().
```shell
CREATE ROLE gar LOGIN PASSWORD 'password';
CREATE SCHEMA gar AUTHORIZATION gar;
ALTER ROLE gar SET search_path TO 'gar';
```

После этого можно запустить "gar.pl --init=<регион>". При первом старте он скачает список всех обновлений и закачает последнее полное обновление. Так-же создаст структуру ГАР в базе данных на основании парсинга полной выгрузки указанного региона. Запускать 1 раз или при изменении формата ГАР справочника. Эта операция пересоздает все таблицы.

gar.pl --add-region=<регион> - загружает новый регион в БД из предварительно скаченных файлов.
gar.pl --del-region=<регион> - удаляет регион из БД.
gar.pl --update - обновляет БД
gar.pl --show-status - показывает текущий статус выгрузки

необязательные параметры:
--config=<file> - откуда читать конфигурацию

## Немного о самом справочнике ГАР

В справочнике описаны 5 типа объектов - Здания (houses), Помещения (apartments), Комнаты (rooms), Земельные участки (steads), Парковочные места (carplaces) и Адреса (addr_obj). Каждый из указанных объектов встречаеться на определенном уровне (object_levels).
Все объекты ведуться в едином реестре объектов (reestr_objects).
В зависимости от levelid информацию о объекте надо искать в разных таблицах:

1-8 Адреса (addr_obj)
    1 - Субъект РФ
    2 - Административный район
    3 - Муниципальный район
    4 - Сельское/городское поселение
    5 - Город
    6 - Населенный пункт
    7 - Элемент планировочной структуры
    8 - Элемент улично-дорожной сети
9 Земельные участки (steads)
10 Здания (houses)
11 Помещения (apartments)
12 Комнаты (rooms)
17 Парковочные места (carplaces)
Есть еще устаревшие уровни 13-16, но в адресах их практически нет...
У каждого из этих 5-ти объектов есть таблица в которой храняться ключ(code)=значение(name) с параметрами, которые относяться к данному типу объектов. Таблицы называються *_params. Есть общий справочник параметров param_types, где перечисленны все возможные ключи.

Прямых связей в этих таблицах нет. Тоесть в houses нет ссылки на addr_obj.

Иерархия связей строиться через таблицу adm_hierarchy (деление территорий по административному принципу). Есть еще таблица mun_hierarchy (муниципальное деление) но мы ее не грузим в БД, так как не имеет большой ценности.

Соответственно мы можем выбрать все объекты имеющие какое-то отношение к адресной иерархии.

Получить все объекты в г.Таганроге:

WITH RECURSIVE res AS ( SELECT objectid from adm_hierarchy where parentobjid in (select objectid from addr_obj where name = 'Таганрог') and isactive UNION SELECT adm_hierarchy.objectid from adm_hierarchy JOIN res ON (adm_hierarchy.parentobjid = res.objectid ) ) SELECT objectid,adm_fulladdress(objectid) from res;

Получить все многоквартирные дома в г. Таганроге:

select objectid,adm_fulladdress(objectid) from houses join houses_params using (objectid) where houses_params.typeid = 19 and houses.objectid in (WITH RECURSIVE res AS ( SELECT objectid from adm_hierarchy where parentobjid in (select objectid from addr_obj where name = 'Таганрог') and isactive UNION SELECT adm_hierarchy.objectid from adm_hierarchy JOIN res ON (adm_hierarchy.parentobjid = res.objectid ) ) SELECT * from res);
