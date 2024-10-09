# gar2

Синхронизация адресного справочника ГАР с локальной базой регионов.

## Getting started

Для работы потребуються несколько библиотек Perl.

Установка для pacman:
```shell
pacman -S extra/perl-json-parse
pacman -S extra/perl-data-dump
pacman -S extra/perl-config-simple
pacman -S extra/perl-dbd-pg
```
## Подготовка базы данных

Рекомендуем создать пользователя gar от имени которого будут выполняться все действия.
Так-же справочник ГАР лучше разместить в отдельной схеме в БД, что-бы небыло пересечений пространства имен.

```shell
CREATE ROLE gar LOGIN PASSWORD 'password';
CREATE SCHEMA gar AUTHORIZATION gar;
ALTER ROLE gar SET search_path TO 'gar', 'public';
psql -U gar -f db.sql -h <сервер> <dbname>
```