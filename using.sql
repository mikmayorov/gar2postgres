-- Все объекты в БД я стараюсь создавать с комментариями.
-- Смотрите их в первую очередь.

BEGIN;

DROP TABLE IF EXISTS gar_reestr_objects_catalog CASCADE;
CREATE TABLE IF NOT EXISTS gar_reestr_objects_catalog
(
    objectid bigint NOT NULL,
    name text NOT NULL,
    is_active boolean default true NOT NULL
);

COMMENT ON TABLE  gar_reestr_objects_catalog                IS 'из этих объектов ГАР будут строиться VIEW для справочников';
COMMENT ON COLUMN gar_reestr_objects_catalog.objectid       IS 'уникальный идентификатор из gar.reestr_objects любого уровня';
COMMENT ON COLUMN gar_reestr_objects_catalog.name           IS 'название объекта';
COMMENT ON COLUMN gar_reestr_objects_catalog.is_active      IS 'признак актуальности для нас';

INSERT INTO gar_reestr_objects_catalog VALUES (1086102,'Ростовская область, г. Азов');
INSERT INTO gar_reestr_objects_catalog VALUES (1086255,'Ростовская область, г. Батайск');
INSERT INTO gar_reestr_objects_catalog VALUES (1130082,'Ростовская область, г. Волгодонск');
INSERT INTO gar_reestr_objects_catalog VALUES (1086220,'Ростовская область, г. Гуково');
INSERT INTO gar_reestr_objects_catalog VALUES (1077633,'Ростовская область, г. Донецк');
INSERT INTO gar_reestr_objects_catalog VALUES (1078431,'Ростовская область, г. Зверево');
INSERT INTO gar_reestr_objects_catalog VALUES (1078729,'Ростовская область, г. Новочеркасск');
INSERT INTO gar_reestr_objects_catalog VALUES (1085031,'Ростовская область, г. Новошахтинск');
INSERT INTO gar_reestr_objects_catalog VALUES (1086332,'Ростовская область, г. Таганрог');
INSERT INTO gar_reestr_objects_catalog VALUES (1082239,'Ростовская область, г. Ростов-на-Дону');
INSERT INTO gar_reestr_objects_catalog VALUES (1100679,'Ростовская область, г. Шахты');
INSERT INTO gar_reestr_objects_catalog VALUES (1086719,'Ростовская область, Азовский район');
INSERT INTO gar_reestr_objects_catalog VALUES (1086917,'Ростовская область, Аксайский район');
INSERT INTO gar_reestr_objects_catalog VALUES (1100327,'Ростовская область, Матвеево-Курганский район');
INSERT INTO gar_reestr_objects_catalog VALUES (1099285,'Ростовская область, Миллеровский район');
INSERT INTO gar_reestr_objects_catalog VALUES (1106606,'Ростовская область, Мясниковский район');
INSERT INTO gar_reestr_objects_catalog VALUES (1101381,'Ростовская область, Неклиновский район');
INSERT INTO gar_reestr_objects_catalog VALUES (1108282,'Ростовская область, Октябрьский район');
INSERT INTO gar_reestr_objects_catalog VALUES (1113376,'Ростовская область, Песчанокопский район');
INSERT INTO gar_reestr_objects_catalog VALUES (1104933,'Ростовская область, Сальский район');
INSERT INTO gar_reestr_objects_catalog VALUES (1113255,'Ростовская область, Целинский район');
INSERT INTO gar_reestr_objects_catalog VALUES (1094958,'Ростовская область, Зимовниковский район');
INSERT INTO gar_reestr_objects_catalog VALUES (1097393,'Ростовская область, Каменский район');
INSERT INTO gar_reestr_objects_catalog VALUES (1095654,'Ростовская область, Дубовский район');
INSERT INTO gar_reestr_objects_catalog VALUES (471704,'Белгородская область, Вейделевский район');
INSERT INTO gar_reestr_objects_catalog VALUES (475398,'Белгородская область, Ровеньский район');
INSERT INTO gar_reestr_objects_catalog VALUES (483685,'Белгородская область, Белгородский район');
INSERT INTO gar_reestr_objects_catalog VALUES (473298,'Белгородская область, Корочанский район');
INSERT INTO gar_reestr_objects_catalog VALUES (320106,'Краснодарский край, Ейский район');
INSERT INTO gar_reestr_objects_catalog VALUES (327207,'Краснодарский край, Крыловский район');
INSERT INTO gar_reestr_objects_catalog VALUES (330990,'Краснодарский край, Кущевский район');
INSERT INTO gar_reestr_objects_catalog VALUES (329528,'Краснодарский край, Ленинградский район');
INSERT INTO gar_reestr_objects_catalog VALUES (335141,'Краснодарский край, Павловский район');
INSERT INTO gar_reestr_objects_catalog VALUES (336438,'Краснодарский край, Староминский район');

CREATE UNIQUE INDEX ON gar_reestr_objects_catalog (objectid);

-- общий реестр объектов
DROP MATERIALIZED VIEW IF EXISTS gar_reestr_objects CASCADE;
CREATE MATERIALIZED VIEW gar_reestr_objects AS 
    -- выбираються все объекты где gar_reestr_objects_catalog являеться родителем
    SELECT * FROM (
        WITH RECURSIVE res AS (
            SELECT  gar.reestr_objects.objectid,
                    gar.reestr_objects.levelid,
                    gar.reestr_objects.objectguid,
                    (CASE WHEN gar.reestr_objects.levelid between 1 and 8 THEN 'gar_addresses'
                          WHEN gar.reestr_objects.levelid = 9 THEN 'gar_steads'
                          WHEN gar.reestr_objects.levelid = 10 THEN 'gar_houses'
                          WHEN gar.reestr_objects.levelid = 11 THEN 'gar_apartments'
                          WHEN gar.reestr_objects.levelid = 12 THEN 'gar_rooms'
                          WHEN gar.reestr_objects.levelid = 17 THEN 'gar_carplaces'
                          ELSE NULL END)::text as object_table,
                    gar.adm_hierarchy.parentobjid as parentobjectid_adm,
                    gar.adm_address(gar.reestr_objects.objectid) as full_address
                FROM gar_reestr_objects_catalog
                JOIN gar.reestr_objects USING (objectid)
                JOIN gar.adm_hierarchy USING (objectid)
                WHERE gar.reestr_objects.isactive and gar_reestr_objects_catalog.is_active and gar.adm_hierarchy.isactive
        UNION
            SELECT  gar.reestr_objects.objectid,
                    gar.reestr_objects.levelid,
                    gar.reestr_objects.objectguid,
                    (CASE WHEN gar.reestr_objects.levelid between 1 and 8 THEN 'gar_addresses'
                          WHEN gar.reestr_objects.levelid = 9 THEN 'gar_steads'
                          WHEN gar.reestr_objects.levelid = 10 THEN 'gar_houses'
                          WHEN gar.reestr_objects.levelid = 11 THEN 'gar_apartments'
                          WHEN gar.reestr_objects.levelid = 12 THEN 'gar_rooms'
                          WHEN gar.reestr_objects.levelid = 17 THEN 'gar_carplaces'
                          ELSE NULL END)::text as object_table,
                    gar.adm_hierarchy.parentobjid as parentobjectid_adm,
                    gar.adm_address(gar.reestr_objects.objectid) as full_address
                FROM res
                JOIN gar.adm_hierarchy ON (gar.adm_hierarchy.parentobjid = res.objectid)
                JOIN gar.reestr_objects ON (gar.reestr_objects.objectid = gar.adm_hierarchy.objectid)
                WHERE gar.reestr_objects.isactive and gar.adm_hierarchy.isactive
        ) SELECT objectid,objectguid,levelid,object_table,parentobjectid_adm,full_address from res ) as t1
    UNION
    -- выбираються все объекты где gar_reestr_objects_catalog являеться потомком
    SELECT * FROM (
        WITH RECURSIVE res AS (
            SELECT  gar.reestr_objects.objectid,
                    gar.reestr_objects.levelid,
                    gar.reestr_objects.objectguid,
                    (CASE WHEN gar.reestr_objects.levelid between 1 and 8 THEN 'gar_addresses'
                          WHEN gar.reestr_objects.levelid = 9 THEN 'gar_steads'
                          WHEN gar.reestr_objects.levelid = 10 THEN 'gar_houses'
                          WHEN gar.reestr_objects.levelid = 11 THEN 'gar_apartments'
                          WHEN gar.reestr_objects.levelid = 12 THEN 'gar_rooms'
                          WHEN gar.reestr_objects.levelid = 17 THEN 'gar_carplaces'
                          ELSE NULL END)::text as object_table,
                    gar.adm_hierarchy.parentobjid as parentobjectid_adm,
                    gar.adm_address(gar.reestr_objects.objectid) as full_address
                FROM gar_reestr_objects_catalog
                JOIN gar.reestr_objects USING (objectid)
                JOIN gar.adm_hierarchy USING (objectid)
                WHERE gar.reestr_objects.isactive and gar_reestr_objects_catalog.is_active and gar.adm_hierarchy.isactive
        UNION
            SELECT  gar.reestr_objects.objectid,
                    gar.reestr_objects.levelid,
                    gar.reestr_objects.objectguid,
                    (CASE WHEN gar.reestr_objects.levelid between 1 and 8 THEN 'gar_addresses'
                          WHEN gar.reestr_objects.levelid = 9 THEN 'gar_steads'
                          WHEN gar.reestr_objects.levelid = 10 THEN 'gar_houses'
                          WHEN gar.reestr_objects.levelid = 11 THEN 'gar_apartments'
                          WHEN gar.reestr_objects.levelid = 12 THEN 'gar_rooms'
                          WHEN gar.reestr_objects.levelid = 17 THEN 'gar_carplaces'
                          ELSE NULL END)::text as object_table,
                    gar.adm_hierarchy.parentobjid as parentobjectid_adm,
                    gar.adm_address(gar.reestr_objects.objectid) as full_address
                FROM res
                JOIN gar.adm_hierarchy ON (gar.adm_hierarchy.objectid = res.parentobjectid_adm)
                JOIN gar.reestr_objects ON (gar.reestr_objects.objectid = gar.adm_hierarchy.objectid)
                WHERE gar.reestr_objects.isactive and gar.adm_hierarchy.isactive
        ) SELECT objectid,objectguid,levelid,object_table,parentobjectid_adm,full_address
            FROM res
            WHERE objectid NOT IN (SELECT objectid
                                        FROM gar_reestr_objects_catalog)
    ) as t2;

CREATE UNIQUE INDEX ON gar_reestr_objects (objectid);
CREATE UNIQUE INDEX ON gar_reestr_objects (objectguid);
CREATE INDEX ON gar_reestr_objects (object_table);
CREATE INDEX ON gar_reestr_objects (parentobjectid_adm);

-- адреса
DROP MATERIALIZED VIEW IF EXISTS gar_addresses CASCADE;
CREATE MATERIALIZED VIEW gar_addresses AS
    SELECT gar_reestr_objects.*,
           gar.addr_obj.name
        FROM gar_reestr_objects
            JOIN gar.addr_obj USING (objectid)
        WHERE gar_reestr_objects.object_table = 'gar_addresses' and
              gar.addr_obj.isactive;

CREATE UNIQUE INDEX ON gar_addresses (objectid);
CREATE INDEX ON gar_addresses (parentobjectid_adm);

-- дома
DROP MATERIALIZED VIEW IF EXISTS gar_houses CASCADE;
CREATE MATERIALIZED VIEW gar_houses AS
    SELECT gar_reestr_objects.*,
           gar.houses.housenum,
           gar.houses.addnum1,
           gar.houses.addnum2,
           (SELECT value FROM gar.houses_params where gar.houses_params.objectid = gar_reestr_objects.objectid and gar.houses_params.typeid = 19 and enddate > now() limit 1) as apartmentbuilding,
           (SELECT value FROM gar.houses_params where gar.houses_params.objectid = gar_reestr_objects.objectid and gar.houses_params.typeid = 8 and enddate > now() limit 1) as cadastrnum,
           (SELECT value FROM gar.houses_params where gar.houses_params.objectid = gar_reestr_objects.objectid and gar.houses_params.typeid = 5 and enddate > now() limit 1) as postindex,
           (SELECT lower(name) FROM gar.house_types where gar.house_types.id = gar.houses.housetype) as housetype
        FROM gar_reestr_objects
            JOIN gar.houses USING (objectid)
        WHERE gar_reestr_objects.object_table = 'gar_houses' and
              gar.houses.isactive;

CREATE UNIQUE INDEX ON gar_houses (objectid);
CREATE INDEX ON gar_houses (parentobjectid_adm);

-- земельный участок
DROP MATERIALIZED VIEW IF EXISTS gar_steads CASCADE;
CREATE MATERIALIZED VIEW gar_steads AS
    SELECT gar_reestr_objects.*,
           gar.steads.number
        FROM gar_reestr_objects
        JOIN gar.steads USING (objectid)
        WHERE gar_reestr_objects.object_table = 'gar_steads' and gar.steads.isactive;

CREATE UNIQUE INDEX ON gar_steads (objectid);
CREATE INDEX ON gar_steads (parentobjectid_adm);

-- помещения
DROP MATERIALIZED VIEW IF EXISTS gar_apartments CASCADE;
CREATE MATERIALIZED VIEW gar_apartments AS
    SELECT gar_reestr_objects.*,
           gar.apartments.number
        FROM gar_reestr_objects
        JOIN gar.apartments USING (objectid)
        WHERE gar_reestr_objects.object_table = 'gar_apartments' and gar.apartments.isactive;

CREATE UNIQUE INDEX ON gar_apartments (objectid);
CREATE INDEX ON gar_apartments (parentobjectid_adm);

-- комнаты
DROP MATERIALIZED VIEW IF EXISTS gar_rooms CASCADE;
CREATE MATERIALIZED VIEW gar_rooms AS
    SELECT gar_reestr_objects.*,
           gar.rooms.number
        FROM gar_reestr_objects
        JOIN gar.rooms USING (objectid)
        WHERE gar_reestr_objects.object_table = 'gar_rooms' and gar.rooms.isactive;

CREATE UNIQUE INDEX ON gar_rooms (objectid);
CREATE INDEX ON gar_rooms (parentobjectid_adm);

-- парковочные места
DROP MATERIALIZED VIEW IF EXISTS gar_carplaces CASCADE;
CREATE MATERIALIZED VIEW gar_carplaces AS
    SELECT gar_reestr_objects.*,
           gar.carplaces.number
        FROM gar_reestr_objects
        JOIN gar.carplaces USING (objectid)
        WHERE gar_reestr_objects.object_table = 'gar_carplaces' and gar.carplaces.isactive;

CREATE UNIQUE INDEX ON gar_carplaces (objectid);
CREATE INDEX ON gar_carplaces (parentobjectid_adm);

COMMENT ON MATERIALIZED VIEW gar_reestr_objects IS 'перечень объектов адресного справочника (ВСЕ объекты)';
COMMENT ON MATERIALIZED VIEW gar_addresses IS 'только планировачные объекты. особой ценности не имеет';
COMMENT ON MATERIALIZED VIEW gar_houses IS 'здания в широком смысле этого слова';
COMMENT ON MATERIALIZED VIEW gar_apartments IS 'квартиры';
COMMENT ON MATERIALIZED VIEW gar_carplaces IS 'парковочные места';
COMMENT ON MATERIALIZED VIEW gar_rooms IS 'комнаты и помещения';
COMMENT ON MATERIALIZED VIEW gar_steads IS 'земельные участки. особой ценности не имеет, но может быть местом где появиться здание в дальнейшем';

-- для обновлений автоматических
GRANT MAINTAIN ON gar_reestr_objects TO gar;
GRANT MAINTAIN ON gar_addresses TO gar;
GRANT MAINTAIN ON gar_houses TO gar;
GRANT MAINTAIN ON gar_apartments TO gar;
GRANT MAINTAIN ON gar_carplaces TO gar;
GRANT MAINTAIN ON gar_rooms TO gar;
GRANT MAINTAIN ON gar_steads TO gar;

COMMIT;
