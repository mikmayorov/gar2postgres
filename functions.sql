CREATE OR REPLACE FUNCTION gar.abbr_dot_add(_t text) RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO gar
AS $BODY$
BEGIN
    IF _t ~ '\.$' THEN
    ELSIF _t ~ '(-|\.|парк|край)' THEN
    ELSE
        _t := _t || '.';
    END IF;
    RETURN _t;
END
$BODY$;
COMMENT ON FUNCTION gar.abbr_dot_add IS 'используеться в gar.adm_address для формирования сокращений';

CREATE OR REPLACE FUNCTION gar.adm_address(_objectid bigint, _address_format integer DEFAULT 1, _abbr boolean DEFAULT true, _debug integer DEFAULT 0) RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO gar
AS $BODY$
-- 
-- Функция возвращает адрес в соответствии с административным делением в различных форматах
--
-- _objectid - идентификатор ГАР (не uuid)
-- _address_format - формат адреса:
--      1 - полный адрес (по умолчанию)
--      2 - адрес без региона
--      3 - адрес без региона и населенного пункта
--      4 - только регион и населенный пункт
--      5 - только регион
--      6 - только населенный пункт
-- _abbr - какие название объектов использовать:
--      true - аббревиатуры
--      false - полные
--      null - не выводить вообще
-- _debug - уровень отладочных сообщений. в обычном режиме 0
--
--  addr_obj
--       1 - Субъект РФ, 
--       2 - Административный район,
--       3 - Муниципальный район,
--       4 - Сельское/городское поселение,
--       5 - Город,
--       6 - Населенный пункт,
--       7 - Элемент планировочной структуры,
--       8 - Элемент улично-дорожной сети
--  steads
--       9 - Номер участка
--  houses
--       10 - Дома (строения)
--  apartments
--       11 - Квартиры
--  rooms
--       12 - Комнаты
--  carplaces
--       17 - Парковочные места

DECLARE
    _rt text;
    _levelid int;
    _name text;
    _parentobjid bigint;
    _prefixid text;
    _prefix text;
BEGIN
    IF _address_format NOT BETWEEN 1 AND 8 THEN
        RAISE EXCEPTION 'unknown address format: "%"', _address_format
            USING HINT = '1 - полный адрес (по умолчанию) (levelid not check)
       2 - адрес без региона ( >= 4 )
       3 - адрес без региона и населенного пункта ( >=7 )
       4 - только регион и населенный пункт  (1-6)
       5 - только регион (1-3)
       6 - только населенный пункт (4-6)
       7 - элементы планировочной структуры и улично-дорожной (7-8)
       8 - дом, квартира, комната, парковочное место или участок (9-12,17)
       третий параметр (название объектов): true - аббревиатуры, false - полные наименования, null - не выводить вообще';
    END IF;
    IF _debug > 5 THEN
        RAISE NOTICE 'DEBUG[5]: objectid: "%"', _objectid;
    END IF;
    SELECT levelid INTO _levelid FROM reestr_objects WHERE objectid = _objectid and isactive;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT parentobjid INTO _parentobjid FROM adm_hierarchy WHERE objectid = _objectid and isactive;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    IF _debug > 5 THEN
        RAISE NOTICE 'DEBUG[5]: levelid: "%" parentobjid: "%"', _levelid, _parentobjid;
    END IF;
    -- в зависимости от levelid определяем в какой таблице искать информацию об этом object_id
    CASE _levelid
        WHEN 1,2,3,4,5,6,7,8 THEN
            IF ( _address_format = 2 ) and ( _levelid in (1,2,3) ) THEN
            ELSIF ( _address_format = 3 ) and ( _levelid in (1,2,3,4,5,6) ) THEN
            ELSIF ( _address_format = 4 ) and ( _levelid in (7,8) ) THEN
            ELSIF ( _address_format = 5 ) and ( _levelid in (4,5,6,7,8) ) THEN
            ELSIF ( _address_format = 6 ) and ( _levelid in (1,2,3,7,8) ) THEN
            ELSIF ( _address_format = 7 ) and ( _levelid in (1,2,3,4,5,6) ) THEN
            ELSIF ( _address_format = 8 ) THEN
            ELSE
                SELECT name, typename INTO _name, _prefixid FROM addr_obj WHERE objectid = _objectid and isactive;
                IF _abbr IS NOT NULL THEN
                    SELECT lower(CASE WHEN _abbr THEN abbr_dot_add(shortname) ELSE name END) INTO _prefix FROM addr_obj_types WHERE level = _levelid and shortname = _prefixid;
                    IF _levelid IN (1,2,3) THEN
                        _name := concat_ws(' ', _name, _prefix);
                    ELSE
                        _name := concat_ws(' ', _prefix, _name);
                    END IF;
                END IF;
            END IF;
        WHEN 9 THEN -- земля
            IF ( _address_format in (4,5,6,7) ) THEN
            ELSE
                SELECT number FROM steads WHERE objectid = _objectid and isactive INTO _name;
            END IF;
        WHEN 10 THEN -- строение / дом
            IF ( _address_format in (4,5,6,7) ) THEN
            ELSE
                SELECT concat_ws(' ', lower((SELECT CASE WHEN _abbr IS NULL THEN NULL WHEN _abbr THEN abbr_dot_add(shortname) ELSE name END FROM house_types WHERE house_types.id=A.housetype)), housenum,
                                            lower((SELECT CASE WHEN _abbr IS NULL THEN NULL WHEN _abbr THEN abbr_dot_add(shortname) ELSE name END FROM house_types WHERE house_types.id=A.addtype1)), addnum1,
                                            lower((SELECT CASE WHEN _abbr IS NULL THEN NULL WHEN _abbr THEN abbr_dot_add(shortname) ELSE name END FROM house_types WHERE house_types.id=A.addtype2)), addnum2
                                )
                    INTO _name FROM houses as A WHERE objectid = _objectid and isactive;
            END IF;
        WHEN 11 THEN -- помещение
            IF ( _address_format in (4,5,6,7) ) THEN
            ELSE
                SELECT concat_ws(' ', lower((select CASE WHEN _abbr IS NULL THEN NULL WHEN _abbr THEN abbr_dot_add(shortname) ELSE name END from apartment_types where id = A.aparttype)), number)
                    FROM apartments as A WHERE objectid = _objectid and isactive INTO _name;
            END IF;
        WHEN 12 THEN -- комната
            IF ( _address_format in (4,5,6,7) ) THEN
            ELSE
                SELECT concat_ws(' ', lower((select CASE WHEN _abbr IS NULL THEN NULL WHEN _abbr THEN abbr_dot_add(shortname) ELSE name END from room_types where id = A.roomtype)), number)
                    FROM rooms as A WHERE objectid = _objectid and isactive INTO _name;
            END IF;
        WHEN 17 THEN -- парковка
            IF ( _address_format in (4,5,6,7) ) THEN
            ELSE
                SELECT number FROM carplaces WHERE objectid = _objectid and isactive INTO _name;
            END IF;
        ELSE
            _name := concat_ws('unknown levelid: ', _levelid, ' for objectid: ', _objectid);
    END CASE;
    IF _debug > 5 THEN
        RAISE NOTICE 'DEBUG[5]: name: "%"', _name;
    END IF;

    _rt := concat_ws(', ', adm_address(_parentobjid,_address_format,_abbr,_debug), _name);

    -- concat_ws(', ', NULL) возвращает пустую строку а не NULL
    IF _rt = '' THEN
        _rt := NULL;
    END IF;

    RETURN _rt;

END
$BODY$;

COMMENT ON FUNCTION gar.adm_address IS 'возвращает адрес в соответствии с административным делением в различных форматах';

CREATE OR REPLACE FUNCTION gar.adm_getallchildobjectid(_objectid bigint) RETURNS TABLE (objectid bigint)
LANGUAGE SQL
SECURITY DEFINER
SET search_path TO gar
AS $BODY$
-- 
-- Функция возвращает все объекты которые имеют родителем _objectid по административному делению
--
    WITH RECURSIVE res AS (
        SELECT  reestr_objects.objectid
            FROM reestr_objects
            WHERE reestr_objects.isactive and reestr_objects.objectid = _objectid
    UNION
        SELECT  reestr_objects.objectid
            FROM reestr_objects
                JOIN adm_hierarchy USING (objectid)
                JOIN res ON (adm_hierarchy.parentobjid = res.objectid)
            WHERE reestr_objects.isactive and adm_hierarchy.isactive
    ) SELECT objectid from res;

$BODY$;

COMMENT ON FUNCTION gar.adm_getallchildobjectid IS 'возвращает все объекты которые имеют родителем objectid по административному делению';

CREATE OR REPLACE FUNCTION gar.adm_isparent(_objectid_pattern bigint, _objectid_check bigint) RETURNS boolean
LANGUAGE SQL
SECURITY DEFINER
SET search_path TO gar
AS $BODY$
-- 
-- Функция возвращает TRUE если _objectid_check имеет в родителях _objectid_pattern
--
SELECT CASE WHEN count(*) > 0 then true ELSE false END
    FROM adm_hierarchy WHERE objectid = _objectid_pattern and
                             (path LIKE '%.' || _objectid_check || '.%' or
                             path LIKE '' || _objectid_check || '.%');
$BODY$;

COMMENT ON FUNCTION gar.adm_isparent IS 'Функция возвращает TRUE если _objectid_check имеет в родителях _objectid_pattern';
