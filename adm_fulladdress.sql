CREATE OR REPLACE FUNCTION abbr_dot_add(_t text) RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
AS $BODY$
BEGIN
    IF _t ~ '\.$' THEN
    ELSIF _t ~ '(-|\.|парк)' THEN
    ELSE
        _t := _t || '.';
    END IF;
    RETURN _t;
END
$BODY$;

CREATE OR REPLACE FUNCTION adm_address(_objectid bigint, _address_format integer DEFAULT 1, _abbr boolean DEFAULT true, _debug integer DEFAULT 0) RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO gar
AS $BODY$
-- _address_format - формат адреса:
--      1 - полный адрес
--      2 - адрес без региона
--      3 - адрес без региона и населенного пункта
--      4 - только регион и населенный пункт
-- _abbr - использовать абривиатуры вместо полных названий
DECLARE
    _rt text;
    _levelid int;
    _name text;
    _parentobjid bigint;
    _prefixid text;
    _prefix text;
BEGIN
    IF _debug > 5 THEN
        RAISE NOTICE 'DEBUG[5]: objectid: "%"', _objectid;
    END IF;
    SELECT levelid INTO _levelid FROM reestr_objects WHERE objectid = _objectid and isactive;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT parentobjid INTO _parentobjid FROM adm_hierarchy WHERE objectid = _objectid and isactive;
    IF _debug > 5 THEN
        RAISE NOTICE 'DEBUG[5]: levelid: "%" parentobjid: "%"', _levelid, _parentobjid;
    END IF;
    -- определяем в какой таблице искать информацию об этом object_id
    CASE _levelid
        WHEN 1,2,3,4,5,6,7,8 THEN -- адрес
            IF ( _address_format in (2,3) ) and ( _levelid in (1,2,3) ) THEN
            ELSIF ( _address_format in (3) ) and ( _levelid in (4,5,6) ) THEN
            ELSIF ( _address_format in (4) ) and ( _levelid in (7,8) ) THEN
            ELSE
                SELECT name, typename INTO _name, _prefixid FROM addr_obj WHERE objectid = _objectid and isactive;
                SELECT lower(CASE WHEN _abbr THEN abbr_dot_add(shortname) ELSE name END) INTO _prefix FROM addr_obj_types WHERE level = _levelid and shortname = _prefixid;
                IF _levelid IN (1,2,3) THEN
                    _name := concat_ws(' ', _name, _prefix);
                ELSE
                    _name := concat_ws(' ', _prefix, _name);
                END IF;
            END IF;
        WHEN 9 THEN -- земля
            IF ( _address_format in (4) ) THEN
            ELSE
                SELECT number FROM steads WHERE objectid = _objectid  and isactive INTO _name;
            END IF;
        WHEN 10 THEN -- строение / дом
            IF ( _address_format in (4) ) THEN
            ELSE
                SELECT lower(concat_ws(' ', (SELECT CASE WHEN _abbr THEN abbr_dot_add(shortname) ELSE name END FROM house_types WHERE house_types.id=A.housetype), housenum,
                                            (SELECT CASE WHEN _abbr THEN abbr_dot_add(shortname) ELSE name END FROM addhouse_types WHERE id=A.addtype1), addnum1,
                                            (SELECT CASE WHEN _abbr THEN abbr_dot_add(shortname) ELSE name END FROM addhouse_types WHERE id=A.addtype2), addnum2
                                      )
                            )
                    INTO _name FROM houses as A WHERE objectid = _objectid and isactive;
            END IF;
        WHEN 11 THEN -- помещение
            IF ( _address_format in (4) ) THEN
            ELSE
                SELECT lower(concat_ws(' ', (select CASE WHEN _abbr THEN abbr_dot_add(shortname) ELSE name END from apartment_types where id = A.aparttype), number))
                    FROM apartments as A WHERE objectid = _objectid and isactive INTO _name;
            END IF;
        WHEN 12 THEN -- комната
            IF ( _address_format in (4) ) THEN
            ELSE
                SELECT lower(concat_ws(' ', (select CASE WHEN _abbr THEN abbr_dot_add(shortname) ELSE name END from room_types where id = A.roomtype), number))
                    FROM rooms as A WHERE objectid = _objectid and isactive INTO _name;
            END IF;
        WHEN 17 THEN -- парковка
            IF ( _address_format in (4) ) THEN
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
$BODY$ ;
