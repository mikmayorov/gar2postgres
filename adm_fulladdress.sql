CREATE OR REPLACE FUNCTION adm_address(_objectid bigint, _shortname boolean DEFAULT false, _debug integer DEFAULT 0) RETURNS text AS
$BODY$
DECLARE
    _rt text;
    _levelid int;
    _name text;
    _data RECORD;
BEGIN
    IF _debug > 5 THEN
        RAISE NOTICE 'DEBUG[5]: find objectid: %', objectid;
    END IF;
    SELECT * INTO _data FROM adm_hierarchy WHERE objectid = _objectid;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    IF _debug > 5 THEN
        RAISE NOTICE 'DEBUG[5]: find in adm_hierarchy -> objectid: %, parentobjid: %', _data.objectid, _data.parentobjid;
    END IF;
    -- определяем в какой таблице искать информацию об этом object_id
    SELECT levelid INTO _levelid FROM reestr_objects WHERE objectid = _objectid;
    CASE _levelid
        WHEN 1,2,3,4,5,6,7,8,13,14,15,16 THEN -- объект адреса
            SELECT concat_ws(' ', (SELECT CASE WHEN _shortname THEN shortname ELSE name END
                                        FROM addr_obj_types
                                        WHERE shortname=A.typename and
                                              level=A.level),
                                    A.name)
                FROM addr_obj as A
                WHERE objectid = _objectid
                INTO _name;
        WHEN 9 THEN -- земля
            SELECT number FROM steads WHERE objectid = _objectid INTO _name;
        WHEN 10 THEN -- строение
            SELECT concat_ws(' ', (SELECT lower(name)
                                        FROM house_types WHERE house_types.id=A.housetype),
                                    housenum,
                                    (SELECT lower(name)
                                        FROM addhouse_types
                                        WHERE id=A.addtype1),
                                    addnum1,
                                    (SELECT lower(name)
                                        FROM addhouse_types
                                        WHERE id=A.addtype2),
                                    addnum2)
                FROM houses as A
                WHERE objectid = _objectid
                INTO _name;
        WHEN 11 THEN -- помещение
            SELECT number FROM apartments WHERE objectid = _objectid INTO _name;
        WHEN 12 THEN -- комната
            SELECT number FROM rooms WHERE objectid = _objectid INTO _name;
        WHEN 17 THEN -- парковка
            SELECT number FROM carplaces WHERE objectid = _objectid INTO _name;
        ELSE
            RAISE EXCEPTION 'unknown levelid: % for objectid: %', _levelid, _objectid;
    END CASE;
    IF _data.parentobjid IS NOT NULL THEN
        _rt := concat_ws(', ', adm_address(_data.parentobjid,_shortname,_debug), _name);
    ELSE
        _rt := concat_ws(', ', _name, _rt);
    END IF;
    RETURN _rt;
END
$BODY$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = gar, public;
