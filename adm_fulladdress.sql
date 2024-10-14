CREATE OR REPLACE FUNCTION adm_fulladdress(_objectid bigint, _debug integer DEFAULT 0) RETURNS text AS
$BODY$
DECLARE
    _region int;
    _rt text;
    _levelid int;
    _name text;
    _data RECORD;
BEGIN
    IF _debug > 5 THEN
        RAISE NOTICE 'DEBUG[5]: find objectid: %', objectid;
    END IF;
    SELECT * INTO _data FROM adm_hierarchies WHERE objectid = _objectid;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    IF _debug > 5 THEN
        RAISE NOTICE 'DEBUG[5]: find in adm_hierarchies -> objectid: %, parentobjid: %', _data.objectid, _data.parentobjid;
    END IF;
    -- определяем в какой таблице искать информацию об этом object_id
    SELECT levelid INTO _levelid FROM reestr_objs WHERE objectid = _objectid;
    CASE _levelid
        WHEN 1,2,3,4,5,6,7,8,13,14,15,16 THEN -- объект адреса
            SELECT concat_ws('' '', (SELECT lower(name) from addr_obj_types where shortname=A.typename and level=A.level),A.name) FROM addr_objs as A WHERE objectid = _objectid INTO _name;
        WHEN 9 THEN -- земля
        WHEN 10 THEN -- строение
            SELECT concat_ws('' '',(SELECT lower(name) from house_types where house_types.id=A.housetype),housenum,(SELECT lower(name) from house_add_types where house_add_types.id=A.addtype1),addnum1,(SELECT lower(name) from house_add_types where house_add_types.id=A.addtype2),addnum2) FROM houses as A WHERE objectid = _objectid INTO _name;
        WHEN 11 THEN -- помещение
            SELECT name FROM houses WHERE objectid = _objectid INTO _name;
        WHEN 17 THEN -- парковка
        ELSE
            RAISE EXCEPTION 'unknown levelid: % for objectid: %', _levelid, _objectid;
    END CASE;
    IF _data.parentobjid IS NOT NULL THEN
        _rt := concat_ws(', ', adm_fulladdress(_data.parentobjid,_debug), _name);
    ELSE
        _rt := concat_ws(', ', _name, _rt);
    END IF;
    RETURN _rt;
END
$BODY$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = gar, public;
