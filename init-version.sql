BEGIN;
DROP TABLE IF EXISTS region CASCADE;
DROP TABLE IF EXISTS version CASCADE;

CREATE TABLE IF NOT EXISTS version (
    export_date date,
    version_id bigint PRIMARY KEY,
    gar_xml_delta_url text,
    gar_xml_delta_local_file text,
    gar_xml_full_url text,
    gar_xml_full_local_file text
);

CREATE TABLE IF NOT EXISTS region (
    region integer,
    version_sync bigint REFERENCES version ( version_id ) ON DELETE restrict ON UPDATE restrict
);

insert into version (version_id,gar_xml_full_local_file) VALUES ('20241001','/home/mik/work/gar2/distfiles/20241001_full.zip');

END;
