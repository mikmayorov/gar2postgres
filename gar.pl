#!/usr/bin/perl

use warnings;
use strict qw/vars/;
use feature qw/say/;
use autodie;
use utf8;
use open qw/:std :encoding(utf8)/;

use POSIX qw(strftime);
use JSON::Parse qw/parse_json/;
use Data::Dump qw/dump dd/;
use Config::Simple;
use Getopt::Long;
use DBI;
use DBIx::RunSQL;

my $cfgfile = "gar.cfg";
my ( %cfg,
     $initdb,
     $addregion,
     $delregion,
     $update,
     $showstatus,
     %regions
   );

GetOptions ( 'config=s' => \$cfgfile,
             'initdb=s' => \$initdb,
             'add-region=s' => \$addregion,
             'del-region=s' => \$delregion,
             'update' => \$update,
             'show-status' => \$showstatus
           );

Config::Simple->import_from($cfgfile, \%cfg) or die Config::Simple->error();

# базовый каталог запуска
$0 =~ /^(.+\/).+?.pl$/;
our $cmddir = $1;

# определяем куда выводить логи
if ( defined $cfg{logfile} ) {
    open (STDOUT, '>>', "$cfg{workfiles}/gar.log");
    open (STDERR, '>&', STDOUT);
}

my $dbh = DBI->connect("dbi:Pg:dbname=$cfg{db_name};host=$cfg{db_host}", $cfg{db_user}, $cfg{db_password},
                       { PrintWarn => 0, PrintError => 0, RaiseError => 1, AutoCommit => 0 }) || die "Не могу соедениться с базой данных";


if ( defined $initdb ) {
    logging("main:: Инициализация БД по региону $initdb",1);
    my $file = "${cmddir}init-version.sql";
    DBIx::RunSQL->run_sql_file(verbose => $cfg{loglevel} - 2, dbh => $dbh, sql => $file);
    logging("main:: Созданы служебные таблицы из файла $file",1);
    loadgarfiles();

    my @index;
    my $fullzip = $dbh->selectrow_arrayref("SELECT gar_xml_full_local_file,version_id FROM version where gar_xml_full_local_file is not null order by version_id desc limit 1");
    my $version_id = $$fullzip[1];
    $fullzip = $$fullzip[0];
    if ( ! defined $fullzip ) {
        logging("main:: Отсутствует $fullzip. Создание структуры невозможно.",1);
        die;
    }
    foreach my $cregion ( ( 0, $initdb ) ) {
        foreach my $cfile ( xmlregionfiles($fullzip, $cregion) ) {
            logging("main:: Анализ структуры $cfile",2);
            $cfile =~ /AS_(.+?)_\d/;
            my $table=lc($1);
            my %columns;
            local $/="/><";
            open(my $FH, "unzip -p $fullzip $cfile |");
            while ( <$FH> ) {
                chomp;
                next if not /^(\w+) .+$/;
                while ( /(\w+)=/g ) {
                    $columns{$1} = 0;
                }
            }
            close $FH;
            my $SQL="";
            $SQL .= "CREATE TABLE $table (\n";
            foreach my $ccol (sort keys %columns) {
                my $datatype;
                $ccol = lc($ccol);
                if ( $ccol eq "desc" ) { $ccol = "description"; }
                if ($ccol =~ /date/ ) { $datatype="date"; }
                elsif ($ccol =~ /guid|adrobjectid/ ) { $datatype="uuid"; }
                elsif ($ccol =~ /^isa/ ) { $datatype="boolean"; }
                elsif ($ccol =~ /name/ ) { $datatype="text"; }
                elsif ($ccol =~ /id|level|type/ ) { $datatype="bigint"; }
                else { $datatype="text"; }
                $SQL .= "\t" . $ccol . "\t $datatype,\n";
                # массив с индексами
                if ( $ccol =~ /$cfg{index_colum}/ ) {
                    push @index, "CREATE INDEX ${table}_${ccol} ON $table ($ccol)";
                }
            }
            if ( $cregion eq "0" ) {
                chop $SQL;
                chop $SQL;
                $SQL .= "\n\t);\n";
            }
            else {
                $SQL .= "\tregion\tint";
                $SQL .= "\n\t) PARTITION BY list (region)\n";
            }
            logging("main:: $SQL", 3);
            $dbh->do($SQL);
        }
    }
    $dbh->do("INSERT INTO region VALUES (0, NULL)");
    foreach my $cindex ( @index ) {
        $dbh->do($cindex);
    }
    logging("main:: Инициализация БД завершена. Добавте необходимые регионы.",1);
    $dbh->commit;
}

if ( defined $addregion ) {
    logging("main:: Добавление нового региона: $addregion", 1);
    my $region_tables = $dbh->selectall_arrayref("SELECT table_name from information_schema.tables where table_schema = current_schema() and table_name in (select relname FROM pg_class where relkind = 'p')");
    foreach my $ctable ( @{$region_tables} ) {
        logging("main:: Добавляем $$ctable[0] для региона $addregion",2);
        $dbh->do("CREATE TABLE " . $$ctable[0] . "_$addregion PARTITION OF " . $$ctable[0] . " FOR VALUES IN ($addregion)");
    }
    $dbh->do("INSERT INTO region VALUES ($addregion, NULL)");
    logging("main:: Инициализация нового региона завершена.",1);
    $dbh->commit;
}

if ( defined $delregion ) {
    logging("main:: Удаление региона: $delregion", 1);
    my $region_tables = $dbh->selectall_arrayref("SELECT table_name from information_schema.tables where table_schema = current_schema() and table_name like '%$delregion'");
    foreach my $ctable ( @{$region_tables} ) {
        logging("main:: Удаляем $$ctable[0]",2);
        $dbh->do("DROP TABLE " . $$ctable[0]);
    }
    $dbh->do("DELETE FROM region WHERE region = $delregion");
    logging("main:: Удаление региона завершено.",1);
    $dbh->commit;
}

if ( defined $update ) {
    logging("main:: Обновление БД");
    loadgarfiles();
    my $regions = $dbh->selectall_arrayref("SELECT * FROM region WHERE version_sync is null or version_sync < (select max(version_id) from version)");
    foreach my $cregion ( @{$regions} ) {
        logging("main:: Обновляем регион " . $$cregion[0],1);
        if ( ! defined $$cregion[1]) {
            my $fullzip = $dbh->selectrow_arrayref("SELECT gar_xml_full_local_file,version_id FROM version where gar_xml_full_local_file is not null order by version_id desc limit 1");
            my $version_id = $$fullzip[1];
            $fullzip = $$fullzip[0];
            logging("main:: Первое обновление региона из последнего полного дампа $fullzip",1);
            foreach my $cfile ( xmlregionfiles($fullzip, $$cregion[0]) ) {
                $cfile =~ /AS_(.+?)_\d/;
                my $table = lc($1);
                if ($$cregion[0] ne "0") {
                    $table .= "_" . $$cregion[0];
                }
                logging("main:: Загружаем $cfile из $fullzip в таблицу $table",1);
                xml2table($fullzip, $cfile, $table);
                $$cregion[1] = $version_id;
            }
            $dbh->do("UPDATE region set version_sync = '$version_id' WHERE region = $$cregion[0]");
        }
        logging("main:: Текущая версия региона $$cregion[1]",1);
        my $deltaupdate = $dbh->selectall_arrayref("SELECT version_id,gar_xml_delta_local_file FROM version where version_id > '$$cregion[1]' and gar_xml_delta_local_file is not null order by version_id");
        foreach my $delta ( @{$deltaupdate} ) { 
            logging("main:: Применяем обновление $$delta[0]",1);
            foreach my $cfile ( xmlregionfiles($$delta[1], $$cregion[0]) ) {
                $cfile =~ /AS_(.+?)_\d/;
                my $table = lc($1);
                if ($$cregion[0] eq "0") {
                    # справочники из корня всегда идут с полной выгрузкой
                    # очищаем и загружаем их полностью
                    logging("main:: Очищаем таблицу и инициализируем текущими данными $table", 1);
                    $dbh->do("TRUNCATE TABLE $table");
                    xml2table($$delta[1],$cfile,$table);
                }
                else {
                    # загружаем обновление в отдельную временную таблицу
                    logging("main:: Создаем временную таблицу tmp_${table}_$$cregion[0]", 1);
                    $dbh->do("CREATE TABLE tmp_${table}_$$cregion[0] ( LIKE $table )");
                    xml2table($$delta[1],$cfile,"tmp_${table}_$$cregion[0]");
                    # синхронизируем таблицы
                    # во всех таблицах есть уникальный id объекта кроме reestr_objects!
                    # пришлось захардкодить проверку такую....
                    my $id;
                    if ( $table eq "reestr_objects" ) {
                        $id = "objectid";
                    }
                    else {
                        $id = "id";
                    }
                    logging("main:: Синхронизируем временную таблицу tmp_${table}_$$cregion[0] и основную ${table}_$$cregion[0] используя $id", 2);
                    $dbh->do("delete from ${table}_$$cregion[0] where $id in (select $id from tmp_${table}_$$cregion[0])");
                    $dbh->do("insert into ${table}_$$cregion[0] select * from tmp_${table}_$$cregion[0]");
                    $dbh->do("DROP TABLE tmp_${table}_$$cregion[0]");
                    $dbh->commit;
                }
            }
            $dbh->do("UPDATE region set version_sync = '$$delta[0]' WHERE region = $$cregion[0]");
            $dbh->commit;
            logging("main:: Обновление региона завершено");
        }
    }
}

if ( defined $showstatus ) {
    say "Статус локального хранилища";
    my $fullzip = $dbh->selectrow_arrayref("SELECT gar_xml_full_local_file,version_id FROM version where gar_xml_full_local_file is not null order by version_id desc limit 1");
    my $version_id = $$fullzip[1];
    $fullzip = $$fullzip[0];
    my $currentid = ${$dbh->selectrow_arrayref("SELECT max(version_id) FROM version")}[0];
    say "Полный архив $version_id: $fullzip";
    say "Последнее обновление $currentid";
    my $regions = $dbh->selectall_arrayref("SELECT * FROM region;");
    foreach my $cregion ( @{$regions} ) {
        say "\tРегион " . $$cregion[0] . " текущая версия " . $$cregion[1];
    }

}

$dbh->disconnect;

exit;

##########

sub logging {
    my $msg = shift;
    my $level = shift;
    if ( (! defined $level ) or ( $level <= $cfg{loglevel} ) ) {
        say strftime("%d.%M.%Y %H:%M:%S ", localtime()) . ' ['.$$ . '] '. $msg;
    }
}

sub xml2table {
    my $zip = shift;
    my $file = shift;
    my $table = shift;
    my @nulldata;
    my %columns_order;
    my $nr = 0;
    my $fr = 0;
    my $debug_data = 0;
    my $xmldata;
    my $sqldata;

    my $columns = $dbh->selectall_arrayref("select CASE WHEN column_name = 'description' THEN 'DESC' ELSE upper(column_name) END as column_name,
                                                   ordinal_position - 1 as position from information_schema.columns
                                                where table_name = '$table' and table_schema = CURRENT_SCHEMA() order by ordinal_position;");
    if (! defined $$columns[0]) {
        logging("xml2table:: Таблица $table отсутствует в БД. Пропускаем импорт данного файла.");
        return;
    }
    # формируем заготовку для одной строки данных в правильном порядке ориентируясь на оригинальный порядок указанный в information_schema.columns
    foreach my $ccol ( @{$columns} ) {
        $columns_order{$$ccol[0]} = $$ccol[1];
        if ( $$ccol[0] eq "REGION" ) {
            $table =~ /_(\d+)$/;
            push @nulldata, $1;
        }
        else {
            push @nulldata, "NULL"; 
        }
    }
    logging("xml2table:: Таблица: $table (" . join(",", keys %columns_order) . ")", 1);

    local $/="><";
    open(my $FH, "unzip -p $zip $file |");
    $dbh->do("COPY $table FROM STDIN WITH NULL 'NULL' DELIMITER '\007'");

    if ($debug_data) {
        my $fname = $file;
        $fname =~ s/\//_/;
        $fname = $cmddir . $$ . "_" . $table . "_" . $fname;
        open($xmldata, ">${fname}_xml");
        open($sqldata, ">${fname}_sql");
    }
    logging("xml2table:: Загружаем данные",1);
    while ( <$FH> ) {
        say $xmldata $_ if ( $debug_data );
        $fr++;
        logging("$nr: $_\n", 10);
        my @current_row = @nulldata;
        next if not /^(\w+) .+$/;
        my $nc = 0;
        while ( /(\w+)="(.+?)"/g ) {
            my $var=$1;
            my $val=$2;
            $current_row[$columns_order{$var}] = $val;
            $current_row[$columns_order{$var}] =~ s/&quot;/\"/g;
            $current_row[$columns_order{$var}] =~ s/\\/\\\\/g;
            $nc++;
        }
        logging("xml2table:: " . join("\t", @current_row) . "\n", 10);
        # вставляем строку если нашли хоть одно значение в строке
        if ( $nc ) {
            say $sqldata join("\007", @current_row) if ( $debug_data );
            $dbh->pg_putcopydata(join("\007", @current_row) . "\n");
            $nr++;
        }
    }
    if ($debug_data) {
        close $xmldata;
        close $sqldata;
    }
    $dbh->pg_putcopyend();
    $dbh->commit;
    logging("xml2table:: Успешно загружено $nr строк. Прочитано из файла $fr строк",1);
}

sub loadgarfiles {
    my $xmlfile = "$cfg{workfiles}/GetAllDownloadFileInfo";
    logging("loadgarfiles:: Загружаю список файлов доступных для скачивания $cfg{urlallfiles}", 1);
    downloadfile($cfg{urlallfiles}, $xmlfile);
    my $allremotefiles = `cat $xmlfile`;
    if (not defined $allremotefiles) {
        logging("loadgarfiles:: Ошибка чтения файла $xmlfile",1);
        die;
    }
    my $max_version_id = $dbh->selectcol_arrayref("SELECT max(version_id) FROM version");
    if ( defined $$max_version_id[0] ) {
        $max_version_id = $$max_version_id[0];
    }
    else {
        $max_version_id = 0;
    }
    my $newfile = 0;
    logging("loadgarfiles:: Последнее обновление файлов в БД: $max_version_id",1);
    my $sth = $dbh->prepare("INSERT INTO version (export_date,version_id,gar_xml_delta_url,gar_xml_full_url) VALUES (?,?,?,?)");
    foreach my $curupdate ( @{parse_json($allremotefiles)} ) {
        if ( ${$curupdate}{VersionId} > $max_version_id ) {
            logging("loadgarfiles:: Доступно новое обновлении: ${$curupdate}{VersionId}", 1);
            $sth->execute(${$curupdate}{Date}, ${$curupdate}{VersionId}, ${$curupdate}{GarXMLDeltaURL}, ${$curupdate}{GarXMLFullURL});
            $newfile=1;
        }
    }
    if ( $newfile == 1) {
        $dbh->commit;
        my $fullzip = $dbh->selectrow_arrayref("SELECT gar_xml_full_local_file,version_id FROM version where gar_xml_full_local_file is not null order by version_id desc limit 1");
        my $version_id = $$fullzip[1];
        $fullzip = $$fullzip[0];
        if ( defined $fullzip ) {
            logging("loadgarfiles:: Полный архив: $fullzip Скачиваем доступные обновления.",1);
            my $deltafiles = $dbh->selectall_arrayref("SELECT gar_xml_delta_url,version_id FROM version where version_id > '$version_id' and gar_xml_delta_local_file is null");
            foreach my $curupdate ( @{$deltafiles} ) {
                my $version_id = $$curupdate[1];
                my $src = $$curupdate[0];
                my $dst = "$cfg{workfiles}/" . $version_id . "_delta.zip";
                logging("loadgarfiles:: Пробуем скачать обновление $src",1);
                if ( downloadfile($src, $dst) == 0 ) {
                    $dbh->do("UPDATE version SET gar_xml_delta_local_file = '$dst' where version_id = '$version_id'");
                    logging("loadgarfiles:: Файл сохранен $dst",1);
                }
                else {
                    logging("loadgarfiles:: Что-то пошло не так.",1);
                }
            }
        } 
        else {
            my $src = $dbh->selectrow_arrayref("SELECT gar_xml_full_url,version_id FROM version order by version_id desc limit 1");
            my $version_id = $$src[1];
            $src = $$src[0];
            my $dst = "$cfg{workfiles}/" . $version_id . "_full.zip";
            logging("loadgarfiles:: Полный архив отсутствует. Качаем последний доступный полный архив $src",1);
            if ( downloadfile($src, $dst) == 0 ) {
                $dbh->do("UPDATE version SET gar_xml_full_local_file = '$dst' where version_id = '$version_id'");
                logging("loadgarfiles:: Файл сохранен $dst",1);
            }
            else {
                logging("loadgarfiles:: Что-то пошло не так.",1);
            }
        }
        $dbh->commit;
    }
    unlink $xmlfile;
}

sub downloadfile {
    my $src = shift;
    my $dst = shift;

    if ( -e $dst ) {
        logging("downloadfile:: $dst уже существует. Не качаем.");
        return 0;
    }

    logging("downloadfile:: wget $src $dst", 1);
    my @wget=("/usr/bin/wget");
    if ( $cfg{wget_verbose} eq "yes" ) {
        push @wget, "--append-output=$cfg{workfiles}/wget.log";
        push @wget, "--progress=dot:giga";
        push @wget, "--verbose";
    }
    else {
        push @wget, "--quiet";
    }
    push @wget, "--continue";
    push @wget, "--output-document=$dst.part";
    push @wget, "$src";
    my $exitcode = system(@wget);
    if ($exitcode == 0) {
        rename("$dst.part","$dst");
        logging("downloadfile:: успешное скачивание файла", 2);
        return 0;
    }
    logging("downloadfile:: не получилось скачать файл $src, wget вернул $exitcode");
    return $exitcode;
}

sub xmlregionfiles {
    my $file = shift;
    my $region = shift;
    my @rt;

    my $filelist=`unzip -qql $file | awk '{ print \$4 }'`;

    foreach my $cf (split (/\n/,$filelist)) {
        next if ( ( defined $cfg{skipfiles} ) and ( $cf =~ /$cfg{skipfiles}/ ) );
        next if ( $cf !~ /\.XML$/ );
        next if ( ( $cf =~ /^\d/ ) and ( $region eq 0 ) );
        next if ( ( $cf !~ /^$region/ ) and ( $region ne 0 ) );
        push @rt, $cf;
    }

    return @rt;

}
