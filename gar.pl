#!/usr/bin/perl

use warnings;
use strict;
use v5.10;

use JSON::Parse qw/parse_json/;
use Data::Dump qw/dump dd/;
use Config::Simple;
use Getopt::Long;
use DBI;

my %cfg;
my $cfgfile = "gar.cfg";
my $verbose = 0;
my $initdb = 0;
my %regions;

GetOptions ('config=s' => \$cfgfile, 'verbose' => \$verbose, 'initdb' => \$initdb);
Config::Simple->import_from($cfgfile, \%cfg) or die Config::Simple->error();

my $dbh = DBI->connect("dbi:Pg:dbname=$cfg{db_name};host=$cfg{db_host}", $cfg{db_user}, $cfg{db_password},
                       { PrintWarn => 0, PrintError => 0, RaiseError => 1, AutoCommit => 0 }) || die "Не могу соедениться с базой данных";

# проверка наличия файлов для обновления и их скачивание
(loadgarfiles() != 0) && die "ошибка получения файлов обновлекний";

# получение текущей информации о загруженных обновлениях из БД по интересующим нас регионам
say "Синхронизируем информацию по следующим регионам: " . join(",", @{$cfg{regions}});
foreach my $curregion ( @{$cfg{regions}} ) {
}

if ( $initdb ) {
    say "Создание объектов в БД $cfg{db_name} на $cfg{db_host}";
    }

$dbh->disconnect;

exit;

# -----------------------------------------------------------------------------------------------
sub loadgarfiles {

    my $allfiles;
    say "Получаю список файлов доступных для скачивания $cfg{urlallfiles}";
    if ( downloadfile($cfg{urlallfiles}, "GetAllDownloadFileInfo") == 0 ) {
        say "Обновлен список $cfg{urlallfiles}";
    }
    if ( open(my $fh, '<:encoding(UTF-8)', "$cfg{downloadfiles}/GetAllDownloadFileInfo") ) {
        $allfiles = <$fh>;
        close $fh;
    }
    
    if (not defined $allfiles) {
        say "Ошибка получения файла $cfg{urlallfiles}";
        return -1;
    }

    my $max_version_id = $dbh->selectcol_arrayref("SELECT max(version_id) FROM version");
    if ( defined $$max_version_id[0] ) {
        $max_version_id = $$max_version_id[0];
    }
    else {
        $max_version_id = 0;
    }
    say "Последнее обновление в БД: $max_version_id";

    my $sth = $dbh->prepare("INSERT INTO version (export_date,version_id,gar_xml_delta_url,gar_xml_full_url) VALUES (?,?,?,?)");
    foreach my $cupdate ( @{parse_json($allfiles)} ) {
        if ( ${$cupdate}{VersionId} > $max_version_id ) {
            say "доступно новое обновлении в БД: ${$cupdate}{VersionId}";
            $sth->execute(${$cupdate}{Date}, ${$cupdate}{VersionId}, ${$cupdate}{GarXMLDeltaURL}, ${$cupdate}{GarXMLFullURL});
        }
    }
    $dbh->commit;

    synclocalfiles();

    # в локальном хранилище должен ОБЯЗАТЕЛЬНО лежать полный архив и все обновления которые были сделаны после полного архива для корректной загрузки в БД
    my $fullzip = $dbh->selectcol_arrayref("SELECT gar_xml_full_local_file FROM version where gar_xml_full_local_file is not null order by version_id desc limit 1");
    $fullzip = $$fullzip[0];
    defined $fullzip && say "Полный архив: " . $fullzip;
    if ( ! defined $fullzip ) {
        $fullzip = $dbh->selectrow_arrayref("SELECT version_id, gar_xml_full_url FROM version order by version_id desc limit 1");
        my $url=$$fullzip[1];
        my $localfile=$$fullzip[0] . "_full.zip";
        if (downloadfile($url, $localfile) == 0) { 
            say "Успешно скачан архив $url";
            synclocalfiles();
        }
        else {
            say "Не смогли скачать полный архив. Попробуйте еще раз.";
            return(-1);
        };
    }

    # проверяем наличее всех delta файлов после полного архива и если нехватает, то скачиваем
    my $flagupdatebd = 0;
    my $delta = $dbh->selectall_arrayref("SELECT version_id, gar_xml_delta_url FROM version WHERE version_id > (SELECT version_id from version WHERE gar_xml_full_local_file is not null) and gar_xml_delta_local_file IS NULL");
    foreach my $curdelta ( @$delta ) {
        say "Скачиваем: $$curdelta[1] в $$curdelta[0]_delta.zip";
        if (downloadfile($$curdelta[1], "$$curdelta[0]_delta.zip") == 0) { 
            say "Успешно скачан архив $$curdelta[1]";
            $flagupdatebd += 1;
        }
    }
    if ( $flagupdatebd > 0 ) {
        say "Скачали обновлений: $flagupdatebd";
        synclocalfiles();
    }

    # финальная проверка что все файлы необходимые для синхронизации справичника (1 full и все delta) получены
    my $cdelta = $dbh->selectrow_arrayref("SELECT count(*) FROM version WHERE version_id > (SELECT version_id from version WHERE gar_xml_full_local_file is not null) and gar_xml_delta_local_file is null");
    if ( $$cdelta[0] > 0 ) {
        say "Не все delta файлы загружены. Повторите попытку обновления еще раз.";
        return(-1);
    }
    return(0);
}

sub synclocalfiles {
    # обновляем текущую БД файлами которые реально лежат на диске
    $dbh->do("UPDATE version SET gar_xml_delta_local_file = NULL, gar_xml_full_local_file = NULL");
    my $sth_delta = $dbh->prepare("UPDATE version SET gar_xml_delta_local_file=? WHERE version_id = ?");
    my $sth_full = $dbh->prepare("UPDATE version SET gar_xml_full_local_file=? WHERE version_id = ?");
    my $localfiles = `/bin/ls $cfg{downloadfiles}/*.zip 2> /dev/null`;
    foreach my $cf ( split(/\n/, $localfiles) ) {
        ( $verbose > 2 ) && print "локальный файл $cf: ";
        if ( $cf =~ /(\d+)_(full|delta)\.zip$/ ) {
            if ( $2 eq "full" ) {
                ( $verbose > 2 ) && say "full выгрузка";
                $sth_full->execute($cf, $1);
            } else {
                ( $verbose > 2 ) && say "delta выгрузка";
                $sth_delta->execute($cf, $1);
            }
        }
        else {
            ( $verbose > 2 ) && say "неизвестный формат";
        }
    }
    $dbh->commit;
}

sub downloadfile {
    my $url = shift;
    my $save = shift;
    # скачиваем файл
    my @wget=("/usr/bin/wget");
    push @wget, "--append-output=$cfg{downloadfiles}/wget.log";
#    push @wget, "--no-verbose";
    push @wget, "--continue";
    push @wget, "--progress=dot:giga";
    push @wget, "--output-document=$cfg{downloadfiles}/$save.part";
    push @wget, "$url";
    my $exitcode = system(@wget);
    if ($exitcode == 0) { rename("$cfg{downloadfiles}/$save.part","$cfg{downloadfiles}/$save"); return 0; }
    return 1;
}
