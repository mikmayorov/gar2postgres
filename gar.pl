#!/usr/bin/perl

use warnings;
use strict;
use v5.10;

use JSON::Parse qw/parse_json/;
use Data::Dump qw/dump dd/;
use LWP::Simple;
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

my $dbh = DBI->connect("dbi:Pg:dbname=$cfg{db_name};host=$cfg{db_host}", $cfg{db_user}, $cfg{password},
                       { PrintWarn => 0, PrintError => 0, RaiseError => 1, AutoCommit => 0 }) || die "Не могу соедениться с базой данных";

# обновление файлов выгрузки
loadgarfiles();

# получение текущей информации о загруженных обновлениях из БД по интересующим нас регионам
foreach my $cr ( split($cfg{regions}, /,/) ) {
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
    if ( open(my $fh, '<:encoding(UTF-8)', "GetAllDownloadFileInfo") ) {
        $allfiles = <$fh>;
        close $fh;
        say "Используеться локальный файл GetAllDownloadFileInfo";
    }
    else {
        $allfiles = get($cfg{urlallfiles});
    }
    die "Ошибка получения файла $cfg{urlallfiles}" if not defined $allfiles;

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
            say "вносим информации о новом обновлении в БД: ${$cupdate}{VersionId}";
            $sth->execute(${$cupdate}{Date}, ${$cupdate}{VersionId}, ${$cupdate}{GarXMLDeltaURL}, ${$cupdate}{GarXMLFullURL});
        }
    }
    $dbh->commit;

    # обновляем текущую БД файлами которые реально лежат на диске
    $dbh->do("UPDATE version SET gar_xml_delta_local_file = NULL, gar_xml_full_local_file = NULL");
    my $sth_delta = $dbh->prepare("UPDATE version (gar_xml_delta_local_file) VALUES (?) WHERE version_id = ?");
    my $sth_full = $dbh->prepare("UPDATE version (gar_xml_delta_full_file) VALUES (?) WHERE version_id = ?");
    my $localfiles = `/bin/ls $cfg{downloadfiles}/*.zip 2> /dev/null`;
    print "! /bin/ls $cfg{downloadfiles}/*.zip 2> /dev/null : $localfiles !\n";
    dd split( $localfiles );
    say "непонятно";
    foreach my $cf ( split(`ls $cfg{downloadfiles}/*.zip 2> /dev/null`, /\n/) ) {
        print "локальный файл - $cf: ";
        if ( $cf =~ /(\d+)_(full|delta)\.zip$/ ) {
            if ( $2 eq "full" ) {
                say "full выгрузка";
                $sth_delta->execute($cf, $1);
            } else {
                say "delta выгрузка";
                $sth_full->execute($cf, $1);
            }
        }
        else {
            say "неизвестный формат";
        }
    }
    $dbh->commit;

    # в локальном хранилище должен ОБЯЗАТЕЛЬНО лежать полный архив и все обновления которые были сделаны после полного архива для корректной загрузки в БД
    my $fullzip = $dbh->selectcol_arrayref("SELECT gar_xml_full_local_file FROM version where gar_xml_full_local_file is not null order by version_id desc limit 1");
    if ( ! defined $$fullzip[0] ) {
        say "Нет полного архива в БД";
    }
}
