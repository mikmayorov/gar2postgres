#!/usr/bin/perl

use warnings;
use strict;
use v5.10;
use utf8;

use Data::Dump qw/dump dd/;
use Config::Simple;
use DBI;
use open qw(:std :utf8);

my %cfg;
my $cfgfile = "gar.cfg";

Config::Simple->import_from($cfgfile, \%cfg) or die Config::Simple->error();

my $zipfile=$ARGV[0]; # путь к архиву
my $region=$ARGV[1]; # регион какой заливать, 0 регион означает справочники из корня
my $fullload=1; # заливаеться без процедуры diff, предварительно таблица очищается или создаеться если это регион....

if ( $zipfile =~ /delta\.zip$/ ) { $fullload = 0; }

my $xmlfilelist = `unzip -qql $ARGV[0] | awk '{ print \$4 }'`;

my $dbh = DBI->connect("dbi:Pg:dbname=$cfg{db_name};host=$cfg{db_host}", $cfg{db_user}, $cfg{db_password},
            { PrintWarn => 0, PrintError => 0, RaiseError => 1, AutoCommit => 0 }) || die "Не могу соедениться с базой данных";

if ( $fullload == 1 ) {
    say "Распаковываем регион $region из файла $zipfile с полной очистой таблиц и загрузкой";
}
else {
    say "Распаковываем регион $region из файла $zipfile во временную БД и вносим изменения";
}

foreach my $cf (split (/\n/,$xmlfilelist)) {
    next if ( $cf =~ /(NORMATIVE_DOCS)/ );
    next if ( $cf !~ /\.XML$/ );
    next if (( $cf =~ /^\d/ ) and ( $cf !~ /^$region\// ));
    next if (( $cf =~ /^AS/) and ( $region ne "0" ))  ;
    $cf =~ /AS_(.+?)_\d/;
    my $table=lc($1);
    if ( $fullload == 1 ) {
        if ( $region eq "0" ) {
            $dbh->do("TRUNCATE TABLE $table");
        }
        else {
            $dbh->do("DROP TABLE IF EXISTS ${table}_$region");
            $dbh->do("CREATE TABLE ${table}_$region PARTITION OF $table FOR VALUES IN ($region)");
        }
    }
    if ( $region ne "0" ) { $table .= "_$region" };
    my %columns;
    my @nulldata;
    say "Обрабатываем файл $cf / таблица $table";
    my $data = $dbh->selectall_arrayref("select CASE WHEN column_name = 'description' THEN 'DESC' ELSE upper(column_name) END as column_name, ordinal_position - 1 as position from information_schema.columns where table_name = '$table' and table_schema = CURRENT_SCHEMA() order by ordinal_position;");
    if (! defined $$data[0]) {
        say "Таблица $table отсутствует в БД. Пропускаем импорт данного файла.";
        next;
    }
    foreach my $data ( @{$data} ) {
        $columns{$$data[0]} = $$data[1];
        if ( $$data[0] eq "REGION" ) {
            push @nulldata, $region;
        }
        else {
            push @nulldata, "NULL"; 
        }
    }
#    dd %columns;
#   dd @nulldata;
    local $/="/><";
    open(my $FH, "unzip -p $ARGV[0] $cf |");
    binmode $FH, ":utf8";
    <$FH>;
    $dbh->do("COPY $table FROM STDIN WITH NULL 'NULL' DELIMITER '\007'");
    while ( <$FH> ) {
        my @current_row = @nulldata;
#        dd @current_row;
#        chomp;
        next if not /^(\w+) .+$/;
        while ( /(\w+)="(.+?)"/g ) {
            $current_row[$columns{$1}] = $2;
        }
#        say join('|', @current_row);
        $dbh->pg_putcopydata(join("\007", @current_row) . "\n");
    }
    $dbh->pg_putcopyend();
    $dbh->commit;
}
$dbh->disconnect;
