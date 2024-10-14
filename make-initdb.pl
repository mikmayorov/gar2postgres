#!/usr/bin/perl

use warnings;
use strict;
use v5.10;
use utf8;
use open qw(:std :utf8);

use Data::Dump qw/dump dd/;

my $filelist=`unzip -qql $ARGV[0] | awk '{ print \$4 }'`;
my $extractlist;

my $dstdir=$ARGV[1];
die if (! -d $dstdir);

local $/="/><";

my %structure_root;
my %structure_region;

foreach my $cf (split (/\n/,$filelist)) {
    next if (( $cf =~ /^\d/ ) and ( $cf !~ /^61\// ));
    next if ( $cf !~ /\.XML$/ );
    my $typeoftable = "root";
    if ( $cf =~ /^\d/ ) {
        $typeoftable = "region";
    }
    say STDERR "$cf";
    $cf =~ /AS_(.+?)_\d/;
    my $table=$1;
    local $/="/><";
    open(my $FH, "unzip -p $ARGV[0] $cf |");
    binmode $FH, ":utf8";
    while ( <$FH> ) {
        chomp;
        next if not /^(\w+) .+$/;
        while ( /(\w+)="(.+?)"/g ) {
            my $col=$1;
            if ($col eq "DESC") { $col = "description" }
            my $datatype=$2;
            if ($typeoftable eq "region") {
                $structure_region{$table}{$col}=$datatype;
            } 
            else {
                $structure_root{$table}{$col}=$datatype;
            }
        }
    }
}

open(my $SQL, "> $dstdir/init-gar-object.sql") or die;
open(my $PERLFORMAT, "> $dstdir/gar-format.pl") or die;

foreach my $table ( keys %structure_root ) {
    my @columns_sql=();
    my @columns_format=();
    foreach my $col ( sort keys %{$structure_root{$table}} ) {
        my $datatype;
        $col = lc($col);

        if ($col =~ /date/ ) { $datatype="date"; }
        elsif ($col =~ /guid|adrobjectid/ ) { $datatype="uuid"; }
        elsif ($col =~ /^isa/ ) { $datatype="boolean"; }
        elsif ($col =~ /id|level/ ) { $datatype="bigint"; }
        else { $datatype="text"; }

        push @columns_sql, "\t" . $col . " " . $datatype;
        push @columns_format, '$' . $col;
    }
    say $SQL "CREATE TABLE " . lc($table) . " (";
    say $SQL join(",\n", @columns_sql);
    say $SQL "\t);";

    say $PERLFORMAT "format $table = ";
    for (my $i=0; $i < @columns_format; $i++) { print $PERLFORMAT '@<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< '; }
    say $PERLFORMAT "";
    say $PERLFORMAT join(",", @columns_format);
    say $PERLFORMAT ".";
}

foreach my $table ( keys %structure_region ) {
    my @columns_sql=();
    my @columns_format=();
    foreach my $col ( sort keys %{$structure_region{$table}} ) {
        my $datatype;
        $col = lc($col);

        if ($col =~ /date/ ) { $datatype="date"; }
        elsif ($col =~ /guid|adrobjectid/ ) { $datatype="uuid"; }
        elsif ($col =~ /^isa/ ) { $datatype="boolean"; }
        elsif ($col =~ /(id|level|addtype)/ ) { $datatype="bigint"; }
        else { $datatype="text"; }

        push @columns_sql, "\t" . $col . " " . $datatype;
        push @columns_format, '$' . $col;
    }
        push @columns_sql, "\tregion integer";
        push @columns_format, '$' . 'region';

    say $SQL "CREATE TABLE " . lc($table) . " (";
    say $SQL join(",\n", @columns_sql);
    say $SQL "\t) PARTITION BY list (region);";

    say $PERLFORMAT "format $table = ";
    for (my $i=0; $i < @columns_format; $i++) { print $PERLFORMAT '@<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< '; }
    say $PERLFORMAT "";
    say $PERLFORMAT join(",", @columns_format);
    say $PERLFORMAT ".";
}

close($SQL);
close($PERLFORMAT);
