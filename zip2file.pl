#!/usr/bin/perl

use warnings;
use strict;
use v5.10;
use utf8;

use open qw(:std :utf8);

my $zipfile=$ARGV[0]; # путь к архиву
my $dstdir=$ARGV[1]; # куда распоковывать
my $region=$ARGV[2]; # регион

my $xmlfilelist = `unzip -qql $ARGV[0] | awk '{ print \$4 }'`;

if ( $region ne "0" ) { mkdir "$dstdir/$region" or die; }

foreach my $cf (split (/\n/,$xmlfilelist)) {
    next if ( $cf !~ /\.XML$/ );
    next if (( $cf =~ /^\d/ ) and ( $cf !~ /^$region\// ));
    next if (( $cf =~ /^AS/) and ( $region ne "0" ))  ;
    open(my $OUTPUT, "> $dstdir/$cf");
    local $/="><";
    open(my $INPUT, "unzip -p $ARGV[0] $cf |");
    binmode $INPUT, ":utf8";
    while ( <$INPUT> ) {
        chomp;
        print $OUTPUT $_ . ">\n<";
    }
    close $OUTPUT;
}
