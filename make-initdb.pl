#!/usr/bin/perl

use warnings;
use strict;
use v5.10;

my $filelist=`unzip -qql $ARGV[1]`;

print $filelist;
# | awk ' $4 ~ /(AS_NORMATIVE_DOCS|CHANGE_HISTORY)/ { next; }; $4 !~ /^[[:digit:]]+\// { print }; $4 ~ /^(61|31|23|26)\// { print; } ' | awk '$4 ~ /^[[:digit:]]+/ { split($4,s,"/"); system("test ! -d ./full/"s[1]" && install -d ./full/"s[1]); }; $4 ~ /XML$/ { print("unzip xml file with sed: "$4); system("unzip -p full.zip "$4" | sed \47s/></>\\n</g\47 > ./full/"$4); next; }; { print("unzip: "$4); system("unzip full.zip "$4" -d ./full/"); }; '
#
#system()
