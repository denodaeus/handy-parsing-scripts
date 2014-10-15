#!/usr/bin/env perl -w

use strict;
use IO::Socket::INET;
use Text::CSV_XS;
use Time::Local;
use Data::Dumper;

#### CHANGE THESE VALUES ####
my $hostname = "cproxy1";
my $statsd_host = "graphite.qa.vocal-dev.com:2003";
my $statsfile = "20140920.loadtestcproxy1_vmstat.tsv";
#### CHANGE THESE VALUES ####


my $sock = IO::Socket::INET->new(
  Proto => 'udp',
  PeerAddr => $statsd_host) or die ("Could not create socket: $!");

my $csv = Text::CSV_XS->new({binary =>1, auto_diag => 1});

sub collect_data_from_file($) {
  open FILE, $statsfile or die("Could not open stats file for parsing.");
  foreach my $line (<FILE>) {
    $csv->parse($line);
    my @metric = $csv->fields();
    &compose_and_send_metric(\@metric);
  }
}

my %mon2num = qw(
  jan 0 feb 1 mar 2 apr 3 may 4 jun 5 jul 6 aug 7 sep 8 oct 9 nov 10 dec 11
);

sub compose_and_send_metric($) {
  my @values = @{$_[0]};
  my $second = (split ":", $values[3])[2];
  my $minute = (split ":", $values[3])[1];
  my $hour = (split ":", $values[3])[0];
  my $day = $values[2];
  my $month = $mon2num { lc substr($values[1], 0, 3) };
  my $year = $values[5];

  my $timestamp = timelocal($second,$minute,$hour,$day,$month,$year);
  &send_metric("r", $values[6], $timestamp);
  &send_metric("b", $values[7], $timestamp);
  &send_metric("swpd", $values[8], $timestamp);
  &send_metric("free", $values[9], $timestamp);
  &send_metric("buff", $values[10], $timestamp);
  &send_metric("cache", $values[11], $timestamp);
  &send_metric("si", $values[12], $timestamp);
  &send_metric("so", $values[13], $timestamp);
  &send_metric("bi", $values[14], $timestamp);
  &send_metric("bo", $values[15], $timestamp);
  &send_metric("in", $values[16], $timestamp);
  &send_metric("cs", $values[17], $timestamp);
  &send_metric("us", $values[18], $timestamp);
  &send_metric("sy", $values[19], $timestamp);
  &send_metric("id", $values[20], $timestamp);
  &send_metric("wa", $values[21], $timestamp);
  &send_metric("st", $values[22], $timestamp);
  &send_metric("calls", $values[23], $timestamp);
}

sub trim_to_csv($) {
  my $string = shift;
  $string =~ s/^\s+//;
  $string =~ s/\s+$//;
  $string =~ s/\s+/,/g;
  return $string;
}

sub send_metric($) {
  my ($metric, $value, $timestamp) = @_;
  my $time = $timestamp;
  my $metric_name = "loadtest.vmstat.metric.$metric";
  my $output = "$metric_name.$hostname $value $time\n";
  print $output;
  $sock->send($output);
}

&collect_data_from_file($statsfile);
