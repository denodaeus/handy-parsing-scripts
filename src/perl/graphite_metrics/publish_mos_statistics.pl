#!/usr/bin/env perl

use strict;
use IO::Socket::INET;
use Text::CSV_XS;
use Time::Local;
use Data::Dumper;

my $hostname = "hammer";

my $statsd_host = "graphite.qa.vocal-dev.com:2003";
my $sock = IO::Socket::INET->new(
  Proto => 'udp',
  PeerAddr => $statsd_host) or die ("Could not create socket: $!");

my $csv = Text::CSV_XS->new({binary =>1, auto_diag => 1});
my ($folder) = @ARGV;
my $directory = "$folder/";

sub parse_files($) {
  opendir(DIR, $directory) or die $!;
  while (my $file = readdir(DIR)) {
    if ($file =~ m/\.csv$/) {
      open FILE, "$folder/$file" or die("Could not open stats file for parsing.");
      foreach my $line (<FILE>) {
        $csv->parse($line);
        my @metric = $csv->fields();
        &compose_and_send_metric(\@metric, $file);
      }
    }
  }
  closedir(DIR);
}

my %mon2num = qw(
  jan 0 feb 1 mar 2 apr 3 may 4 jun 5 jul 6 aug 7 sep 8 oct 9 nov 10 dec 11
);

sub compose_and_send_metric($) {
  my @values = @{$_[0]};
  my $account = $_[1];
  my $ts = $values[0];
  my $mos = $values[17];

  my ($date, $time) = (split " ", $ts);
  my ($month, $day, $year) = (split "/", $date);
  my ($hour, $minute, $second) = (split ":", $time);

  my $fixed_month = $month - 1;

  my $timestamp = timelocal($second,$minute,$hour,$day,$fixed_month,"20" . $year);
  my $account_id = substr($account, 0, -4);
  &send_metric("mos", $mos, $timestamp, $account_id);
}

sub send_metric($) {
  my ($metric, $value, $timestamp, $account_id) = @_;
  my $time = $timestamp;
  my $metric_name = "$hostname.loadtest.metric.$metric.$account_id";
  my $output = "$metric_name $value $time\n";
  print $output;
  $sock->send($output);
}

&parse_files();
