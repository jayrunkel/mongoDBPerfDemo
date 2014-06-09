#!/usr/bin/perl -w
# load511NY_v6.pl --- Loads the 511 Traffic data into MongoDB
# Author: Jay Runkel <jayrunkel@runkelmac.home>
# Created: 10 Mar 2014
# Version: 0.01

use warnings;
use strict;

use feature qw(say);

use TryCatch;
use MongoDB;
use LWP::UserAgent;
use XML::XML2JSON;

my $dateTimeStrRegEx = '^(\d\d?)/(\d\d?)/(\d\d\d\d)\s(\d\d?):(\d\d?):(\d\d?)\s(AM|PM)$';
my $dateRegEx = '^(\d\d?)/(\d\d?)/(\d\d\d\d)$';

my ($err, $err1, $err2, $err3);

my @EVENT_FIELDS = ("EVENT_TYPE", "PAVEMENT_CONDITION", "TO_LAT", "TO_LOC_POINT", "UPDATE_NUMBER", "EVENT_ID", "EVENT_CLASS", "LOCAL_ONLY", "LAT", "FROM_LOC_POINT", "LON", "TOTAL_LANES", "WEATHER_CODITION", "TO_LON", "EST_DURATION", "CITY", "EVENT_STATE", "LANES_AFFECTED", "From_Mile_Marker", "CONSTRUCTION_TYPE", "RESPOND_ORG_ID", "FACILITY_NAME", "CLOSURE_TYPE", "EVENT_DESCRIPTION", "To_Mile_Marker", "EVENT_OTHER_DESC", "LANE_STATUS", "END_DATE", "CREATE_TIME", "CONFIRMATION_CODE", "LAST_UPDATE", "LANE_DESCRIPTION", "REPORT_ORG_ID", "ARTICLE_CODE", "START_DATE", "STATE", "LANE_DETAIL", "COUNTY", "DIRECTION");

my $XML2JSON = XML::XML2JSON->new();

my $client = MongoDB::MongoClient->new(host => 'localhost:27017');
my $db = $client->get_database( 'traffic' );
my $segCol = $db->get_collection( 'wtaSegments' );
my $linksCol = $db->get_collection( 'links' );
my $vmsCol = $db->get_collection( 'vms' );
my $eventsCol = $db->get_collection( 'events' );
my $totalRecords = 0;

sub parseDate($) {
    my $dateTimeStr = shift;

    $dateTimeStr =~ /$dateRegEx/;
    my $hour;

#    print "Parsing time: |$dateTimeStr|\n";
    
    my $result = DateTime->new(
        year   => $3 + 2000,
        month  => $1 + 0,
        day    => $2 + 0,
        hour   => 0,
        minute => 0,
        second => 0,
        time_zone => 0);

#    print "Parsed string: $dateTimeStr to @{[$result->DateTime()->mdy()]}\n";
    
    return $result;


}



sub parseTime($) {
    my $dateTimeStr = shift;

    my $hour;
    my $result;
        
    if ($dateTimeStr =~ /$dateTimeStrRegEx/ ) {


        #    print "Parsing time: |$dateTimeStr|\n";
    
        if (($7 eq 'AM') && ($4 == 12)) {
            $hour = 0;
        }
        elsif (($7 eq 'PM') && ($4 != 12)) {
            $hour = $4 + 12;
        }
        else {
            $hour = $4 + 0;
        }
        
        $result = DateTime->new(
            year   => $3 + 2000,
            month  => $1 + 0,
            day    => $2 + 0,
            hour   => $hour,
            minute => $5 + 0,
            second => $6,
            time_zone => 0);
    }
    else {
       $result = parseDate($dateTimeStr); 
    }

#    print "Parsed string: $dateTimeStr to @{[$result->DateTime()->mdy()]}\n";
    
    return $result;
}




sub getWTASegCondFeedData($) {
    my $ua = shift;
    
        
    my $response = $ua->post('https://165.193.215.51/XMLFeeds/createXML.aspx',
                           ['username' => 'jay.runkel@mongodb.com',
                            'password' => 'jr0314@',
                            'dataType' => 'wtastatus']);
    my $numSegments = 0;
        
    if ($response->is_success) {
       my $segmentData = $response->decoded_content;
       return 0 if length($segmentData) < 200;
       my $tempStr = substr($segmentData, 46);
       $tempStr =~ s/^\n//;
       my @segments = split(/<WTASegment>/, $tempStr);
       shift(@segments) if $segments[0] eq '';   # if the first segment is empty, remove it.

       $numSegments = @segments;
       my $count = 1;
       
       foreach my $segment (@segments) {
           my $segmentXML = '<WTASegment>';
           my $segmentJSON;
           my $segmentObj;
           my $segRecord = {};
           my $segmentId;
           my $updateTime;
           my $linkId;
           my $weather;
           my $pavement;
           
           if ($count == $numSegments) {
               $segmentXML = $segmentXML . substr($segment, 0, length($segment) - 26);
    #           print "Last line: $segment\n";
           }
           else {
               $segmentXML = $segmentXML . $segment;
           }
           
           $count++;
#           print "$segmentXML\n";
           try {
	       $segmentJSON = $XML2JSON->convert($segmentXML);
#            print "$linkJSON\n";
	       $segmentObj = $XML2JSON->xml2obj($segmentXML);
	       $linkId = $segmentObj->{'WTASegment'}->{'LINK_ID'}->{'$t'};
	       $updateTime = parseTime($segmentObj->{'WTASegment'}->{'LAST_UPDATE'}->{'$t'});
	       $pavement = $segmentObj->{'WTASegment'}->{'PAVEMENT_STATUS'}->{'$t'};
	       $weather = $segmentObj->{'WTASegment'}->{'WEATHER_STATUS'}->{'$t'};
           
           #$segRecord->{'update'} = $updateTime;
	       $segRecord->{'status'} = $segmentObj->{'WTASegment'}->{'OVERALL_STATUS'}->{'$t'};
	       $segRecord->{'weather'} = $weather if defined $weather;
	       $segRecord->{'pavement'} = $pavement if defined $pavement;
	       $segRecord->{'_id'} = {'WTASegment' => $linkId, 'update' => $updateTime};
           
	       $segCol->update({'_id' => {'WTASegment' => $linkId, 'update' => $updateTime}}, $segRecord, {'upsert' => 1});
#           $segCol->insert($segmentObj);
	   }
	   catch ($err) {
		print "Error processing WTA Segment: $segmentXML\n";
		print "Error: $err\n";
	   }
       }
   }
    else {
        print "$response->status_line\n";
        return 0;
    }

    return $numSegments;
}


sub getLinksFeedData($) {
    my $ua = shift;

    my $response = $ua->post('https://165.193.215.51/XMLFeeds/createXML.aspx',
                        ['username' => 'jay.runkel@mongodb.com',
                         'password' => 'jr0314@',
                         'dataType' => 'links']);

    # my $aborted =$response->header('Client-Aborted');
    # print "Client-Aborted: $aborted\n";

#    say $response->headers->as_string;    
    
    my $numLinks = 0;
    
    if ($response->is_success) {
        my $linkData = $response->decoded_content;
        return 0 if length($linkData) < 200;
        
        my @links = split(/<link>/, substr($linkData, 34));
        $numLinks = @links;
        my $count = 1;
            
        foreach my $link (@links) {
            my $linkXML = '<link>';
     #       my $linkJSON;
            my $linkObj;
            my $linkRecord = {};
            my $linkId;
            my $updateTime;
            
            
            
            if ($count == $numLinks) {
                $linkXML = $linkXML . substr($link, 0, length($link) - 8);
#               print "Last line: $link\n";
            }
            else {
                $linkXML = $linkXML . $link;
            }
            
            $count++;
#           $linkJSON = $XML2JSON->convert($linkXML);
#           print "$linkJSON\n";
	    
	    try {
		$linkObj = $XML2JSON->xml2obj($linkXML);
		$linkId = $linkObj->{'link'}->{'LINK_ID'}->{'$t'};
		$updateTime = parseTime($linkObj->{'link'}->{'LAST_UPDATE'}->{'$t'});
    
		$linkRecord->{'update'} = $updateTime;
		$linkRecord->{'travelTime'} = $linkObj->{'link'}->{'CURRENT_TRAVEL_TIME'}->{'$t'};
		$linkRecord->{'speed'} = $linkObj->{'link'}->{'CURRENT_SPEED'}->{'$t'};
		$linkRecord->{'_id'} = {'link' => $linkId, 'update' => $updateTime};
            
		$linksCol->update({'_id' => {'link' => $linkId, 'update' => $updateTime}}, $linkRecord, {'upsert' => 1});
	    }
	    catch ($err1) {
		print "Error processing link: $linkXML\n";
		print "Error: $err1\n";
	    }
        }
    }
    else {
        print "$response->status_line\n";
        return 0;
    }

    return $numLinks;
}

sub getVMSFeedData($) {
    my $ua = shift;

    my $response = $ua->post('https://165.193.215.51/XMLFeeds/createXML.aspx',
                        ['username' => 'jay.runkel@mongodb.com',
                         'password' => 'jr0314@',
                         'dataType' => 'vms']);
    my $numLinks = 0;
    
    if ($response->is_success) {
        my $linkData = $response->decoded_content;
        return 0 if length($linkData) < 200;       #an error occurred. Return 0;
        
        my @links = split(/<\/vms>/, substr($linkData, 27));
        $numLinks = @links;
        my $count = 1;
            
        foreach my $link (@links) {
            my $linkXML;
            my $linkObj;
            my $linkRecord = {};
            my $linkId;
            my $updateTime;
            my $status;
            
            
            $linkXML = $link . '</vms>';
            
            if ($count != $numLinks) { # Last line is a garbage line. Do nothing.
                $linkObj = $XML2JSON->xml2obj($linkXML);

 #              $vmsCol->insert($linkObj);
                 
                $linkId = $linkObj->{'vms'}->{'@id'};
                $updateTime = parseTime($linkObj->{'vms'}->{'last_update'}->{'$t'});
                $status = $linkObj->{'vms'}->{'message'}->{'$t'};
    
                $linkRecord->{'status'} = $linkObj->{'vms'}->{'status'}->{'$t'};
                $linkRecord->{'name'} = $linkObj->{'vms'}->{'@name'};
                $linkRecord->{'status'} = $status if defined $status;
                $linkRecord->{'coord'} = [$linkObj->{'vms'}->{'@lon'}, $linkObj->{'vms'}->{'@lat'}];
                $linkRecord->{'location'} = $linkObj->{'vms'}->{'@location'};
                $linkRecord->{'owner'} = $linkObj->{'vms'}->{'@owner_name'};
                $linkRecord->{'_id'} = {'link' => $linkId, 'update' => $updateTime};
            
                $vmsCol->update({'_id' => {'link' => $linkId, 'update' => $updateTime}}, $linkRecord, {'upsert' => 1});
             }
            $count++;

        }
        
    }
    else {
        print "$response->status_line\n";
        return 0;
    }

    return $numLinks;
}

sub buildEventField($$$) {
    my $record = shift;
    my $jsonObj = shift;
    my $field = shift;
    my $value = $jsonObj->{'event'}->{$field}->{'$t'};

    if ($value ne "NULL") {
        if ( $field =~ m/(_date|time|_update)/i ) {
#           print "Parsing time on field $field value: $value\n";
            $record->{$field} = parseTime($value);            
        }
        else {
            $record->{$field} = $value;
        }
    }    
}


sub getEventFeedData($) {
    my $ua = shift;

    my $response = $ua->post('https://165.193.215.51/XMLFeeds/createXML.aspx',
                        ['username' => 'jay.runkel@mongodb.com',
                         'password' => 'jr0314@',
                         'dataType' => 'events']);
    my $numLinks = 0;

    if ($response->is_success) {
        my $linkData = $response->decoded_content;
        return 0 if length($linkData) < 200;       #an error occurred. Return 0;
        
        my @links = split(/<\/event>/, substr($linkData, 29));
        $numLinks = @links;
        my $count = 1;
            
        foreach my $link (@links) {
            my $linkXML;
            my $linkObj;
            my $linkRecord = {};
            my $linkId;
            my $updateTime;
            my $status;

#           print "$link\n";
            
            $linkXML = $link . '</event>';
            
            if ($count != $numLinks) { 
                $linkObj = $XML2JSON->xml2obj($linkXML);

                foreach my $f (@EVENT_FIELDS) {
                    buildEventField($linkRecord, $linkObj, $f);
                }

                $linkId = $linkRecord->{'EVENT_ID'};
                delete $linkRecord->{'EVENT_ID'};
                $updateTime = $linkRecord->{'LAST_UPDATE'};
                delete $linkRecord->{'LAST_UPDATE'};
                 
                $linkRecord->{'_id'} = {'event' => $linkId, 'update' => $updateTime};
            
                $eventsCol->update({'_id' => {'event' => $linkId, 'update' => $updateTime}}, $linkRecord, {'upsert' => 1});
            }
            $count++;

        }
        
    }
    else {
        print "$response->status_line\n";
        return 0;
    }

    return $numLinks;
}


sub printFeedStats() {
    my $numVMS = $vmsCol->count();
    my $numLinks = $linksCol->count();
    my $numWTASegs = $segCol->count();
    my $numEvents = $eventsCol->count();
    
    my $total = $numVMS + $numLinks + $numWTASegs + $numEvents;
    my $delta = $total - $totalRecords;
    $totalRecords = $total;
    
    print "TOTALS [$total/$delta] - vms: $numVMS, links: $numLinks, WTA Segments: $numWTASegs, Events: $numEvents\n";
}


sub processFeeds($) {
    my $ua = shift;

    my $numProcessed;
    
    while (1) {
        print "Processing WTASegments...";
        $numProcessed = getWTASegCondFeedData($ua);
        print "[$numProcessed segments]\n";
        
        sleep(10);
        print "Process Links...";
        $numProcessed = getLinksFeedData($ua);
        print "[$numProcessed links]\n";

        sleep(10);
        print "Process VMS...";
        $numProcessed = getVMSFeedData($ua);
        print "[$numProcessed VMS]\n";

        sleep(10);
        print "Process Events...";
        $numProcessed = getEventFeedData($ua);
        print "[$numProcessed Events]\n";

        printFeedStats();
        sleep(10);
    }
}



MAIN: {
    my $ua = LWP::UserAgent->new;
    $ua->timeout(10);
    $ua->env_proxy;
    $ua->ssl_opts('verify_hostname' => 0);
    $ua->show_progress(1);

    processFeeds($ua);
#    getEventFeedData($ua);
}

__END__

=head1 NAME

load511NY.pl - Describe the usage of script briefly

=head1 SYNOPSIS

load511NY.pl [options] args

      -opt --long      Option description

=head1 DESCRIPTION

Stub documentation for load511NY.pl, 

=head1 AUTHOR

Jay Runkel, E<lt>jayrunkel@runkelmac.homeE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2014 by Jay Runkel

This program is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.2 or,
at your option, any later version of Perl 5 you may have available.

=head1 BUGS

None reported... yet.

=cut
