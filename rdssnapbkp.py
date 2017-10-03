#!/usr/bin/env python
#Maintainer - Umesh Pandey ucpandey2@gmail.com 
#any one free to use modify and redistribute this utility - This tools is to create manual snapshot of an Oracle RDS &  manage
#retnetion  
#Usage : The script to be run from an EC2 machine that has assigned IAM role to create RDS snapshot and ability to connect RDS .
#Update the appropriate region and provide RDS DB name in the script 
import boto.rds
import pprint
import time
import re
import os
import pdb
import boto.rds2
import glob
import ntpath
import datetime

def touch(fname, times=None):
        with file(fname, 'a'):
                os.utime(fname, times)


def getDay():
	from datetime import datetime
	return datetime.now().strftime('%C-%B')
	
def createsnapshots(backupdb,backupschedule,directory):
	if not os.path.exists(directory):
        		os.makedirs(directory)
	today = getDay()
	if today in backupschedule.keys():
		bktype = backupschedule[today]
		conn = boto.rds.connect_to_region("us-west-2")
		print("Running backup :"+ today )
		dbs = backupdb.keys()
		instances = conn.get_all_dbinstances()
		for db in instances:
        		# Backup only main database;Skip read replicas
        		dbname = str(db)[11:]
        		if dbname in dbs:
				#<DBNAME_BACKUP TYPE_DATE_M|Y> [ e.g. AgilePRD_SNAP_08202015_M| AgilePRD_EXP_08202015_M] 
                        	backup_name = dbname + "-SNAP-"+ (time.strftime("%m%d%Y-%H%M")) +"-" + bktype
				backup_name = backup_name.lower()
                        	touch(directory +'/'+ backup_name + '.bak')
                        	print "Backing up db... " + backup_name
                        	db.snapshot(backup_name)
	else:
		print "No backup scheduled to run today " + today 

def purgeoldsnapshots(backupdb,backupdir):


	# Get all the filenames
	dirfiles = glob.glob(backupdir+"/*")
	filenames = []

	# Clear up extensions
	for files in dirfiles:
        	file = ntpath.basename(files).split('.')[0] #Remove .bak extension
        	filenames.append(file)


	conn = boto.rds2.connect_to_region("us-west-2") #AWS region to be updated . The script can run from a host that has assigned IAM role to create SNAPSHOT
	snapshots = conn.describe_db_snapshots()
	list_snapshots = snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots']
	for snap in list_snapshots:
        	if snap['SnapshotType'] == 'automated':
                	continue
        	dbs_id = snap['DBSnapshotIdentifier']
        	if dbs_id in filenames:
			snapattrib = dbs_id.split("-")
			dbname = snapattrib[0]
			bktype = snapattrib[4].upper()
			bkexpiration =backupdb[dbname]['retention'][bktype]
        		delta_date = datetime.timedelta(days=bkexpiration)
        		acceptable_date = datetime.datetime.now() - delta_date
			datestr = snapattrib[2]+"-" + snapattrib[3]
                	date_object = datetime.datetime.strptime(datestr, '%m%d%Y-%H%M')
                	if (date_object < acceptable_date):
                        	# Delete it here
                        	print "Deleting expired snapshot... " + dbs_id
                        	conn.delete_db_snapshot(dbs_id)
                        	# Remove the files
                        	os.remove(backupdir+'/' + dbs_id)

			else:
				print "No snapshot backup for " + dbname + " deleted "

if(__name__ == "__main__"):
	#Provide a location to create .bak zero byte file
	backup_file_dir='/vol01/scripts/rds_snapshots' # location where the .bak file to be created . This file is used to identify the snapshot age
	#below dict will need to be modfied to add/delete/update retention of the RDS DB backup . Add RDS DB to take backup
	#retention 183 days for Monthly backup 
	#retention 2555 days for yearly backup
	#yearly backup is taken on 1st feb
	backupdb = {'<DB Name>':{'retention':{'M':183,'Y':2555}}} #pass the RDS DB name 
	print "Starting manual snapshot backup of " + str(backupdb.keys()) + " RDS databases "
	backupschedule = {
				'1-January'	:'M',
				'1-February'	:'Y',
				'1-March'	:'M',
				'1-April'	:'M',
				'1-May'		:'M',
				'1-June'	:'M',
				'1-July'	:'M',
				'1-August'	:'M',
				'1-September'	:'M',
				'1-October'	:'M',
				'1-November'	:'M',
				'1-December'	:'M'
			}
	createsnapshots(backupdb,backupschedule,backup_file_dir)
	print "Starting expired manual snapshot backup file deletion "
	purgeoldsnapshots(backupdb,backup_file_dir)

