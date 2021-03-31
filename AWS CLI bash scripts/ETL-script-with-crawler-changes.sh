#!/bin/bash

#RDS settings
rds_uuid=$(openssl rand -hex 5) #random generated id for process name
snapshot_name="companyname-"$rds_uuid"-reporting" #snapshot name
instance_name="companyname-reporting" #db-instance name

#S3 settings
s3_report_bucket="s3://companynamereporting"
s3_archive_bucket="s3://companynamereporting-archive/"$(date +%Y/%m/%d)

#Export to S3 settings
export_task_name="companynamereporting-"$(date +%Y-%m-%d)"-"$rds_uuid"-migration"
export_task_bucket_name="companynamereporting"
iam_role="iam-role"
kms_key_id="kms-key-id"

#Glue settings
crawlers=()
glue_jobs=("s3snapshot_tbl1_to_redshift" "s3snapshot_tbl2_to_redshift" "s3snapshot_tbl3_to_redshift" "s3snapshot_tbl4_to_redshift")
declare -A glue_job_ids=()
declare -A finishedGlueJobs=()

#START PROCESS
start_datetime=$(date)
echo "ETL starts now."

#RDS - create rds snapshot
aws rds create-db-snapshot --db-snapshot-identifier $snapshot_name --db-instance-identifier $instance_name
echo "Started creating manual snapshot: "$snapshot_name

#RDS - check rds snapshot
rds_snapshot_progress=$(aws rds describe-db-snapshots --db-snapshot-identifier $snapshot_name --db-instance-identifier $instance_name | grep -o '"PercentProgress":[^"]*' | grep -o -E '[0-9]+')
while [ $rds_snapshot_progress != '100' ]
do
	echo "Current Progress = $rds_snapshot_progress%"
	sleep 180 #update every 3 minutes
	rds_snapshot_progress=$(aws rds describe-db-snapshots --db-snapshot-identifier $snapshot_name --db-instance-identifier $instance_name | grep -o '"PercentProgress":[^"]*' | grep -o -E '[0-9]+')
done

#RDS - finish rds snapshot
echo "$snapshot_name (RDS manual snapshot) is created."

#S3 - archive s3 bucket
aws s3 mv $s3_report_bucket $s3_archive_bucket --recursive

#S3 - finish archive s3 bucket
echo "S3 archive is complete."
echo "Archived location: $s3_archive_bucket"

#RDS - start export to S3 task
aws rds start-export-task --export-task-identifier $export_task_name --source-arn arn:aws:rds:us-east-1:awsaccountid:snapshot:$snapshot_name --s3-bucket-name $export_task_bucket_name --iam-role-arn arn:aws:iam::awsaccountid:role/$iam_role --kms-key-id arn:aws:kms:us-east-1:awsaccountid:key/$kms_key_id

#RDS - finish export to s3 task
echo "Started export-to-s3-task: "$export_task_name

#RDS - check export to s3 task
rds_export_progress=$(aws rds describe-export-tasks --export-task-identifier $export_task_name | grep -o '"PercentProgress":[^"]*' | grep -o -E '[0-9]+')
while [ $rds_export_progress != '100' ]
do
	echo "Current Progress = $rds_export_progress%"
	sleep 300 #update every 5 minutes
	rds_export_progress=$(aws rds describe-export-tasks --export-task-identifier $export_task_name | grep -o '"PercentProgress":[^"]*' | grep -o -E '[0-9]+')
done

#RDS - finish export to s3 task
echo "$export_task_name is complete."

#Glue - create glue crawler/s
aws lambda invoke --function-name ETL-createCrawlers newCrawlers.json

#Glue - start glue crawler/s
readCrawlers=$(cat newCrawlers.json)
for crawler in $readCrawlers; do crawlers+=$(echo $crawler" " | tr -d ["",] | sed -e 's/^"//' -e 's/"$//'); done
for crawler in $crawlers; do aws glue start-crawler --name=$(echo $crawler | sed -e 's/"//') | echo "Started crawler: $(echo $crawler | sed -e 's/"//')"; done
rm newCrawlers.json

#Glue - check glue crawler/s
declare -A finishedCrawlers=()
while [ ${#finishedCrawlers[@]} != ${#crawlers[@]} ]
do
	for crawler in "${crawlers[@]}"
	do
		crawler_status=$(aws glue get-crawler --name=$(echo $crawler | sed -e 's/"//') | grep -o '"State*"\s*:\s*"[^"]*"' | sed 's/^ *//;s/.*: *"//;s/"//')
		echo $(echo $crawler | sed -e 's/"//')": "$crawler_status
		if ! [ -v finishedCrawlers[=$(echo $crawler | sed -e 's/"//')] ]
		then
			if [ "$crawler_status" == "READY" ]
			then
				finishedCrawlers[=$(echo $crawler | sed -e 's/"//')]="READY"
			fi
		else
			sleep 120 #wait 2 mins
		fi
	done
done

#Glue - finish glue crawlers
echo "All related crawlers are complete."

#Glue - start glue job/s
for job in "${glue_jobs[@]}"
do
	jobrunid=$(aws glue start-job-run --job-name=$job | grep -o '"JobRunId*"\s*:\s*"[^"]*"' | sed 's/^ *//;s/.*: *"//;s/"//')
	echo "Started Glue Job: $job"
	glue_job_ids["$job"]="$jobrunid"
	echo "RunID: $jobrunid"
done

#Glue - check glue job/s
while [ ${#finishedGlueJobs[@]} != ${#glue_jobs[@]} ]
do
	for job in "${glue_jobs[@]}"
	do
		job_status=$(aws glue get-job-run --job-name="$job" --run-id=${glue_job_ids[$job]} | grep -o '"JobRunState*"\s*:\s*"[^"]*"' | sed 's/^ *//;s/.*: *"//;s/"//')
		if ! [ -v finishedGlueJobs[$job] ]
		then
			if [ "$job_status" != "RUNNING" ]
			then
				finishedGlueJobs[$job]="$job_status"
				echo ${finishedGlueJobs[$job]}
			else
				echo "$job: $job_status"
				sleep 120 #wait 2 minutes
			fi
		else
			echo "$job: $job_status"
			sleep 120 #wait 2 minutes
		fi
	done
done

#ETL finishes
echo "All Glue jobs are complete."
echo "ETL started on "$start_datetime"."
echo "ETL completed on $(date)."
