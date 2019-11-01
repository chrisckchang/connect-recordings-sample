'''
Bluetax Agent Recording Redaction
Description: Grabs recordings as they come into S3, queries Dynamo to see if
there were marked pauses in the recording, and processes accordingly. 
Runtime: Python 3.7
'''
import json
import boto3
import botocore
import os
from urllib.parse import unquote_plus

def lambda_handler(event, context):
    print('<-- START -->')
    print('Incoming event: ' + str(event))
    
    print('STEP 1: Determine if redaction needs to happen')
    # Establish clients
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    # Establish the result object
    result = {
        'result' : '0',
        'note' : '0',
        'step_1_result' : 'not attempted',
        'step_2_result' : 'not attempted',
        'step_3_result' : 'not attempted'
    }
    
    # Step 1: Get the incoming event, extract records, extract required info
    # Extract the records from the incoming event
    records = event['Records']
    
    # Process for multiple records, even though there should not be.
    for i in records:
        try:
            # Extract the Bucket name
            bucket_name = i['s3']['bucket']['name']
            # Extract the key name
            key_name = i['s3']['object']['key']
            # Extract the file name
            file_name = key_name.rpartition('/')[2]
            # Extract the contact ID without the timestamp
            contact_id = (file_name.rpartition('.')[0]).partition('_')[0]
        except: 
            # Log the extraction failure
            print('STEP 1 Failed: Failed to extract the required data.')
            print('<--END-->')
            result.update({'result': 'fail'})
            result.update({'step_1_result': 'incomplete'})
            result.update({'note': 'event extraction failed'})
            return result
        
        # Grab the Tags for this recording
        try:
            object_tags = s3.get_object_tagging(
                Bucket=bucket_name,
                Key=key_name
            )
            # Log the tags
            print('Tags extracted: ' + str(object_tags))
        except: 
            # Log the tag extraction failure
            print('STEP 1 Failed: Failed to extract the tracking tags.')
            print('<--END-->')
            result.update({'result': 'fail'})
            result.update({'step_1_result': 'incomplete'})
            result.update({'note': 'tag extraction failed'})
            return result
        
        # Check Tags to see if this has already been redacted
        for tags in object_tags['TagSet']:
            if tags['Key'] == 'agent_redacted' and tags['Value'] == '1':
                print('STEP 1 COMPLETE: File already processed.')
                print('<--END-->')
                result.update({'result': 'success'})
                result.update({'step_1_result': 'complete'})
                result.update({'note': 'file already processed'})
                return result

        print('STEP 1 COMPLETE: File not processed yet.')
        result.update({'step_1_result': 'complete'})
        
        print('STEP 2: Prepare for redaction')
        # Create new tag set
        tag_set = [
            {
                'Key':'agent_redacted',
                'Value':'0'
            },
            {
                'Key':'ai_redacted',
                'Value':'0'
                
            }
        ]
        # Update the S3 Object with the new tag set
        try:
            set_new_tags = s3.put_object_tagging(
                Bucket=bucket_name,
                Key=key_name,
                Tagging={
                    'TagSet': tag_set
                }
            )
            print('Tracking tags set . . . ')
        except: 
            print('STEP 2 Failed: Failed to set new tracking tags.')
            print('<--END-->')
            result.update({'result': 'success'})
            result.update({'step_2_result': 'incomplete'})
            result.update({'note': 'failed to set tracking tags'})
            return result
        
        # Check Dynamo to see if we need to do any redaction
        # Define table
        table = dynamodb.Table('redaction_db')
        
        # Perform the search based on contact id
        check_redaction_db = table.get_item(
            Key={
                'contactId' : contact_id
            }
        )
        
        # If there is a record, set the redaction events
        if 'Item' in check_redaction_db:
            # Grab the connection timestamp
            connection_timestamp = check_redaction_db['Item']['connection_timestamp']
            # Grab the redaction record
            redaction_record = check_redaction_db['Item']['redaction_record']
            # Set the filter
            filter = ''
            # Iterate the redaction records
            for redaction_item in redaction_record:
                # Grab the start and stop times
                start_mute = (redaction_item['pause'] - connection_timestamp)/1000
                end_mute = (redaction_item['resume'] - connection_timestamp)/1000
                # Build the filter for this record
                temp_filter = 'volume=enable="between(t,{},{})":volume=0,'.format(start_mute,end_mute)
                # Append this filter to the filter collection
                filter = filter + temp_filter
            
            # Strip the ending , from the filter string
            filter = filter.rstrip(',')
            
            # Print the filter
            print('complete filter: ' + filter)
            
            # Build the redaction obj
            redaction_obj = {
                'contact_id' : contact_id,
                'bucket' : bucket_name,
                'key' : key_name,
                'filter' : filter,
                'tag_set' : tag_set,
                'file_name' : file_name
            }
            
        # If there is no redaction record, set the flag and finish
        else:
            # Set the redaction value to 1 since we don't need to redact.
            tag_set[0]['Value'] = '1'
            # Update the tracking tags on the S3 object.
            try:
                update_redaction_tags = s3.put_object_tagging(
                    Bucket=bucket_name,
                    Key=key_name,
                    Tagging={
                        'TagSet': tag_set
                    }
                )
                # Log the successful update
                print('Tracking tags updated.')
                print('STEP 2 COMPLETE: No redaction required . . .')
                print('<--END-->')
                result.update({'result': 'success'})
                result.update({'step_2_result': 'complete'})
                result.update({'note': 'no redaction required'})
                return result
            
            except: 
                # Log the failed update
                print('STEP 2 FAILED: Failed to update tracking tags.')
                print('<--END-->')
                result.update({'result': 'fail'})
                result.update({'step_2_result': 'incomplete'})
                result.update({'note': 'failed to update tracking tags'})
                return result
    
        print('STEP 2 COMPLETE: Redaction required, continue processing.')
        result.update({'step_2_result': 'complete'})
        print('STEP 3: Perform redaction.')        
        # Fire off the redaction function
        do_redaction = ffmpeg_redaction(redaction_obj)

        # Check the response
        if do_redaction == '1':
            print('STEP 3 COMPLETE: Redaction completed.')
            print('<--END-->')
            result.update({'result': 'success'})
            result.update({'step_3_result': 'complete'})
            result.update({'note': 'redaction completed successfully'})
            return result
        elif do_redaction == '2':
            print('STEP 3 FAILED: Redaction DID NOT complete.')
            print('<--END-->')
            result.update({'result': 'fail'})
            result.update({'step_3_result': 'incomplete'})
            result.update({'note': 'file redaction failed'})
            return result
        elif do_redaction == '3':
            print('STEP 3 FAILED: Redaction DID NOT complete.')
            print('<--END-->')
            result.update({'result': 'fail'})
            result.update({'step_3_result': 'incomplete'})
            result.update({'note': 'tracking tag update failed'})
            return result

# Define the ffmpeg redaction function
def ffmpeg_redaction(redaction_obj):
    # Define the clients needed in this function
    s3 = boto3.client('s3')
    
    # Print the incoming redaction object
    print('Redaction Object: ' + str(redaction_obj))
    # Define the temp object
    local_copy = '/tmp/' + redaction_obj['file_name']
    # Log the temp filename
    print('Temp object: ' + str(local_copy))
    
    print('TODO: Implement Actual Redaction Steps')
    ffmpeg_convert = '1'
    
    if ffmpeg_convert == '1':
        print('File redaction completed.')
    else:
        print('File redaction failed.')
        return('2')
    
    # Extract the redaction tags
    tag_set = redaction_obj['tag_set']
    # Set the redaction tag to 1
    tag_set[0]['Value'] = '1'
    # Log the updated tag set
    print('tag set: ' + str(tag_set))
    # Update the object tags
    try:
        write_redaction_tags = s3.put_object_tagging(
            Bucket=redaction_obj['bucket'],
            Key=redaction_obj['key'],
            Tagging={
                'TagSet': tag_set
            }
        )
        # Log the update tag update success
        print('Tracking tag update successful.')
        return('1')
    except: 
        # Log the update tag update fail
        print('Tracking tag update failed.')
        print('Tag response: ' + str(write_redaction_tags))
        return('3')