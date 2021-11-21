import json
import os
import logging
import urllib.parse
import gzip
import boto3
import io
import traceback
import re
from coralogix.handlers import CoralogixLogger

PRIVATE_KEY = os.environ.get("CORALOGIX_KEY")
APP_NAME = os.environ.get("APP_NAME")
SUB_SYSTEM = os.environ.get("SUBSYSTEM_NAME")

# Get an instance of Python standard logger.
logger = logging.getLogger("Python Logger")
logger.setLevel(logging.DEBUG)

# Get a new instance of Coralogix logger.
coralogix_handler = CoralogixLogger(PRIVATE_KEY, APP_NAME, SUB_SYSTEM)

# Add coralogix logger as a handler to the standard Python logger.
logger.addHandler(coralogix_handler)

s3 = boto3.client('s3')

def cleanup_json_structure(log_line='', list_of_fields_to_remove=[]):
    if len(log_line)>1:
        json_log=json.loads(log_line)
        for element_to_remove in list_of_fields_to_remove:
            if '.' in element_to_remove:
                removal_list = element_to_remove.split('.')
                removal_path = ''
                for removal_item in removal_list[:-1]:
                    removal_path += '[\"'+removal_item+'\"]'
                    dyn_removal_path = 'json_log'+removal_path
                try:    
                    eval(dyn_removal_path).pop(removal_list[-1],None)
                except Exception as e:
                    print(e)
            else:
                json_log.pop(element_to_remove, None)
        
        #remove empty keys from log line
        for element in json_log.copy():
            #isinstance(list(d2.keys())[0], type('str'))
            if isinstance(json_log[element], type('str')):
                if len(json_log[element]) == 0:
                    json_log.pop(element, None)

        return json.dumps(json_log)

def log_it(json_log):
    # Send message
    logger.debug(json_log)

def process_line(log_line_to_process):
    try:
        #list_of_fields_to_remove list object should contain a link of keys from the log structure
        #if you need to removed a nested field use "key.nested_key"
        json_log=cleanup_json_structure(
            log_line=log_line_to_process,
            list_of_fields_to_remove=["beat","ecs", "field_to_remove_name"] 
            )
        #split the message field

        json_log=json.loads(json_log)
        # this part is basically a preparser for the massage body if you dont wish to have it done on the rules
        json_log['message']=json_log['message'].lstrip('"')
        json_log['message']=re.sub(r"(REF|LastOriginUtcTime)=([a-zA-Z0-9]{22}|\d{1,2}\/\d{1,2}\/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+(?:AM|PM))", r'\1="\2"',str(json_log['message']).rstrip())
        json_log['message']=re.sub(r"^\"?(\d{4}-\d{2}-\d{2})\s(\d{2}:\d{2}:\d{2})(Z)", r'coralogix_custom_timestamp="\1T\2.000\3"',json_log['message'])
        regex = re.compile( r"""([a-zA-Z_\-]+)=("[^"]*"|\d+(\.\d+)?|\[("[^"]*")(,\s*(?:"[^"]*")*)?\]|)""")
        json_message_list=regex.findall(json_log['message'])
        
        for message_item in json_message_list:
            if len(message_item[0])>0: #then we have a key
                if len(message_item[1])>0: #we also have value
                    json_log[message_item[0]]=message_item[1].rstrip('"').lstrip('"') #create the json object
                else:
                    json_log[message_item[0]]=''
        if 'source' in json_log:
            json_log['logtype']=json_log['source'].split('-')[-1].rstrip('.log')

        log_it(json.dumps(json_log))
        return True
    except Exception as e:
        just_the_string = traceback.format_exc()
        logger.error(just_the_string)
        print(e)
        return False

def move_object_to_failed_folder(bucket,key):
    s3_resource = boto3.resource('s3')
    failed_key='failed/'+key
    copy_source = {
        'Bucket': bucket,
        'Key': key
    }

    #copy the whole failed object
    s3_resource.meta.client.copy(CopySource=copy_source, Bucket=bucket, Key=failed_key)
    
    # Delete the original
    response = s3.delete_object(
                Bucket = bucket,
                Key = key
            )


def lambda_handler(event, context):
    for item in event['Records']:
        bucket = item['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(item['s3']['object']['key'], encoding='utf-8')
        try:
            # head_response = s3.head_object(Bucket='bucketname', Key='keyname')
            size = (int(item['s3']['object']['size']) / 1024 / 1024)
            print(size)
            if size <= 100:
                response = s3.get_object(Bucket=bucket, Key=key)
                content = response['Body'].read()
                with gzip.GzipFile(fileobj=io.BytesIO(content), mode='rb') as fh:
                    log_lines=fh.readlines()
                    idx = 0
                    for line in log_lines:
                        decoded_line = line.decode('utf-8')    
                        try:
                            idx += 1
                            process_line(decoded_line)
                        except Exception as e:
                            #if line parsing failes isulate the line as an individual object in S3
                            failed_object_key='failed/'+str(key)+'_'+str(idx)
                            s3.put_object(
                                 Body=str(line),
                                 Bucket=bucket,
                                 Key=failed_object_key
                            )
                            just_the_string = traceback.format_exc()
                            logger.error(just_the_string)
                            print(e)
                            
                # once file is done processing, it should be removed
                response = s3.delete_object(
                    Bucket = bucket,
                    Key = key
                )
                print(response)

        except Exception as e:
            #if the whole file failes to load or parse, it will be completly sent to s3, line failures are handled seperatly.
            
            print(e)
            print('Failed parsing: {} from bucket: {}. Will place in failed folder.'.format(key, bucket))
            just_the_string = traceback.format_exc()
            logger.error(just_the_string)
            print(e)


#execution in case this is ran locally and not within lambdaa context
if __name__ == "__main__":
    testing_log_line={} #a sample log line goes here
    process_line(testing_log_line)
