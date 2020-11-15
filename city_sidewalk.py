import os
import boto3
import pandas as pd

ACCESS_KEY = 'AKIASJWUPBMEBDXVGFUK'
SECRET_KEY = '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'

# Create AWS 3 clients 'S3','translate' and  'comprehend'
s3 = boto3.client('s3', region_name='us-east-1',
                  aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY)

translate = boto3.client('translate', region_name='us-east-1',
                         aws_access_key_id=ACCESS_KEY,
                         aws_secret_access_key=SECRET_KEY)

comprehend = boto3.client('comprehend', region_name='us-east-1',
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)

sns = boto3.client('sns',
                   region_name='us-east-1',
                   aws_access_key_id=ACCESS_KEY,
                   aws_secret_access_key=SECRET_KEY)


# This function Upload our datasets from local disc to amazon S3
def upload_objects_to_s3():
    try:
        root_path = 'C:/Users/kon-boot/Desktop/project/projectbd/city-of-bloomington'
        i = 1
        for path, subdirs, files in os.walk(root_path):
            for file in files:
                s3.upload_file(os.path.join(path, file), 'sidewalk-requests', file)
                print('file {} uploaded successfully '.format(i))
                i += 1
    except Exception as err:
        print(err)


# We have 3 CSV files so we gonna load the into one list and concat them to create our DataFrame
def create_csv_df():
    df_list = []

    list_columns = ['ticket_id', 'category', 'description',
                    'department', 'location', 'city', 'state', 'zip', 'latitude',
                    'longitude']

    response = s3.list_objects(Bucket='sidewalk-requests')
    request_response = response['Contents']
    for file in request_response:
        obj = s3.get_object(Bucket='sidewalk-requests', Key=file['Key'])
        obj_df = pd.read_csv(obj['Body'], usecols=list_columns, dtype={'description': str})
        df_list.append(obj_df)

    df_all_data = pd.concat(df_list, ignore_index=True)
    # my_dataframe = df_all_data[list_columns]
    return df_all_data


# Complaints are not always in english
# translate_complaints() iterate over all complaints and translate them to english
def translate_complaints(my_dataframe):
    my_dataframe = my_dataframe.astype({'description': 'str'})
    for index, row in my_dataframe.iterrows():
        desc = my_dataframe.loc[index, 'description']
        if desc != '':
            resp = translate.translate_text(Text=desc,
                                            SourceLanguageCode='auto',
                                            TargetLanguageCode='en')

            my_dataframe.loc[index, 'description'] = resp['TranslatedText']
    return my_dataframe


# My hunch is that that when we have a blocked sidewalk the description sentiments will be negative
def detect_sentiment(my_dataframe):
    my_dataframe = my_dataframe.astype({'description': 'str'})
    for index, row in my_dataframe.iterrows():
        desc = my_dataframe.loc[index, 'description']
        if desc != '':
            resp = comprehend.detect_sentiment(Text=desc, LanguageCode='en')

            my_dataframe.loc[index, 'sent_iment'] = resp['Sentiment']

    # my_dataframe.to_csv(r'C:\Users\kon-boot\Desktop\project\projectbd\city-of-bloomington\df_result.csv', index=False,
    # header=True)
    print(my_dataframe)
    return my_dataframe


# Count the number of barriers blocking the sidewalk
def pickup(my_dataframe):
    pickups = my_dataframe[my_dataframe['sent_iment'] == 'NEGATIVE']
    num_pickups = len(pickups)
    return num_pickups


def notification(my_dataframe, topicArn):
    for index, row in my_dataframe.iterrows():
        # Check if notification should be sent
        if (row['sentiment'] == 'NEGATIVE'):
            # Construct a message to publish to the scooter team.
            message = "Please remove barrier at {}, {}. Description: {}".format(
                row['longitude'], row['latitude'], row['description'])

            # Publish the message to the topic!
            sns.publish(TopicArn=topicArn,
                        Message=message,
                        Subject="Blocked SideWalk Alert")


if __name__ == '__main__':
    bucket = s3.create_bucket(Bucket='the-sidewalk-requests')
    upload_objects_to_s3()
    df = create_csv_df()
    _df = translate_complaints(df)
    m_df = detect_sentiment(_df)
    blocked_sw_num = pickup(m_df)
    print('You have {} a Barriers to picks up'.format(blocked_sw_num))
    # Get topic ARN for our blocked sidewalk notifications
    topicArn = sns.create_topic(Name='sidewalk_notification')['TopicArn']
    if blocked_sw_num > 10:
        notification(m_df, topicArn)
