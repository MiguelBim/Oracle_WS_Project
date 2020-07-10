import pandas as pd
import json
import urllib.request

#------------------------------------------------ DATA READ AND STORAGE -----------------------------------------------#

# Function created to load tables into dataframes
def read_custom_db_tables():

    # Data frame for -> W_ETL_DASH_WEBSERVICE_MAP
    mappings_df = pd.read_csv(r'C:\Users\miojeda\Google Drive\Projects\BD-Oracle\WS_Mapping.csv')
    # Data frame for -> W_ETLDASH_LOOKUP
    lookup_df = pd.read_csv(r'C:\Users\miojeda\Google Drive\Projects\BD-Oracle\WS_Lookup.csv')

    return mappings_df, lookup_df


# Function created to save WS response in csv files
def save_ws_response_to_csv(data, csv_name):

    # try:
    current_df = pd.DataFrame(data)
    history_df = pd.read_csv(csv_name)
    # merged_df = history_df.append(current_df, sort=True)
    merged_df = pd.concat([history_df, current_df], ignore_index=True, sort=True)
    new_entries = merged_df.drop_duplicates(subset="SESS_NO", keep=False)
    new_entries.to_csv(csv_name, header=False, index=False, mode='w', encoding='utf-8')

    print("\t-> File '" + csv_name + "' was correctly saved.")
    # except:
    #     print("\t-> File '" + csv_name + "' was NOT saved. Please check.")
    quit()


#------------------------------------------------ WEB SERVICES REQUESTS -----------------------------------------------#
# Function created to obtain URL for each environment and each process
def get_ws_url(environment, code):

    mappings_df = pd.read_csv(r'C:\Users\miojeda\Google Drive\Projects\BD-Oracle\WS_Mapping.csv')

    # Prints all values for those columns
    # print(mappings_df[['CODE','WS_URL']])

    # URL value extract from given conditions
    try:
        df_result = mappings_df.loc[(mappings_df['ENVIRONMENT'] == environment) & (mappings_df['CODE'] == code), ['WS_URL','INACTIVE_FLG']]
        search_results = df_result.values.tolist()[0]
        url_flag = search_results[1]
        if url_flag == 'N':
            url_to_be_used = search_results[0]
            return url_to_be_used
        else:
            print("\tThe retrieved URL is no longer valid, please check.")
            quit()
    except:
        print("\tNo URL was found with the given conditions:\n" + "\t" + environment +"\n" + "\t" + code + "\n\n\tPlease check and retry")
        quit()


# Function created to request LPs schedules
def consume_schedules_ws(url):

    print("\tURL to be used: " + url)

    try:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
    except:
        print("\tWeb service request for: " + url + " was not completed")
        quit()

    return data


# Function created to consume main WS
def consume_main_info_ws(url, days_to_retrieve_data):

    complete_url = url + str(days_to_retrieve_data)
    print("\t-> URL to be used for sessions history: " + complete_url)
    print("\t-> Process for LP general details info retrieval: STARTED")
    try:
        response = urllib.request.urlopen(complete_url)
        data = json.loads(response.read())
        print("\t-> Process for LP general details info retrieval: COMPLETED")
    except:
        print("\t-> Process for LP general details info retrieval: FAILED")
        quit()

    return data



#-------------------------------------------------- DATA PROCESSING ---------------------------------------------------#

# Function created to extract records from the json response and return a dictionary
def extract_json_response(json_input):

    data_dict = json_input.get('items')
    # for schedule in schedules:
    #     print(schedule)
    return data_dict


# Function created to extract failed ETL jobs sessions
def get_failed_records(history_data_json):

    history_data_df = pd.DataFrame(history_data_json)
    history_data_df.columns = history_data_df.columns.str.upper()
    failed_sessions = history_data_df.loc[history_data_df['STATUS'] == 'Failed', ['SESS_NO', 'NAME', 'NB_RUN', 'START_DATE', 'END_DATE']]

    # Formatting date columns and changing them from UTC to PST time
    # START_DATE
    failed_sessions['START_DATE'] = failed_sessions['START_DATE'].map(lambda x: x.rstrip('.0'))
    failed_sessions['START_DATE'] = pd.to_datetime(failed_sessions['START_DATE'])
    failed_sessions['START_DATE'] = failed_sessions['START_DATE'].dt.tz_localize('GMT').dt.tz_convert('America/Los_Angeles').dt.tz_localize(None)
    # END_DATE
    failed_sessions['END_DATE'] = failed_sessions['END_DATE'].map(lambda x: x.rstrip('.'))
    failed_sessions['END_DATE'] = pd.to_datetime(failed_sessions['END_DATE'])
    failed_sessions['END_DATE'] = failed_sessions['END_DATE'].dt.tz_localize('GMT').dt.tz_convert('America/Los_Angeles').dt.tz_localize(None)

    return failed_sessions


def get_failed_records_info(environment, failed_records_df):

    url_code = 'LP_LOG'
    ws_url = get_ws_url(environment, url_code)
    failed_records_list = failed_records_df.values.tolist()
    errors_details_list = []
    print("\t-> Base URL to be used for errors details: " + ws_url + "<session_id>" + "/<nb_run>")

    # Web service for errors details is consume for every failed record
    print("\t-> Process for errors details retrieval: STARTED")
    try:
        for record in failed_records_list:
            complete_url = ws_url +  record[0] + '/' + str(record[2])
            response = urllib.request.urlopen(complete_url)
            data = json.loads(response.read())
            error = data.get('items')[0]
            errors_details_list.append(error)
        print("\t-> Process for errors details retrieval: COMPLETED")
    except:
        print("\t-> Process for errors details retrieval: FAILED")


    # Process to create new data frame with all the details (including error message)
    errors_details_df = pd.DataFrame(errors_details_list)
    failed_records_df = failed_records_df.reset_index()
    # Appending column extracted
    failed_records_df['ERROR_MESSAGE'] = errors_details_df['error']
    # Removing NB_RUN unused column
    del failed_records_df['NB_RUN']

    # Changing indexed column to SESSION_ID and sorting DF
    # failed_records_df.set_index('SESS_NO')
    # del failed_records_df['index']
    # failed_records_df.sort_index()

    return failed_records_df


# Function created to detect long running sessions
def detect_long_running_session():

    return


#--------------------------------------------------- MAIN FUNCTION ----------------------------------------------------#
def main():

    environment = 'CDPAP-OCI'
    days_to_retrieve_data = 2

    ###### SCHEDULES WS CONSUME ######
    # print("\nLPs Schedules process started:")
    #
    # # Get URL to be used
    # cdpap_schedule_url = get_ws_url(mappings_df, environment, 'SCHEDULE')
    #
    # # WS response processing
    # response = consume_schedules_ws(cdpap_schedule_url)
    # schedules_data = extract_json_response(response)
    #
    # # create csv file
    # file_name = 'schedules.csv'
    # save_ws_response_to_csv(schedules_data, file_name)



    ###### MAIN WS CONSUME ######
    print("\nLP general details process execution:")

    # Get URL to be used
    cdpap_history_url = get_ws_url(environment, 'HISTORY')

    # WS response processing

    response = consume_main_info_ws(cdpap_history_url, days_to_retrieve_data)
    history_data = extract_json_response(response)

    print("\nLP errors details process execution:")
    # bad records extraction
    history_failed_records_df = get_failed_records(history_data)
    # ws_url = get_ws_url(environment, 'LP_LOG')
    # print(ws_url)
    failed_sessions = get_failed_records_info(environment, history_failed_records_df)

    # create csv file
    file_name = 'CDP_failed_records.csv'
    save_ws_response_to_csv(failed_sessions, file_name)


if __name__ == '__main__':
    main()
