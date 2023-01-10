import logging
import os, csv
import requests, datetime, time
from r2essentials.context import RContext, make_extra, make_context
import uuid, json
from threading import Thread, current_thread
from functools import partial

def get_from_env(key: str, fallback: str) -> str:
    try:
        return os.environ[key]
    except KeyError:
        return fallback

POLICY_SET_URL = get_from_env("POLICY_SET_URL", "https://internal.prd.rightrev.cloud/apis/policy-sets")
RECORDER_DEV_URL = get_from_env("RECORDER_URL", "https://internal.prd.rightrev.cloud/apis/recorder")
LEGAL_ENTITY_END_POINT = POLICY_SET_URL + "/v1/policy/legal-entity"
RECORDER_URL = RECORDER_DEV_URL + "/v1/revenue_contract_ids"
RECORDER_FETCH_URL = RECORDER_DEV_URL + "/v1/revenue_contract"

def new_uuid_as_string():
    return str(uuid.uuid4())

def make_header_info(context:RContext, user_roles:str):
    return {'X-R2-USER-ID': context.user_context.user_id, 'x-r2-tenant-id': context.tenant_context.tenant_id, 'x-r2-user-roles': user_roles}

def write_header_to_csv(fields, dest_path):
     with open(dest_path, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)

def write_to_csv(rows, dest_path):
    with open(dest_path, 'a') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(rows)

def write_to_path(temp_file_name, path='/Users/thiyageshdhandapani/Documents/Rightrev/Holmes/src/lib/prod_scripts/result_files'):
    today_date = str(datetime.datetime.today()).split(' ')[0]
    backup_time = str(datetime.datetime.now())
    folder = f"{path}/{today_date}/{backup_time}"
    os.makedirs(folder)
    dest_path = f"{folder}/{temp_file_name}.csv"
    with open(dest_path, "w+") as fin:
        json.dump(temp_file_name, fin)

def fetch_rc_ids(context:RContext):
    total_rcs = 0
    final_rc_list = []
    page_str = ''
    rc_filter_cnt = get_from_env('RC_FILTER_COUNT', 'ALL')
    while True:
        response_data = get_policy_data(context, RECORDER_URL, page_str=page_str)
        if not response_data:
            break
        rc_dict = response_data.get('items', [])
        page_str = response_data.get('page')
        rc_ids = [i.get('rc_id') for i in rc_dict]
        rc_ids, msg = sort_rcs(rc_ids, rc_filter_cnt)
        print(msg)
        total_rcs += len(rc_ids)
        final_rc_list.extend(rc_ids)
    print(f'Total_rcs_fetched: {total_rcs}')
    return final_rc_list

def get_rc_json(context:RContext, rc_id):
    rc_url = f"{RECORDER_FETCH_URL}/{rc_id}"
    rc_json = get_policy_data(context, rc_url)
    return rc_json

def get_duplicate_release_actions(context:RContext, rc_id, dest_path):
    rc_json = get_rc_json(context, rc_id)
    rc_attributes = rc_json['rc_attributes']
    identifiers = rc_json['identifiers']
    legal_entity = rc_attributes['legal_entity']
    rc_guid = identifiers['revenue_contract_guid']
    contract_details = rc_json['contract_details']
    order_lines = contract_details['order_lines']
    dup_ord_dict = {}
    for ord in order_lines:
        for ord_id, d in ord.items():
            release_actions = d.get('release_actions')
            if not release_actions:
                continue
            dup_dict = fetch_duplicate(release_actions)
            if dup_dict:
                dup_ord_dict[ord_id] = dup_dict
    if dup_ord_dict:
        rows = get_rows(legal_entity, rc_id, rc_guid, dup_ord_dict)
        write_to_csv(rows, dest_path)
    print(f"Completed processing for {rc_id}")

def get_rows(legal_entity, rc_id, rc_guid, dup_ord_dict):
    rows = []
    for ord_id, dup_dict in dup_ord_dict.items():
        for schedule_type, l in dup_dict.items():
            ra_guid = l[0]
            count = l[1]
            rows.append([legal_entity, rc_id, rc_guid, ord_id, schedule_type, ra_guid, count])
    return rows

def fetch_duplicate(release_actions):
    schedule_dict = {}
    dup_dict = {}
    for ra in release_actions:
        schedule_type = ra.get('schedule_type')
        release_action_guid = ra.get('release_action_guid')
        if schedule_dict.get(schedule_type):
            if schedule_dict[schedule_type] == release_action_guid:
                dup_dict = update_dup_dict(dup_dict, schedule_type, release_action_guid)
        else:
            schedule_dict[schedule_type]= release_action_guid
    return(dup_dict)

def update_dup_dict(dup_dict, schedule_type, release_action_guid):
    if dup_dict.get(schedule_type):
        count = dup_dict[schedule_type][1]
        dup_dict[schedule_type][1] = count+1
    else:
        dup_dict[schedule_type] = [release_action_guid, 1]

    return dup_dict

def sort_rcs(rc_ids, rc_filter_cnt):
    rc_ids = [i.split('-')[-1] for i in rc_ids]
    rc_ids.sort(key=int)
    rc_ids = ['RC-' + i for i in rc_ids]
    rc_ids = rc_ids[::-1]
    msg = "processing for all Rcs"
    if rc_filter_cnt != 'ALL':
        msg = f"Filtering the latest {rc_filter_cnt} RCs since RC_FILTER_COUNT env is set to {rc_filter_cnt}"
        rc_ids = rc_ids[0:rc_filter_cnt]
    return rc_ids, msg

def get_policy_data(context:RContext, service_end_pt, page_str='', retry=3):
    excp_msg = ''
    while retry > 0:
        try:
            header_dict = make_header_info(
                context, 'configuration:policy:orders-ingestion:read')
            if page_str:
                service_end_pt += f"&page={page_str}"
            data = requests.get(service_end_pt, headers=header_dict)
            if data.status_code == 200:
                return data.json()
            elif data.status_code == 404:
                return {}
            elif data.status_code == 400:
                return {}
        except Exception as e:
            excp_msg = e
            logging.exception(f"get_accounting_period Conection Error: {excp_msg}")
            import time
            time.sleep(3)
            retry -= 1
    return {}

def get_legal_entities_list(context:RContext):
    legal_entity_json = get_policy_data(context, LEGAL_ENTITY_END_POINT)
    return [j['name'] for j in [i['compliance_spec'] for i in legal_entity_json][0] if j['active']]

def process_rcs(rc_ids_process, context:RContext, dest_path):
    current_thread_name = current_thread().name
    for rc_id in rc_ids_process:
        get_duplicate_release_actions(context, rc_id, dest_path)

def process_threading(context, rc_ids, dest_path):
    try:
        print(f'Global record list: {len(rc_ids)}')
        chunk_size = len(rc_ids) // 4
        payload_chunks = [rc_ids[i * chunk_size:(i + 1) * chunk_size] for i in range((len(rc_ids) + chunk_size - 1) // chunk_size )]
        target_partial = partial(process_rcs)
        thread_list = []
        for each_chunk in payload_chunks:
            thread_list.append(Thread(target=target_partial,args=(each_chunk, context, dest_path)))
        for each_thread in thread_list:
            each_thread.start()
        for each_thread in thread_list:
            each_thread.join()
            print(f'Completed processing {each_thread.name}')
    except Exception as e:
        logging.exception('process_rc_list_threading_error', exc_info=e)

if __name__ == '__main__':
    context = make_context('snowflake', 'test', 'test-compliance-lib', 'unittest', 'lib')
    #legal_entities_list = get_legal_entities_list(context)
    total_rcs = fetch_rc_ids(context)
    today_date = str(datetime.datetime.today()).split(' ')[0]
    backup_time = str(datetime.datetime.now())
    path = '/Users/thiyageshdhandapani/Documents/Rightrev/Holmes/src/lib/prod_scripts/result_files'
    folder_path = f"{path}/{today_date}/{backup_time}"
    os.makedirs(folder_path)
    temp_file = new_uuid_as_string()
    dest_path = f"{folder_path}/{temp_file}.csv"
    fields = ['Legal Entity', 'Revenue Contract Id', 'Revenue Contract GUID', 'Order Id', 'Schedule Type', 'Release Action GUID', 'Count']
    write_header_to_csv(fields, dest_path)
    process_threading(context, total_rcs, dest_path)