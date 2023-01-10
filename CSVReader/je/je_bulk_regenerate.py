import os
import sys
import json
import requests
import logging
from functools import partial
from threading import Thread
from r2essentials import init_logger
from r2essentials.config import get_from_env
from r2essentials.context import make_context

init_logger.setup(service_name = "je-adhoc-regenerate-api")

JE_URL_HOST = get_from_env("JE_URL", "")
URL_TYPE = get_from_env("URL_TYPE", "/apis/je/v1/")
if not JE_URL_HOST:
    HOST_FLAG = False
    JE_URL_HOST = "https://internal.evn_var.rightrev.cloud"
JE_URL = f"{JE_URL_HOST}{URL_TYPE}"
REGENERATE_API = f"{JE_URL}regenerate/"

# def read_s3_file_data(s3_resource,bucket,file_path):
#     try:
#         temp_file_name = "temp.csv"
#         s3_resource.Bucket(bucket).download_file(Key=file_path,Filename=temp_file_name)
#         with open(temp_file_name,"r") as fin:
#             file_data = fin.read()
#         os.remove(temp_file_name)
#     except Exception as e:
#         return False
#     return file_data

def run_regenerate_api(context, rc_list):
    try:
        tenant_id = context.tenant_context.tenant_id
        user_id = context.user_context.user_id
        headers = {'X-R2-USER-ID': user_id,
                    'x-r2-tenant-id': tenant_id,
                    'x-r2-user-roles': 'journal-run:view:create',
                    'Content-Type': 'application/json'
                }
        payload = json.dumps({
                    "rc_list": ",".join(rc_list)
                    })
        resp = requests.put(REGENERATE_API, headers=headers, data=payload)
        logging.info(str(resp))
        if resp.status_code == 200:
            logging.info(f"Successfully triggered regenerate for {rc_list}")
        else:
            logging.info(f"Failed to regenerate for {rc_list}")
    except Exception as e:
        logging.exception(f"An Exception Occurred : {e}")

if __name__ == "__main__":
    #Sample command : python scripts/je_bulk_regenerate.py pavan-dev 2 0 5 dev
    # tenant_id thread_size pick_size_start pick_size_end env
    sysdata = sys.argv
    batch_size = 500
    total_len = 31000
    pick_size_st = 0
    parent_chunk_size = 50
    tenant_id = 'vqah00000000d00'
    env = 'prd'
    iteration = 1
    while total_len > 0:
        pick_size_en = pick_size_st + batch_size
        print(f"Iteration: {iteration} start for {pick_size_st}:{pick_size_en}")
        if not HOST_FLAG:
                REGENERATE_API = REGENERATE_API.replace("evn_var", env)
        user_id = "admin@rightrev.com"
        if not pick_size_st:
            pick_size_st = 1
        rc_file_list = [f"RC-{i}" for i in range(pick_size_st, pick_size_en)]
        context = make_context(tenant_id, user_id, 'generate-je', '', '')
        target_partial = partial(run_regenerate_api, context)
        # run_regenerate_api(context, rc_file_list)
        thread_list = []
        print(rc_file_list)
        payload_chunks = [rc_file_list[i * parent_chunk_size:(i + 1) * parent_chunk_size] for i in range((len(rc_file_list) + parent_chunk_size - 1) // parent_chunk_size )]
        for each_chunk in payload_chunks:
            thread_list.append(Thread(target=target_partial,args=(each_chunk,)))
        for each_thread in thread_list:
            each_thread.start()
        for each_thread in thread_list:
            each_thread.join()
            #print(f"Completed {each_thread.name}")

        total_len -= batch_size
        print(f"Completed iteration: {iteration} for {pick_size_st}:{pick_size_en}")
        pick_size_st = pick_size_en
        iteration += 1

    # if len(sysdata) == 6:
    #     try:
    #         tenant_id, parent_chunk_size, pick_size_st, pick_size_en, env  = sysdata[1:]
    #         parent_chunk_size = int(parent_chunk_size)
    #         pick_size_st = int(pick_size_st)
    #         pick_size_en = int(pick_size_en)
    #         if not HOST_FLAG:
    #             REGENERATE_API = REGENERATE_API.replace("evn_var", env)
    #         user_id = "admin@rightrev.com"
    #         # # keyname = f"{tenant_id}/RCJERegenerate.csv"
    #         # # rc_data = read_s3_file_data(s3_resource, bucket, keyname)
    #         # temp_file_name = f"/Users/svpavankumar/Downloads/DUDERCJEGEnerate.csv"
    #         # with open(temp_file_name,"r") as fin:
    #         #     rc_data = fin.read()
    #         # rc_list = rc_data.split("\n")[1:]
    #         if not pick_size_st:
    #             pick_size_st = 1
    #         rc_file_list = [f"RC-{i}" for i in range(pick_size_st, pick_size_en)]
    #         # if pick_size_en:
    #         #     pick_size_st = pick_size_st or 0
    #         #     rc_file_list = rc_file_list[pick_size_st:pick_size_en]
    #         context = make_context(tenant_id, user_id, 'generate-je', '', '')
    #         target_partial = partial(run_regenerate_api, context)
    #         # run_regenerate_api(context, rc_file_list)
    #         thread_list = []
    #         print(rc_file_list)
    #         payload_chunks = [rc_file_list[i * parent_chunk_size:(i + 1) * parent_chunk_size] for i in range((len(rc_file_list) + parent_chunk_size - 1) // parent_chunk_size )]
    #         for each_chunk in payload_chunks:
    #             thread_list.append(Thread(target=target_partial,args=(each_chunk,)))
    #         for each_thread in thread_list:
    #             each_thread.start()
    #         for each_thread in thread_list:
    #             each_thread.join()
    #             print(f"Completed {each_thread.name}")
    #     except Exception as ex:
    #         print(f"Exception occured : {ex}")
    # else:
    #     print('Missing argument. ')