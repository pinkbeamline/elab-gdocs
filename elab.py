#!/home/epics/miniconda3/envs/google/bin/python

import epics
import os.path
import pickle
from googleapiclient.discovery import build
import queue
import time

## global variables
qbuff = queue.Queue()
DEBUG = False

def on_post(pvname=None, value=None, char_value=None, **kw):
    global qbuff
    qbuff.put(value)

def build_request(msg):
    requests = [
        {
            'insertText': {
                'endOfSegmentLocation': {
                    'segmentId': "",
                },
                'text': msg
            }
        }
    ]
    return requests


def main():
    global qbuff

    # setup PVs
    docidpv = epics.PV("PINK:SESSION:elab_id", auto_monitor=True)
    sessionstatepv = epics.PV("PINK:SESSION:sessionstate", auto_monitor=True)
    post_pv = epics.PV("PINK:SESSION:elab_queue", auto_monitor=True, callback=on_post)
    elab_status = epics.PV("PINK:SESSION:elab_status", auto_monitor=False)

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    else:
        print("Token not found.")
        return

    service = build('docs', 'v1', credentials=creds)

    try:
        print("Listening PV ...")
        qbuff.queue.clear()
        while(True):
            if qbuff.qsize()>0:
                if (sessionstatepv.value>0):
                    DOCUMENT_ID = docidpv.value.tobytes().decode('UTF-8')
                    msg = ""
                    while qbuff.qsize():
                        msg=msg+qbuff.get().tobytes().decode('UTF-8')+'\n'
                    requests=build_request(msg)
                    if DEBUG: print(DOCUMENT_ID)
                    if DEBUG: print(msg, end='')
                    try:
                        result = service.documents().batchUpdate(documentId=DOCUMENT_ID, body={'requests': requests}).execute()
                    except:
                        elab_status.put("Could not find document")
                        print("Could not find document")

                else:
                    qbuff.queue.clear()
                    print(DOCUMENT_ID)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nBye")

    print("OK")

if __name__ == '__main__':
    main()

