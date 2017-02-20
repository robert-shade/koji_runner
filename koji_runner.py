import koji
import koji.util
import time
import requests
import pprint
import re
import logging
import traceback
import sys
import zipfile
import tempfile
import os
import argparse

logger = logging.getLogger('koji_runner')

def pretty_print_POST(req):
    """
    At this point it is completely built and ready
    to be fired; it is "prepared".

    However pay attention at the formatting used in 
    this function because it is programmed to be pretty 
    printed and may differ from the actual request.
    """
    return '{}\n{}\n{}\n\n{}'.format(
        '-----------START-----------',
        req.method + ' ' + req.url,
        '\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        req.body,
        )

# class GitlabBuildPoller(object)
#     def __init__(self, gitlab_url, gitlab_token):
#         self.gitlab_url = gitlab_url
#         self.gitlab_token = gitlab_token
#         self.requests_session = None

class GitlabBuildManager(object):
    def __init__(self, gitlab_url, gitlab_token, build_info):
        self.build_info = build_info
        self.gitlab_url = gitlab_url
        self.gitlab_token = gitlab_token
        self.trace_index = 0
        self.build_token = build_info["token"]
        self.last_file_name = None
        self.requests_session = requests.Session()
        self.artifacts_tmp_file = None
        self.artifacts = None

    def mark_success(self):
        self.update_build_state("success")

    def mark_failed(self):
        self.update_build_state("failed")

    def mark_running(self):
        self.update_build_state("running")

    def update_build_state(self, state):
        req = requests.Request(\
                "PUT", \
                "%s/api/v1/builds/%s" % (self.gitlab_url, self.build_info["id"]), \
                data = { "token" : self.gitlab_token, "state" : state })

        req_prep = req.prepare()
        resp = self.requests_session.send(req_prep)

        resp.raise_for_status()

    def capture_artifact(self, file_name, data):
        if self.artifacts is None:
            self.artifacts_tmp_file = tempfile.TemporaryFile()
            self.artifacts = zipfile.ZipFile(self.artifacts_tmp_file, mode='w')

        self.artifacts.writestr(file_name, data)

    def upload_artifacts(self):
        #logger.debug("Uploading %s" % (file_name,))

        self.artifacts.close()
        self.artifacts = None
        self.artifacts_tmp_file.seek(0)

        req = requests.Request('POST', \
                "%s/api/v1/builds/%s/artifacts" % (self.gitlab_url, self.build_info["id"]), \
                headers = { "BUILD-TOKEN" : self.build_token }, \
                data = { "token" : self.build_token }, \
                files = { "file": ("artifacts.zip", self.artifacts_tmp_file) } )

        req_prep = req.prepare()
        resp = self.requests_session.send(req_prep)

        self.artifacts_tmp_file.close()
        self.artifacts_tmp_file = None

        resp.raise_for_status()

    def append_build_trace(self, file_name, data):
        if self.last_file_name <> file_name:
            self.last_file_name = file_name

            if file_name is not None:
                data = "\n=============== %s ===============\n%s" % (file_name, data)
            else:
                data = "\n\n"

        if data is not None:
            data_len = len(data)
        else:
            data_len = 0

        if data_len > 0:
            req = requests.Request(\
                     "PATCH", \
                     "%s/api/v1/builds/%s/trace.txt" % (self.gitlab_url, self.build_info["id"]), \
                     headers={ "BUILD-TOKEN" : self.build_token, "Content-Range" : "%d-%d" % (self.trace_index, self.trace_index + data_len) }, \
                     data=data)

            req_prep = req.prepare()
            resp = self.requests_session.send(req_prep)

            if resp.status_code == 202:
                build_status = resp.headers["build-status"]
                trace_range = resp.headers["range"]

                m = re.match(r"([0-9]+)-([0-9]+)", trace_range)

                if m:
                    start = int(m.group(1))
                    end = int(m.group(2))

                    self.trace_index = end
            else:
                r.raise_for_status()

class KojiTaskWatcher(object):
    def __init__(self,task_id,session, gitlab_bm):
        self.id = task_id
        self.session = session
        self.info = None
        self.log_offsets = {}
        self.gitlab_bm = gitlab_bm
        self.child_tasks = {}
        self.children_done = False

    def is_done(self):
        if self.info is None:
            return False
        state = koji.TASK_STATES[self.info['state']]
        return (state in ['CLOSED','CANCELED','FAILED'])

    def is_success(self):
        if self.info is None:
            return False
        state = koji.TASK_STATES[self.info['state']]
        return (state == 'CLOSED')

    def update(self):
        if self.is_done():

            if self.children_done:
                return False

            all_done = True
            for child_task_id in self.child_tasks:
                if self.child_tasks[child_task_id].update():
                    all_done = False

            if all_done:
                self.children_done = True

                # Grab artifacts if we're done
                files = self.session.listTaskOutput(self.id)

                for filename in files:
                    file_data = self.session.downloadTaskOutput(self.id, filename)

                    if filename.endswith(".log"):
                        filename = "%s.%s.log" % (filename.rstrip(".log"), self.info["arch"])

                    self.gitlab_bm.capture_artifact(filename, file_data)

            return False

        last_info = self.info
        self.info = self.session.getTaskInfo(self.id, request=True)

        # State Report
        temp_info = self.info.copy()
        temp_info["state"] = koji.TASK_STATES.get(temp_info["state"],'BADSTATE')
        temp_info["label"] = koji.taskLabel(self.info)

        if last_info: 
            if self.info["state"] != last_info["state"]:
                self.gitlab_bm.append_build_trace(None, "%(id)s %(state)s %(label)s\n" % temp_info)
        else:
            self.gitlab_bm.append_build_trace(None, "%(id)s %(state)s %(label)s\n" % temp_info)

        # Discover/Update Child Tasks
        for child_task in self.session.getTaskChildren(self.id):
            child_task_id = child_task['id']
            
            if child_task_id not in self.child_tasks:
                self.child_tasks[child_task_id] = KojiTaskWatcher(child_task_id, self.session, self.gitlab_bm)

        for child_task_id in self.child_tasks:
            self.child_tasks[child_task_id].update()

        # Update Log Files
        output = self.session.listTaskOutput(self.id)

        logs = [filename for filename in output if filename.endswith('.log')]

        for log in logs:
            contents = 'placeholder'

            while contents:
                if log not in self.log_offsets:
                    self.log_offsets[log] = 0

                logger.debug("Downloading %s starting at offset %d" % (log, self.log_offsets[log],))

                contents = self.session.downloadTaskOutput(self.id, log, self.log_offsets[log], 16384)

                if len(contents) > 0:
                    self.log_offsets[log] += len(contents)

                    self.gitlab_bm.append_build_trace("Task:%d - %s" % (self.id, log,), contents)

        return True

def do_build(build_info, args):
    build_state = {}
    build_state["content_index"] = 0
    build_id = build_info["id"]

    logger.debug("=========== Processing Build %d ===========" % (build_id,))
    #logger.debug(pprint.pprint(build_info))

    bm = GitlabBuildManager(args.gitlab_url, args.gitlab_token, build_info)

    try:
        tag = build_info["options"]["image"]
        repo_url = build_info["repo_url"]
        ci_build_ref = None

        for var in build_info["variables"]:
            if var["key"] == "CI_BUILD_REF":
                ci_build_ref = var["value"]

        bm.mark_running()
        s = koji.ClientSession(args.koji_url)
        s.ssl_login(args.client_cert, None, args.server_ca, proxyuser=None)

        try: 
            s.getAPIVersion()
        except:
            logger.error("Unable to login to Koji")
            raise

        opts = { 'scratch' : True }

        parent_task_id = s.build("git+" + repo_url + "#" + ci_build_ref, tag, opts)

        logger.debug("Created task %d" % (parent_task_id,))
        parent_task = KojiTaskWatcher(parent_task_id, s, bm)

        while not parent_task.is_done():
            parent_task.update()
            time.sleep(1)
                    
        if parent_task.is_success():
            bm.upload_artifacts()
            bm.mark_success()
        else:
            bm.mark_failed()

    except Exception as ex:
        msg = traceback.format_exc()
        logging.error(msg)

        bm.append_build_trace(None, msg)
        bm.mark_failed()

def main():
    parser = argparse.ArgumentParser(prog="koji_runner.py", description='Koji Gitlab Runner')
    parser.add_argument("--gitlab-url", dest='gitlab_url', required=True)
    parser.add_argument("--gitlab-token", dest='gitlab_token', required=True)
    parser.add_argument("-u", "--koji-user", dest='koji_user', help="User %(prog)s will authenticate to Koji with", required=True)
    parser.add_argument("--koji-url", dest='koji_url', required=True)
    parser.add_argument("--client-crt", dest='client_cert', required=True)
    parser.add_argument("--server-ca", dest='server_ca', required=True)

    args = parser.parse_args()

    s = requests.Session()

    while True:
        logger.debug("Requesting next build...")

        req = requests.Request("POST", \
                "%s/api/v1/builds/register" % (args.gitlab_url,), data={ "token" : args.gitlab_token })
        prep = req.prepare()

        #pretty_print_POST(prep)

        r = s.send(prep)

        if r.status_code == 201:
            do_build(r.json(), args)
        else:
            logger.debug("No builds available.  Sleeping...")
            time.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)

    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logger.setLevel(logging.DEBUG)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logger.addHandler(console)

    main()
