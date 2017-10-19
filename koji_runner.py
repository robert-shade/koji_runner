import gitlab
import logging
import requests
import os
import os.path
import shutil
import time
import pprint
import logging
import threading
from Queue import Queue
import traceback
import koji
import argparse
import re
import tempfile
import glob
import zipfile

LOG = "koji_gitlab_runner"
LOG_WORKER = LOG + ".worker"
LOG_WORKER_MANAGEMENT = LOG_WORKER + ".management"
LOG_JOB = LOG + ".job"
LOG_JOB_ARTIFACTS = LOG_JOB + ".artifacts"
LOG_JOB_TRACE = LOG_JOB + ".trace"
LOG_JOB_MANAGEMENT = LOG_JOB + ".management"
LOG_KOJI = LOG + ".koji"
LOG_KOJI_TASK = LOG_KOJI + ".task"
LOG_KOJI_SESSION = LOG_KOJI + ".session"
LOG_KOJI_TASK_OUTPUT = LOG_KOJI_TASK + ".output"

logging.getLogger(LOG).addHandler(logging.NullHandler())

class KojiTaskWatcher(object):
    def __init__(self, job, task_id, session, trace_callback = None, artifact_callback = None):
        self.job = job
        self.id = task_id
        self.session = session
        self.info = None
        self.file_offsets = {}
        self.child_tasks = {}

        self.trace_callback = trace_callback
        self.artifact_callback = artifact_callback

        logging.getLogger(LOG_KOJI_TASK).info(
                "Watching Koji Task {task_id}".format(task_id=self.id),
                extra={ "koji_task_id" : self.id, "gitlab_job_id" : self.job["id"] })

    def cancel(self):
        try:
            logging.getLogger(LOG_JOB).info(
                    "Cancelling task",
                    extra = { "gitlab_job_id" : self.job["id"], "koji_task_id" : self.id })

            self.session.cancelTask(self.id, recurse=True)
        except Exception as ex:
            logging.getLogger(LOG_JOB).info(
                    "Error cancelling task: {msg}".format(msg=str(ex)),
                    extra = { "gitlab_job_id" : self.job["id"], "koji_task_id" : self.id })

    def is_cancelled(self):
        if self.info is None:
            return False

        state = koji.TASK_STATES[self.info['state']]
        return (state in ['CANCELED'])

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
        # Get Lastest Info
        last_info = self.info

        self.info = self.session.getTaskInfo(self.id, request=True)
        self.info["label"] = koji.taskLabel(self.info)
        self.info["state_str"] = koji.TASK_STATES[self.info["state"]]

        # Write the new job state to the trace
        if last_info:
            if self.info["state"] != last_info["state"]:
                logging.getLogger(LOG_KOJI_TASK).info(
                        "Koji Task {old_state} => {new_state}"
                            .format(
                                task_id=self.id, 
                                old_state=last_info["state_str"], 
                                new_state=self.info["state_str"]),
                        extra={ "koji_task_id" : self.id, "gitlab_job_id" : self.job["id"] })

                if self.trace_callback is not None: 
                    self.trace_callback(self.session, None, "%(id)s %(state_str)s %(label)s\n" % self.info)
        else:
            if self.trace_callback is not None: 
                self.trace_callback(self.session, None, "%(id)s %(state_str)s %(label)s\n" % self.info)

        # Discover Child Tasks
        for child_task in self.session.getTaskChildren(self.id):
            child_task_id = child_task['id']

            if child_task_id not in self.child_tasks:
                logging.getLogger(LOG_KOJI_TASK).info(
                        "Discovered new child task {child_task_id}"
                            .format(child_task_id = child_task_id),
                            extra={ "koji_task_id" : self.id, "gitlab_job_id" : self.job["id"] })

                self.child_tasks[child_task_id] = \
                        KojiTaskWatcher(\
                            self.job, \
                            child_task_id, \
                            self.session, \
                            self.trace_callback, \
                            self.artifact_callback)

        # Update child tasks
        all_children_done = True
        for child_task_id in self.child_tasks:
            if self.child_tasks[child_task_id].update():
                all_children_done = False

        # Get log updates/output
        output_files = self.session.listTaskOutput(self.id)

        for file_name in output_files:
            full_file_name = os.path.join(str(self.id), file_name)

            while True:
                if file_name not in self.file_offsets:
                    self.file_offsets[file_name] = 0

                logging.getLogger(LOG_KOJI_TASK_OUTPUT).debug(
                        "Downloading {file_name} starting at offset {offset}"\
                            .format(file_name = full_file_name, offset = self.file_offsets[file_name]),
                            extra={ "koji_task_id" : self.id, "gitlab_job_id" : self.job["id"] })

                contents = self.session.downloadTaskOutput(self.id, file_name, self.file_offsets[file_name], 16384)

                logging.getLogger(LOG_KOJI_TASK_OUTPUT).debug(
                        "Downloaded {content_length} bytes of {file_name} starting at offset {offset}"
                            .format(file_name = full_file_name, offset = self.file_offsets[file_name], content_length = len(contents)),
                            extra={ "koji_task_id" : self.id, "gitlab_job_id" : self.job["id"] })

                if len(contents) > 0:
                    if file_name.endswith('.log') and self.trace_callback is not None: 
                        self.trace_callback(self.session, "Task:%d - %s" % (self.id, file_name,), contents)

                    if self.artifact_callback is not None: 
                        self.artifact_callback(self.session, full_file_name, contents, self.file_offsets[file_name])

                    self.file_offsets[file_name] += len(contents)
                else:
                    break

        logging.getLogger(LOG_KOJI_TASK).info(
                "Koji Task - State: {state}, Done: {is_done}"
                    .format(
                        task_id=self.id, 
                        state=self.info["state_str"],
                        is_done=self.is_done()),
                extra={ "koji_task_id" : self.id, "gitlab_job_id" : self.job["id"] })

        return self.is_done() and all_children_done

class GitlabJobPoller:
    def __init__(self, url, token):
        self.url = url
        self.token = token
        self.session = requests.Session()

    def request_job(self):
        req = requests.Request(\
                "POST", \
                "{url}/api/v4/jobs/request".format(url=self.url), \
                data = { "token" : self.token })

        prep_req = req.prepare()

        resp = self.session.send(prep_req)
        
        if resp.status_code == 204:
            # Code for 'no jobs'
            return None

        if resp.status_code == 201:
            # Code for 'got jobs'
            return resp.json()

        return None

class KojiGitlabJobWorker:
    def __init__(self, job, gitlab_url, koji_url, koji_client_cert, koji_client_ca, koji_server_ca):
        self.job = job
        self.requests_session = requests.Session()
        self.gitlab_url = gitlab_url
        self.koji_url = koji_url
        self.koji_client_cert = koji_client_cert
        self.koji_client_ca = koji_client_ca
        self.koji_server_ca = koji_server_ca

        self.artifacts_temp_dir = tempfile.mkdtemp(suffix="artifacts")

        self.trace_index = 0
        self.last_trace_file_name = None

        self.job_variables = {}
        
        for v in self.job["variables"]:
            self.job_variables[v["key"]] = v["value"]

    def process(self):
        for variable in [v for v in self.job["variables"] if v["public"] == True]:
            logging.getLogger(LOG_JOB).debug(
                    "[{name}]={value} (public)"
                    .format(job_id=self.job["id"], name=variable["key"], value=variable["value"]),
                    extra = { "gitlab_job_id" : self.job["id"] })

        parent_task = None

        try:
            repo_url = self.job_variables["CI_REPOSITORY_URL"]
            ci_build_ref = None
            build_target = None
            is_scratch = None

            for key, value in self.job_variables.iteritems():
                if key == "CI_BUILD_SHA" or key == "CI_COMMIT_SHA":
                    ci_build_ref = value
                elif key == "KOJI_TARGET":
                    build_target = value
                elif key == "KOJI_SCRATCH":
                    is_scratch = value.upper() in ["TRUE", "T", "YES", "Y"]

            if build_target is None:
                raise Exception("No build target specified")

            koji_session = koji.ClientSession(self.koji_url)
            koji_session.ssl_login(self.koji_client_cert, self.koji_client_ca, self.koji_server_ca, proxyuser=None)

            try:
                logging.getLogger(LOG_KOJI_SESSION).debug("Logging into Koji", extra={ "gitlab_job_id" : self.job["id"] })

                koji_session.getAPIVersion()
            except Exception as ex:
                logging.getLogger(LOG_KOJI_SESSION).error("Unable to login to Koji: " + str(ex), extra={ "gitlab_job_id" : self.job["id"] })
                raise

            opts = { 'scratch' : is_scratch }

            parent_task_id = koji_session.build(\
                                "git+" + repo_url + "#" + ci_build_ref, \
                                build_target, \
                                opts)

            logging.getLogger(LOG_KOJI_TASK).info(
                    "Created Parent Koji Task",
                    extra={ "gitlab_job_id" : self.job["id"], "koji_task_id" : parent_task_id })

            parent_task = KojiTaskWatcher(\
                            self.job, \
                            parent_task_id, \
                            koji_session, \
                            self.append_build_trace,
                            self.capture_artifact)

            while not parent_task.update():

                if self.job["status"].lower() == "canceled":
                    parent_task.cancel()

                    while not parent_task.update():
                        time.sleep(1)

                    break

                time.sleep(5)

            logging.getLogger(LOG_KOJI_TASK).info(
                    "Parent Koji Task => {state}"
                    .format(task_id=parent_task_id,
                            state="Success" if parent_task.is_success() else "Failed"),
                    extra = { "gitlab_job_id" : self.job["id"], "koji_task_id" : parent_task_id })

            self.upload_artifacts()

            if parent_task.is_cancelled():
                pass
            elif parent_task.is_success():
                self.update_job_state("success")
            else:
                self.update_job_state("failed")

        except Exception as ex:
            logging.getLogger(LOG_JOB).error("{message}"
                                .format(message=traceback.format_exc()),
                                extra = { "gitlab_job_id" : self.job["id"] })

            if parent_task is not None:
                parent_task.cancel()

            self.update_job_state("failed")

    def capture_artifact(self, koji_session, file_name, data, offset = 0):
        logging.getLogger(LOG_JOB_ARTIFACTS).debug("Captured Artifact {file_name} ({offset}-{file_length})"
                            .format(file_name=file_name, offset=offset, file_length=offset + len(data)),
                            extra = { "gitlab_job_id" : self.job["id"] })

        abs_file_name = os.path.join(self.artifacts_temp_dir, file_name)

        try:
            os.makedirs(os.path.dirname(abs_file_name))
        except:
            pass

        with open(abs_file_name, 'wb') as f:
            f.seek(offset)
            f.write(data)

    def upload_artifacts(self):
        try:
            for artifacts_opts in self.job["artifacts"] if "artifacts" in self.job else []:
                artifact_archive_name = "artifacts.zip"
                artifact_archive_files = []
                upload_archive = True

                # Artifact Name
                if "name" in artifacts_opts and \
                        artifacts_opts["name"] is not None and \
                        len(artifacts_opts["name"]) > 0:
                    artifact_archive_name = artifacts_opts["name"] + ".zip"

                # Find artifacts according to the globs
                for path in artifacts_opts["paths"] if "paths" in artifacts_opts else []:
                    abs_path = os.path.join(self.artifacts_temp_dir, path)

                    logging.getLogger(LOG_JOB_ARTIFACTS).debug("Looking for artifacts in {path}".format(path=abs_path),
                                        extra = { "gitlab_job_id" : self.job["id"] })

                    for f in glob.glob(abs_path):
                        logging.getLogger(LOG_JOB_ARTIFACTS).debug("Found artifact {file_name}".format(file_name=f),
                                            extra = { "gitlab_job_id" : self.job["id"] })

                        artifact_archive_files.append(f)

                # Build/Upload artifact archive
                if self.job["status"].lower() == "canceled":
                    logging.getLogger(LOG_JOB_ARTIFACTS).debug("Skipping uploading artifact archive.  Job Cancelled.")
                elif len(artifact_archive_files) > 0:
                    with tempfile.NamedTemporaryFile() as tmp_file:
                        with zipfile.ZipFile(tmp_file, 'w', zipfile.ZIP_DEFLATED) as zip_file: 
                            for f in artifact_archive_files:
                                relative_name = os.path.relpath(f, self.artifacts_temp_dir)

                                logging.getLogger(LOG_JOB_ARTIFACTS).debug("Adding {file_name} to archive as {rel_file_name}"\
                                                    .format(file_name=f, rel_file_name=relative_name),
                                                    extra = { "gitlab_job_id" : self.job["id"] })

                                zip_file.write(f, relative_name)

                        tmp_file.flush()
                        tmp_file.seek(0)

                        # Upload archive
                        req = requests.Request('POST', \
                                "%s/api/v4/jobs/%s/artifacts" % (self.gitlab_url, self.job["id"]), \
                                headers = { "JOB-TOKEN" : self.job["token"] }, \
                                files = { "file": ( artifact_archive_name, tmp_file) } )

                        req_prep = req.prepare()
                        resp = self.requests_session.send(req_prep)

                        if resp.ok:
                            logging.getLogger(LOG_JOB_ARTIFACTS).debug("Uploaded artifact archive {file_name}".format(file_name=artifact_archive_name))
                        else:
                            resp.raise_for_status()
                else:
                    logging.getLogger(LOG_JOB_ARTIFACTS).debug("Skipping uploading artifact archive.  No artifacts found.")
        finally:
            logging.getLogger(LOG_JOB_ARTIFACTS).debug("Removing Temporary Artifact Directory {artifacts_temp_dir}"
                                .format(artifacts_temp_dir=self.artifacts_temp_dir),
                                extra = { "gitlab_job_id" : self.job["id"] })

            shutil.rmtree(self.artifacts_temp_dir)

    def append_build_trace(self, koji_session, file_name, data):
        if self.last_trace_file_name <> file_name:
            self.last_trace_file_name = file_name

        if file_name is not None:
            data = "\n=============== %s ===============\n%s" % (file_name, data)
        else:
            data = "\n\n"

        if data is not None:
            data_len = len(data)
        else:
            data_len = 0

        if data_len > 0:
            logging.getLogger(LOG_JOB_TRACE).debug(
                    "Updating trace %d-%d" % (self.trace_index, self.trace_index + data_len,),
                    extra = { "gitlab_job_id" : self.job["id"] })

            req = requests.Request(\
                     "PATCH", \
                     "{url}/api/v4/jobs/{job_id}/trace"\
                        .format(url=self.gitlab_url, job_id=self.job["id"]), \
                     headers={ \
                        "JOB-TOKEN" : self.job["token"], \
                        "Content-Range" : "%d-%d" % (self.trace_index, self.trace_index + data_len) }, \
                     data=data)

            req_prep = req.prepare()
            resp = self.requests_session.send(req_prep)

            if resp.ok:
                job_status = resp.headers["job-status"]
                trace_range = resp.headers["range"]

                m = re.match(r"([0-9]+)-([0-9]+)", trace_range)

                if m:
                    start = int(m.group(1))
                    end = int(m.group(2))

                    self.trace_index = end

                if self.job["status"] != job_status:
                    logging.getLogger(LOG_JOB_TRACE).debug(
                            "Trace updated. Job State {old_status} => {new_status}"
                                .format(old_status=self.job["status"], new_status=job_status),
                            extra = { "gitlab_job_id" : self.job["id"] })

                    self.job["status"] = job_status
            else:
                resp.raise_for_status()

    def update_job_state(self, state):
        logging.getLogger(LOG_JOB_MANAGEMENT).debug(
                "Updating Job State => {state}".format(state = state),
                extra = { "gitlab_job_id" : self.job["id"] })

        req = requests.Request(\
                "PUT", \
                "{url}/api/v4/jobs/{job_id}"\
                .format(url=self.gitlab_url, job_id=self.job["id"]), \
                    data = { \
                        "token" : self.job["token"], \
                        "id" : self.job["id"], \
                        "state" : state })

        prep_req = req.prepare()

        resp = self.requests_session.send(prep_req)

        return resp.ok

class KojiGitlabRunner:
    def __init__(self, options):
        self.gitlab_url = options["gitlab_url"]
        self.gitlab_token = options["gitlab_token"]
        self.gitlab_job_poller = GitlabJobPoller(self.gitlab_url, self.gitlab_token)

        self.koji_url = options["koji_url"]
        self.koji_client_cert = options["koji_client_cert"]
        self.koji_client_ca = options["koji_client_ca"]
        self.koji_server_ca = options["koji_server_ca"]

        self.num_workers = 5
        self.work_queue = Queue(maxsize=self.num_workers)

    def worker_run(self):
        logging.getLogger(LOG_WORKER_MANAGEMENT).debug("Starting Worker")

        while True:
            job = self.work_queue.get(block=True)

            if job is None:
                # Poison Pill
                logging.getLogger(LOG_WORKER_MANAGEMENT).debug("Received poison pill. Shutting down.")
                break

            logging.getLogger(LOG_JOB_MANAGEMENT).info("Received Job", extra = { "gitlab_job_id" : job["id"] })

            job["status"] = "created"

            KojiGitlabJobWorker(\
                job, \
                self.gitlab_url, \
                self.koji_url, \
                self.koji_client_cert, \
                self.koji_client_ca, \
                self.koji_server_ca).process()
            
            logging.getLogger(LOG_JOB_MANAGEMENT).info("Job Complete", extra = { "gitlab_job_id" : job["id"] })

            self.work_queue.task_done()

        logging.getLogger(LOG_WORKER_MANAGEMENT).debug("Stopping Worker")

    def run(self):
        logging.getLogger(LOG).info("Starting Up")

        logging.getLogger(LOG_WORKER_MANAGEMENT).debug("Starting {} worker(s)...".format(self.num_workers))

        for i in range(self.num_workers):
            t = threading.Thread(target=self.worker_run)
            t.daemon = True
            t.start()

        while True:
            try:
                logging.getLogger(LOG_JOB_MANAGEMENT).debug("Requesting a job...")

                job = self.gitlab_job_poller.request_job()

                if job is None:
                    logging.getLogger(LOG_JOB_MANAGEMENT).debug("No jobs available.  Sleeping...")
                    time.sleep(5)
                else:
                    self.work_queue.put(job)
            except Exception as ex:
                print ex
                break

        logging.getLogger(LOG).info("Shutting Down")

def main():
    parser = argparse.ArgumentParser(prog="koji_runner.py", description="Koji Gitlab Runner")
    parser.add_argument("--gitlab-url", dest='gitlab_url', required=False)
    parser.add_argument("--gitlab-token", dest='gitlab_token', required=False)
    parser.add_argument("-u", "--koji-user", dest='koji_user', help="User %(prog)s will authenticate to Koji with", required=False)
    parser.add_argument("--koji-url", dest='koji_url', required=False)
    parser.add_argument("--client-crt", dest='client_cert', required=False)
    parser.add_argument("--client-ca", dest='client_ca', required=False)
    parser.add_argument("--server-ca", dest='server_ca', required=False)

    args = parser.parse_args()

    opts = {}
    opts["gitlab_url"] = os.environ.get('GITLAB_URL', args.gitlab_url)	
    opts["gitlab_token"] = os.environ.get('GITLAB_TOKEN', args.gitlab_token)	
    opts["koji_url"] = os.environ.get('KOJI_URL', args.koji_url)	
    opts["koji_user"] = os.environ.get('KOJI_USER', args.koji_user)	
    opts["koji_client_cert"] = os.environ.get('KOJI_CLIENT_CERT', args.client_cert)
    opts["koji_client_ca"] = os.environ.get('KOJI_CLIENT_CA', args.client_ca)
    opts["koji_server_ca"] = os.environ.get('KOJI_SERVER_CA', args.server_ca)

    opts["gitlab_url"] = re.sub('\/ci\/?$', '', opts["gitlab_url"])

    runner = KojiGitlabRunner(opts)
    runner.run()

class CustomFormatter(logging.Formatter): 
    def format(self, record): 
        if not hasattr(record, 'gitlab_job_id'): 
            record.gitlab_job_id = '-' 

        if not hasattr(record, 'koji_task_id'): 
            record.koji_task_id = '-' 

        return super(CustomFormatter, self).format(record)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)

    kojiHandler = logging.StreamHandler()
    kojiHandler.setLevel(logging.INFO)
    kojiHandler.setFormatter(CustomFormatter('%(asctime)s.%(msecs)03d [%(thread)d][%(gitlab_job_id)s][%(koji_task_id)s] %(message)s', "%Y-%m-%d %H:%M:%S"))

    logging.getLogger(LOG).addHandler(kojiHandler)
    logging.getLogger("koji").addHandler(kojiHandler)

    logging.getLogger("requests.packages.urllib3").setLevel(logging.ERROR)

    main()
