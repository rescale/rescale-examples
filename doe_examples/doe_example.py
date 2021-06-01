"""
Example on how to use the Rescale API to create a multi task Rescale Job.
Terminology

1. Rescale DOE Job: a Rescale Job with multiple runs (aka tasks)
2. Rescale Job Run: a single execution of a run (aka task)

See the Rescale API Documentation for details on our REST API
https://engineering.rescale.com/api-docs/#introduction
"""
import requests
import logging
import json
import os
import sys
import polling
from urllib.parse import urljoin


class DoeSettings:

    def __init__(self,
                 slots=1500,
                 poll_step_sec=30,
                 completed_runs_threshold=1848,
                 poll_timeout_sec=3600,
                 walltime_hours=10):

        self.slots = slots
        self.poll_step_sec = poll_step_sec
        self.completed_runs_threshold = completed_runs_threshold
        self.poll_timeout_sec = poll_timeout_sec
        self.walltime_hours = walltime_hours


class DoeInputData:

    def __init__(self,
                 input_file_id=None, template_file_id=None,
                 param_file_id=None, postproc_file_id=None):

        self.input_file_id = input_file_id
        self.template_file_id = template_file_id
        self.param_file_id = param_file_id
        self.postproc_file_id = postproc_file_id


class RescaleDoeClient:

    def __init__(self):
        api_key = os.environ.get("RESCALE_API_KEY", None)
        base_url = os.environ.get("RESCALE_BASE_URL",
                                  "https://eu.rescale.com/api/v2/")
        if api_key is None:
            print("environment variable RESCALE_API_KEY not set!")
            exit(1)

        self.api_key = api_key
        self.base_url = base_url
        # in seconds
        self._connect_timeout = 10
        self._read_timeout = 120
        self._max_runs = 1848
        logging.info("Configured with base_url %s", self.base_url)

    @property
    def client(self):
        client = requests.Session()
        return client

    def get_me(self):
        url = self._get_url("users/me/")
        resp = self._make_request("GET", url)
        return resp.json()

    def create_job(self, job_definition):
        url = self._get_url("jobs/")
        resp = self._make_request("POST", url, json=job_definition)
        return resp.json()

    def submit_job(self, job_id):
        url = self._get_url("jobs/%s/submit/" % job_id)
        resp = self._make_request("POST", url)
        # submit job api returns no content
        return resp

    def stop_job(self, job_id):
        url = self._get_url("jobs/%s/stop/" % job_id)
        return self._make_request("POST", url)

    def get_job(self, job_id):
        url = self._get_url("jobs/%s/" % job_id)
        resp = self._make_request("GET", url)
        return resp.json()

    def get_job_status(self, job_id):
        url = self._get_url("jobs/%s/statuses/" % job_id)
        resp = self._make_request("GET", url)
        return resp.json()

    def is_job_executing(self, job_id):
        status_resp = self.get_job_status(job_id)
        statuses = status_resp.get('results', [])
        if statuses:
            recent_status = statuses[0]
            return recent_status is not None and "EXECUTING" ==  recent_status.get("status", "").upper()

        return False

    def get_runs(self, job_id):
        url = self._get_url("jobs/%s/runs/" % job_id)
        resp = self._make_request("GET",
                                  url,
                                  params={"page_size": self._max_runs})
        return resp.json()

    def upload_file(self, filename, filetype):
        url = self._get_url("files/contents/")

        # If you do not include the content-type header for a file form
        # submission, requests will automatically detect the type, including
        # the content-length stuff. If you specify the content-type
        # manually, it will be more...painful.
        # https://docs.python-requests.org/en/master/user/advanced/?highlight=boundary#post-multiple-multipart-encoded-files
        headers = {
            "Authorization": "Token %s" % self.api_key
        }
        resp = self._make_request("POST",
                                  url,
                                  headers=headers,
                                  files={'file': open(filename, 'rb'),
                                         'typeId': filetype})
        return resp.json()

    def _make_request(self, method, url, headers=None, **kwargs):
        print(url)
        headers = headers or {
            "Content-Type": "application/json",
            "Authorization": "Token %s" % self.api_key
        }

        return self.client.request(
            method,
            url,
            headers=headers,
            timeout=(self._connect_timeout, self._read_timeout),
            **kwargs
        )

    def _get_url(self, url_path) -> str:
        return urljoin(self.base_url, url_path)


def get_example_doe_job(doe_data, doe_settings):
    """The input csv file has 44 inputs which will create 44 runs (tasks)

    doe_data: DoeInputeData
    Since this is one slot in the hardware definition it will execute 1 run
    at a time until the entire job is complete or stopped.
    """
    return {
        "name":"OpenFOAM: Airfoil Parametric Analysis - CSV params (Cloned 1)",
        "description":"",
        "clonedFrom":"Qanyhb",
        "archiveFilters":[],
        "isLowPriority": True,
        "jobanalyses":[
            {
                "analysis":{"code":"openfoam_plus","version":"v1706+-intelmpi"},
                "command":"cd airfoil2D_DOE\n./Allrun",
                "flags":{},
                "hardware":{
                    "coresPerSlot":1,
                    "slots": doe_settings.slots,
                    "walltime": doe_settings.walltime_hours,
                    "type":"compute",
                    "coreType": "emerald"
                },
                "inputFiles":[{"id":doe_data.input_file_id,"decompress": True}],
                "onDemandLicenseSeller": None,
                "envVars": None,
                "postProcessScript":{"id":doe_data.postproc_file_id},
                "postProcessScriptCommand":"python extract.py airfoil2D_DOE/log.simpleFoam Cd Cl",
                "templateTasks":[
                    {"templateFile":{"id":doe_data.template_file_id},
                     "processedFilename":"airfoil2D_DOE/0/U"}
                ]
            }],
        "paramFile":{"id":doe_data.param_file_id},
        "jobvariables":[],
        "inputFileParseTask":"",
        "isTemplate": False,
        "templatedFrom": None,
        "includeNominalRun": False
    }


def process_job_runs(client, job_id):
    runs_resp = client.get_runs(job_id)
    num_expected_runs = runs_resp.get("count", 0)
    run_summary = dict()
    if num_expected_runs > 0:
        runs = runs_resp.get("results", [])
        for run in runs:
            if run_is_complete(run):
                run_summary['completed_runs'] = run_summary.get('completed_runs', 0) + 1
            if run_is_running(run):
                run_summary['executing_runs'] = run_summary.get('executing_runs', 0) + 1

    run_summary['expected_runs'] = num_expected_runs
    run_summary['pending_runs'] = num_expected_runs - run_summary.get('completed_runs', 0)
    logging.info("RUN SUMMARY: %s" % json.dumps(run_summary))
    return run_summary


def run_is_running(run_json):
    date_started = run_json.get("dateStarted", None)
    date_completed = run_json.get("dateCompleted", None)

    return date_started is not None and date_completed is None


def run_is_complete(run_json):
    date_started = run_json.get("dateStarted", None)
    date_completed = run_json.get("dateCompleted", None)

    return date_started is not None and date_completed is not None


def data_transfer(client):
    doe_input = DoeInputData()
    resp = client.upload_file('doe-example-jobfiles/airfoil2D_DOE.zip', 1)
    doe_input.input_file_id = resp['id']
    resp = client.upload_file('doe-example-jobfiles/U.inp_template', 2)
    doe_input.template_file_id = resp['id']
    resp = client.upload_file('doe-example-jobfiles/openfoam.csv', 3)
    doe_input.param_file_id = resp['id']
    resp = client.upload_file('doe-example-jobfiles/extract.py', 4)
    doe_input.postproc_file_id = resp['id']

    return doe_input


if __name__ == "__main__":
    # keeping log level at debug is useful to see that poll requests are
    # executing and the status code of the poll request
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    doe_settings = DoeSettings()
    doe_client = RescaleDoeClient()

    doe_input_data = data_transfer(doe_client)

    resp = doe_client.create_job(get_example_doe_job(doe_input_data, doe_settings))
    print(json.dumps(resp, indent=4, sort_keys=False))

    job_id = resp['id']
    doe_client.submit_job(job_id)

    resp = doe_client.get_job(job_id)
    print(json.dumps(resp, indent=4, sort_keys=False))

    # we wait until the job is in the Executing state before fetching
    # Job run information. Will poll every 30 seconds for 20 minutes
    polling.poll(
        lambda : doe_client.is_job_executing(job_id) is True,
        step=doe_settings.poll_step_sec,
        timeout=1200
    )
    print("job is now executing")
    resp = doe_client.get_runs(job_id)
    print(json.dumps(resp, indent=4, sort_keys=False))

    # poll until number of completed_runs = Threshold
    # poll every 30 seconds for 1 hour by default
    completed_run_threshold = doe_settings.completed_runs_threshold
    # you can use the lambda poll condition to break out of the polling and
    # into post-processing depending on metrics you define
    polling.poll(
        lambda : process_job_runs(doe_client, job_id).get('completed_runs', 0) >= completed_run_threshold,
        step=doe_settings.poll_step_sec,
        timeout=doe_settings.poll_timeout_sec
    )
    logging.info("Stopping Job")
    doe_client.stop_job(job_id)
    resp = doe_client.get_job_status(job_id)
    print(json.dumps(resp, indent=4, sort_keys=False))





