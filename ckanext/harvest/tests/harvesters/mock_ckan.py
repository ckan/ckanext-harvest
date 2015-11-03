import json
import re
import copy

import SimpleHTTPServer
import SocketServer
from threading import Thread

PORT = 8998


class MockCkanHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_GET(self):
        # test name is the first bit of the URL and makes CKAN behave
        # differently in some way.
        # Its value is recorded and then removed from the path
        self.test_name = None
        test_name_match = re.match('^/([^/]+)/', self.path)
        if test_name_match:
            self.test_name = test_name_match.groups()[0]
            if self.test_name == 'api':
                self.test_name = None
            else:
                self.path = re.sub('^/([^/]+)/', '/', self.path)

        # The API version is recorded and then removed from the path
        api_version = None
        version_match = re.match('^/api/(\d)', self.path)
        if version_match:
            api_version = int(version_match.groups()[0])
            self.path = re.sub('^/api/(\d)/', '/api/', self.path)

        if self.path == '/api/rest/package':
            if api_version == 2:
                dataset_refs = [d['id'] for d in DATASETS]
            else:
                dataset_refs = [d['name'] for d in DATASETS]
            return self.respond_json(dataset_refs)
        if self.path.startswith('/api/rest/package/'):
            dataset_ref = self.path.split('/')[-1]
            dataset = self.get_dataset(dataset_ref)
            if dataset:
                return self.respond_json(
                    convert_dataset_to_restful_form(dataset))
        if self.path.startswith('/api/action/package_show'):
            params = self.get_url_params()
            dataset_ref = params['id']
            dataset = self.get_dataset(dataset_ref)
            if dataset:
                return self.respond_json(dataset)
        if self.path.startswith('/api/search/dataset'):
            params = self.get_url_params()
            if params.keys() == ['organization']:
                org = self.get_org(params['organization'])
                dataset_ids = [d['id'] for d in DATASETS
                               if d['owner_org'] == org['id']]
                return self.respond_json({'count': len(dataset_ids),
                                          'results': dataset_ids})
            else:
                return self.respond(
                    'Not implemented search params %s' % params, status=400)
        if self.path.startswith('/api/search/revision'):
            revision_ids = [r['id'] for r in REVISIONS]
            return self.respond_json(revision_ids)
        if self.path.startswith('/api/rest/revision/'):
            revision_ref = self.path.split('/')[-1]
            for rev in REVISIONS:
                if rev['id'] == revision_ref:
                    return self.respond_json(rev)
            self.respond('Cannot find revision', status=404)

        # if we wanted to server a file from disk, then we'd call this:
        #return SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

        self.respond('Mock CKAN doesnt recognize that call', status=400)

    def get_dataset(self, dataset_ref):
        for dataset in DATASETS:
            if dataset['name'] == dataset_ref or \
                    dataset['id'] == dataset_ref:
                if self.test_name == 'invalid_tag':
                    dataset['tags'] = INVALID_TAGS
                return dataset

    def get_org(self, org_ref):
        for org in ORGS:
            if org['name'] == org_ref or \
                    org['id'] == org_ref:
                return org

    def get_url_params(self):
        params = self.path.split('?')[-1].split('&')
        return dict([param.split('=') for param in params])

    def respond_action(self, result_dict, status=200):
        response_dict = {'result': result_dict}
        return self.respond_json(response_dict, status=status)

    def respond_json(self, content_dict, status=200):
        return self.respond(json.dumps(content_dict), status=status,
                            content_type='application/json')

    def respond(self, content, status=200, content_type='application/json'):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.end_headers()
        self.wfile.write(content)
        self.wfile.close()


def serve(port=PORT):
    '''Runs a CKAN-alike app (over HTTP) that is used for harvesting tests'''

    # Choose the directory to serve files from
    #os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)),
    #                      'mock_ckan_files'))

    class TestServer(SocketServer.TCPServer):
        allow_reuse_address = True

    httpd = TestServer(("", PORT), MockCkanHandler)

    print 'Serving test HTTP server at port', PORT

    httpd_thread = Thread(target=httpd.serve_forever)
    httpd_thread.setDaemon(True)
    httpd_thread.start()


def convert_dataset_to_restful_form(dataset):
    dataset = copy.deepcopy(dataset)
    dataset['extras'] = dict([(e['key'], e['value']) for e in dataset['extras']])
    dataset['tags'] = [t['name'] for t in dataset.get('tags', [])]
    return dataset


# Datasets are in the package_show form, rather than the RESTful form
DATASETS = [
    {'id': 'dataset1-id',
     'name': 'dataset1',
     'title': 'Test Dataset1',
     'owner_org': 'org1-id',
     'extras': []},
    {
    "id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
    "name": "cabinet-office-energy-use",
    "private": False,
    "maintainer_email": None,
    "revision_timestamp": "2010-11-23T22:34:55.089925",
    "organization":
    {

        "description": "The Cabinet Office supports the Prime Minister and Deputy Prime Minister, and ensure the effective running of government. We are also the corporate headquarters for government, in partnership with HM Treasury, and we take the lead in certain critical policy areas.\r\nCO is a ministerial department, supported by 18 agencies and public bodies\r\n\r\nYou can find out more at https://www.gov.uk/government/organisations/cabinet-office",
        "created": "2012-06-27T14:48:40.244951",
        "title": "Cabinet Office",
        "name": "cabinet-office",
        "revision_timestamp": "2013-04-02T14:27:23.086886",
        "is_organization": True,
        "state": "active",
        "image_url": "",
        "revision_id": "4be8825d-d3f4-4fb2-b80b-43e36f574c05",
        "type": "organization",
        "id": "aa1e068a-23da-4563-b9c2-2cad272b663e",
        "approval_status": "pending"

    },
    "update_frequency": "other",
    "metadata_created": "2010-08-02T09:19:47.600853",
    "last_major_modification": "2010-08-02T09:19:47.600853",
    "metadata_modified": "2014-05-09T22:00:01.486366",
    "temporal_granularity": "",
    "author_email": None,
    "geographic_granularity": "point",
    "geographic_coverage": [ ],
    "state": "active",
    "version": None,
    "temporal_coverage-to": "",
    "license_id": "uk-ogl",
    "type": "dataset",
    "published_via": "",
    "resources":
    [
    {
        "content_length": "69837",
        "cache_url": "http://data.gov.uk/data/resource_cache/f1/f156019d-ea88-46a6-8fa3-3d12582e2161/elec00.csv",
        "hash": "6f1e452320dafbe9a5304ac77ed7a4ff79bfafc3",
        "description": "70 Whitehall energy data",
        "cache_last_updated": "2013-06-19T00:59:42.762642",
        "url": "http://data.carbonculture.net/orgs/cabinet-office/70-whitehall/reports/elec00.csv",
        "openness_score_failure_count": "0",
        "format": "CSV",
        "cache_filepath": "/mnt/shared/ckan_resource_cache/f1/f156019d-ea88-46a6-8fa3-3d12582e2161/elec00.csv",
        "tracking_summary":
            {
                "total": 0,
                "recent": 0
            },
            "last_modified": "2014-05-09T23:00:01.435211",
            "mimetype": "text/csv",
            "content_type": "text/csv",
            "openness_score": "3",
            "openness_score_reason": "open and standardized format",
            "position": 0,
            "revision_id": "4fca759e-d340-4e64-b75e-22ee1d42c2b4",
            "id": "f156019d-ea88-46a6-8fa3-3d12582e2161",
            "size": 299107
        }
    ],
    "num_resources": 1,
    "tags":
    [
    {

        "vocabulary_id": None,
        "display_name": "consumption",
        "name": "consumption",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "id": "84ce26de-6711-4e85-9609-f7d8a87b0fc8"

    },
    {

        "vocabulary_id": None,
        "display_name": "energy",
        "name": "energy",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "id": "9f2ae723-602f-4290-80c4-6637ad617a45"

    }
    ],
    "precision": "",
    "tracking_summary":
    {
        "total": 0,
        "recent": 0
    },
    "taxonomy_url": "",
    "groups": [],
    "creator_user_id": None,
    "national_statistic": "no",
    "relationships_as_subject": [],
    "num_tags": 8,
    "update_frequency-other": "Real-time",
    "isopen": True,
    "url": "http://www.carbonculture.net/orgs/cabinet-office/70-whitehall/",
    "notes": "Cabinet Office head office energy use updated from on-site meters showing use, cost and carbon impact.",
    "owner_org": "aa1e068a-23da-4563-b9c2-2cad272b663e",
    "theme-secondary":
    [
        "Environment"
    ],
    "extras":
    [
    {
        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "key": "categories",
        "revision_id": "08bac459-1d44-44fb-b388-20f4d8394364",
        "id": "6813d71b-785b-4f56-b296-1b2acb34eed6"
    },
    {
        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "2010-07-30",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "key": "date_released",
        "revision_id": "08bac459-1d44-44fb-b388-20f4d8394364",
        "id": "515f638b-e2cf-40a6-a8a7-cbc8001269e3"
    },
    {

        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "key": "date_updated",
        "revision_id": "08bac459-1d44-44fb-b388-20f4d8394364",
        "id": "bff63465-4f96-44e7-bb87-6e66fff5e596"
    },
    {
        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "000000: ",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "key": "geographic_coverage",
        "revision_id": "08bac459-1d44-44fb-b388-20f4d8394364",
        "id": "414bcd35-b628-4218-99e2-639615183df8"
    },
    {
        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "point",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "key": "geographic_granularity",
        "revision_id": "08bac459-1d44-44fb-b388-20f4d8394364",
        "id": "c7b460dd-c61f-4cd2-90c2-eceb6c91fe9b"
    },
    {
        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "no",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "key": "national_statistic",
        "revision_id": "08bac459-1d44-44fb-b388-20f4d8394364",
        "id": "9f04b202-3646-49be-b69e-7fa997399ff3"
    },
    {
        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "{\"status\": \"final\", \"source\": \"Automatically awarded by ODI\", \"certification_type\": \"automatically awarded\", \"level\": \"raw\", \"title\": \"Cabinet Office 70 Whitehall energy use\", \"created_at\": \"2014-10-28T12:25:57Z\", \"jurisdiction\": \"GB\", \"certificate_url\": \"https://certificates.theodi.org/datasets/5480/certificates/17922\", \"badge_url\": \"https://certificates.theodi.org/datasets/5480/certificates/17922/badge.png\", \"cert_title\": \"Basic Level Certificate\"}",
        "revision_timestamp": "2014-11-12T02:52:35.048060",
        "state": "active",
        "key": "odi-certificate",
        "revision_id": "eae9763b-e258-4d76-9ec2-7f5baf655394",
        "id": "373a3cbb-d9c0-45a6-9a78-b95c86398766"
    },
    {
        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "key": "temporal_coverage-from",
        "revision_id": "08bac459-1d44-44fb-b388-20f4d8394364",
        "id": "39f72eed-6f76-4733-b636-7541cee3404f"
    },
    {
        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "key": "temporal_coverage-to",
        "revision_id": "08bac459-1d44-44fb-b388-20f4d8394364",
        "id": "818e2c8f-fee0-49da-8bea-ea3c9401ece5"
    },
    {
        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "key": "temporal_granularity",
        "revision_id": "08bac459-1d44-44fb-b388-20f4d8394364",
        "id": "f868b950-d3ce-4fbe-88ca-5cbc4b672320"
    },
    {
        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "Towns & Cities",
        "revision_timestamp": "2015-03-16T18:10:08.802815",
        "state": "active",
        "key": "theme-primary",
        "revision_id": "fc2b6630-84f8-4c88-8ac7-0ca275b2bc97",
        "id": "bdcf00fd-3248-4c2f-9cf8-b90706c88e8d"
    },
    {

        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "[\"Environment\"]",
        "revision_timestamp": "2015-04-08T20:57:04.895214",
        "state": "active",
        "key": "theme-secondary",
        "revision_id": "c2c48530-ff82-4af1-9373-cdc64d5bc83c",
        "id": "417482c5-a9c0-4430-8c4e-0c76e59fe44f"
    },
    {
        "package_id": "1c65c66a-fdec-4138-9c64-0f9bf087bcbb",
        "value": "Real-time",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "key": "update_frequency",
        "revision_id": "08bac459-1d44-44fb-b388-20f4d8394364",
        "id": "e8ad4837-514e-4446-81a2-ffacfa7cf683"
    }
    ],
    "license_url": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
    "individual_resources":
    [

    {

        "content_length": "69837",
        "cache_url": "http://data.gov.uk/data/resource_cache/f1/f156019d-ea88-46a6-8fa3-3d12582e2161/elec00.csv",
        "hash": "6f1e452320dafbe9a5304ac77ed7a4ff79bfafc3",
        "description": "70 Whitehall energy data",
        "cache_last_updated": "2013-06-19T00:59:42.762642",
        "url": "http://data.carbonculture.net/orgs/cabinet-office/70-whitehall/reports/elec00.csv",
        "openness_score_failure_count": "0",
        "format": "CSV",
        "cache_filepath": "/mnt/shared/ckan_resource_cache/f1/f156019d-ea88-46a6-8fa3-3d12582e2161/elec00.csv",
        "tracking_summary":

            {
                "total": 0,
                "recent": 0
            },
            "last_modified": "2014-05-09T23:00:01.435211",
            "mimetype": "text/csv",
            "content_type": "text/csv",
            "openness_score": "3",
            "openness_score_reason": "open and standardized format",
            "position": 0,
            "revision_id": "4fca759e-d340-4e64-b75e-22ee1d42c2b4",
            "id": "f156019d-ea88-46a6-8fa3-3d12582e2161",
            "size": 299107
        }

    ],
    "title": "Cabinet Office 70 Whitehall energy use",
    "revision_id": "3bd6ced3-35b2-4b20-94e2-c596e24bc375",
    "date_released": "30/7/2010",
    "theme-primary": "Towns & Cities"
}
]

INVALID_TAGS = [
    {
        "vocabulary_id": None,
        "display_name": "consumption%^&",
        "name": "consumption%^&",
        "revision_timestamp": "2010-08-02T09:19:47.600853",
        "state": "active",
        "id": "84ce26de-6711-4e85-9609-f7d8a87b0fc8"
    },
    ]

ORGS = [
    {'id': 'org1-id',
     'name': 'org1'},
    {'id': 'aa1e068a-23da-4563-b9c2-2cad272b663e',
     'name': 'cabinet-office'}
]

REVISIONS = [
    {
    "id": "23daf2eb-d7ec-4d86-a844-3924acd311ea",
    "timestamp": "2015-10-21T09:50:08.160045",
    "message": "REST API: Update object dataset1",
    "author": "ross",
    "approved_timestamp": None,
    "packages":
    [
        DATASETS[1]['name']
    ],
    "groups": [ ]
    },
    {
    "id": "8254a293-10db-4af2-9dfa-6a1f06ee899c",
    "timestamp": "2015-10-21T09:46:21.198021",
    "message": "REST API: Update object dataset1",
    "author": "ross",
    "approved_timestamp": None,
    "packages":
    [
        DATASETS[1]['name']
    ],
    "groups": [ ]
    }]
