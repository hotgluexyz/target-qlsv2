from singer_sdk.sinks import RecordSink
import json
import ast
from target_qlsv2.rest import Rest


class QlsV2Sink(RecordSink, Rest):
    """WoocommerceSink target sink class."""

    @property
    def name(self):
        raise NotImplementedError

    @property
    def endpoint(self):
        raise NotImplementedError

    @property
    def unified_schema(self):
        raise NotImplementedError

    @property
    def base_url(self):
        company_id = self.config["company_id"]
        return f"https://api.pakketdienstqls.nl/v2/companies/{company_id}/"
    

    def url(self, endpoint=None):
        if not endpoint:
            endpoint = self.endpoint
        return f"{self.base_url}{endpoint}"

    def validate_input(self, record: dict):
        return self.unified_schema(**record).dict()

    def validate_output(self, mapping):
        payload = self.clean_payload(mapping)
        # Add validation logic here
        return payload

    def get_reference_data(self, stream, fields=None, filter={}):
        page = 1
        data = []
        params = {"per_page": 100, "order": "asc", "page": page}
        params.update(filter)
        while True:
            resp = self.request_api("GET", stream, params)
            total_pages = resp.headers.get("X-WP-TotalPages")
            resp = resp.json()
            if fields:
                resp = [{k: v for k, v in r.items() if k in fields} for r in resp]
            data += resp

            if resp and int(total_pages) > page:
                page += 1
                params.update({"page": page})
            else:
                break
        return data

    def init_state(self):
        self.latest_state = self.latest_state or {"bookmarks": {}}
        if self.name not in self.latest_state["bookmarks"]:
            if not self.latest_state["bookmarks"].get(self.name):
                self.latest_state["bookmarks"][self.name] = []


    def parse_stringified_object(self, stringified_object):
        try: # Python obj notation
            obj = ast.literal_eval(stringified_object)
            return obj
        except: # JS Objection notation
            obj = json.loads(stringified_object)
            return obj
