import os.path

from common.ActionName import ActionName
from common.LocalUtils import LocalUtils
from common.ParserName import ParserName
from datetime import datetime
import json

from common.S3Utils import S3Utils


class ProcessLogs:
    def __init__(self,events,context):
        if "completed" in events:
            self.completed = events["completed"]
        else:
            self.completed = False

        if "previous_action" in events:
            self.previous_action = events["previous_action"]
        else:
            self.previous_action = ActionName.PROCESS_LOCATION.value

        self.has_more = events["has_more"]
        self.page_size = events["page_size"]
        self.use_proxy = events["use_proxy"]
        self.offset = events["offset"]

        self.running_in_lambda = False

        if "goto_next_step" in events:
            self.goto_next_step = events["goto_next_step"]
        if "force_fetch" in events:
            self.force_fetch = events["force_fetch"]
        if "offset_end" in events:
            self.offset_end = events["offset_end"]
        if "version" in events:
            self.version = events["version"]

        self.parser_names = list(ParserName.__members__.keys())
        if "log_file_path" in events:
            self.log_file_path = events["log_file_path"]

        if "log_id" in events:
            self.log_id = events["log_id"]


        if "AWS_LAMBDA_FUNCTION_VERSION" in os.environ:
            print("in lambda")
            self.bucket_name = self.log_file_path.split("/")[0]
            self.file_utils = S3Utils(self.bucket_name)
            self.running_in_lambda = True
            self.status_path = f"{self.log_file_path.split('/')[1]}/{self.log_file_path.split('/')[2]}/{self.log_file_path.split('/')[3]}/"
            self.local_data_path = "/tmp/" + self.log_file_path.split("/")[3]
            os.makedirs(self.local_data_path, exist_ok=True)


        print("Initialized ",self.previous_action)

    def read_log_file(self,filename):
        if self.running_in_lambda:
            self.log_file_path = os.path.join(self.local_data_path, self.status_path)
            if self.file_utils.file_exists(self.status_path,filename):
                print("Downloading from s3: " + str(os.path.join(self.status_path, filename)))
                self.file_utils.download_object(os.path.join(self.status_path, filename),
                                                os.path.join(self.local_data_path, self.status_path, filename))

        print(f"currently opening: {self.log_file_path}/{filename}")
        with open(os.path.join(self.log_file_path,filename),"r") as file:
            logs = file.readlines()

        return logs

    def process(self,parser_name=None):
        if parser_name:
            parser_value = ParserName[parser_name].value
            self.parser_names = [name for name, member in ParserName.__members__.items() if member.value == parser_value]

        if self.previous_action == ActionName.PROCESS_MENU.value:
            return self.process_logs("menu")
        elif self.previous_action == ActionName.PROCESS_LOCATION.value:
            return self.process_logs("locations")

    def process_logs(self,action_name):
        for parser in self.parser_names:
            success = 0
            failure = 0
            file_name = f"{parser}_{action_name}.log"
            print(f"\n\nProcessing {action_name} logs for",parser)
            logs = self.read_log_file(file_name)
            total_records = 0
            filtered_logs = []
            for log in logs:
                if log:
                    # print(log)
                    id = log.split(",")[2].strip()
                    if id == self.log_id:
                        total_records += 1
                        if log.split(",")[3] == "generate_files_list":
                            total_records -= 1
                        else:
                            filtered_logs.append(log)

            log_creation_date = datetime.strptime(self.log_id,"%Y%m%d%H%M%S").strftime("%Y-%m-%d_%H%M%S")
            for hits in filtered_logs:
                if "success" in hits:
                    success += 1
                else:
                    failure += 1

            if total_records > 0:
                percentage_success = (success/total_records)*100
                percentage_failure = (failure/total_records)*100
            else:
                percentage_success = 0.0
                percentage_failure = 0.0

            report = {
                "Parser_Name": parser,
                "Date_of_logs": log_creation_date,
                "Total_Hits": total_records,
                "Successful_Hits": success,
                "Percentage_Success": round(percentage_success,3),
                "Failed_Hits": failure,
                "Percentage_Failure": round(percentage_failure,3)
            }
            # reports.append(report)
            print(report)
            output_file_path = os.path.join(self.log_file_path,
                                            f"{parser}_{action_name}_log_report_{log_creation_date}.json")
            with open(output_file_path, "w", encoding="utf-8") as json_file:
                json_file.write(json.dumps(report))

            if self.running_in_lambda:
                print("Uploading to s3: " + output_file_path)
                self.file_utils.upload_object(output_file_path,os.path.join(self.status_path,f"{parser}_{action_name}_log_report_{log_creation_date}.json"))

            if action_name == "menu":

                if not self.goto_next_step:
                    return {
                        "parser": parser,
                        "action": "None",
                        "use_proxy": self.use_proxy,
                        "page_size": self.page_size,
                        "offset": 0,
                        "has_more": False,
                        "offset_end": self.offset_end,
                        "force_fetch": self.force_fetch,
                        "goto_next_step": self.goto_next_step
                    }

                next_action = ActionName.MAKE_CSV.value
                if (parser == ParserName.hardees.name
                        or parser == ParserName.cjr.name
                        or parser == ParserName.popeyes.name
                        or parser == ParserName.metro.name
                        or parser == ParserName.imtiaz.name):
                    next_action = ActionName.PROCESS_POST_MENU.value
                return {
                    "parser": parser,
                    "action": next_action,
                    "use_proxy": self.use_proxy,
                    "page_size": self.page_size,
                    "offset": 0,
                    "has_more": True,
                    "offset_end": self.offset_end,
                    "force_fetch": self.force_fetch,
                    "goto_next_step": self.goto_next_step,
                    "completed": self.completed,
                    "version": self.version
                }

            elif action_name == "locations":
                return {
                    "parser": parser,
                    "action": ActionName.PROCESS_MENU.value,
                    "use_proxy": self.use_proxy,
                    "page_size": self.page_size,
                    "offset": 0,
                    "has_more": True,
                    "completed": self.completed,
                    "version": self.version
                }
