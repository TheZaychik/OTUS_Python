import os
import re
import sys
import gzip
import json
import string
import logging
import statistics
from copy import copy
from datetime import datetime
from collections import namedtuple
from typing import Union
# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "SCRIPT_LOG_FILE": None,
    "ERROR_RATE": 51,
}


def _read_log(file_path: str, file_encoding: str = "utf-8"):
    with gzip.open(file_path, "r") if file_path.endswith(".gz") else open(file_path, "r", encoding=file_encoding) as f:
        line = f.readline()
        while line:
            yield line.decode()
            line = f.readline()
    logging.info(f'Data successfully read from {file_path}')


def _find_newest_log(_config: dict) -> Union[namedtuple, None]:
    Log = namedtuple("Log", "date filename path file_type")
    log = Log(datetime.now().date().min, "", "", "")
    files = os.listdir(_config["LOG_DIR"])
    for filename in files:
        regex = re.findall(r'^nginx-access-ui.log-(\d{8})(\.gz)?$', filename)
        if len(regex) > 0:
            file_date = datetime.strptime(regex[0][0], "%Y%m%d").date()
            if log.date < file_date:
                log = Log(file_date, filename, f'{_config["LOG_DIR"]}/{filename}', regex[0][1])
    logging.info(f'Find newest log - {log.filename}')
    if log.date == datetime.now().date().min:
        return None
    else:
        return log


def _get_prepared_data(log: namedtuple, _config: dict) -> namedtuple:
    RawData = namedtuple("RawData", "total_urls_count total_request_time errors urls_stat")
    total_urls_count = 0
    total_request_time = 0
    errors = 0
    urls_stat = {}

    log_file = _read_log(log.path)
    for line in log_file:
        regex = re.findall(r'^.* \"\w{2,6} (\/.*) HTTP/\d\.\d\".* (\d*\.\d*)$', line)
        if len(regex) > 0 and len(regex[0]) == 2:
            if regex[0][0] not in urls_stat.keys():
                urls_stat[regex[0][0]] = []
            urls_stat[regex[0][0]].append(float(regex[0][1]))
            total_request_time += float(regex[0][1])
            total_urls_count += 1
        else:
            errors += 1
    logging.info(f'Data prepared with url count - {total_urls_count}')
    return RawData(total_urls_count, total_request_time, errors, urls_stat)


def _calculate_stat(total_urls_count: int, total_request_time: float, urls_stat: dict, _config: dict) -> list:
    report = []
    for url in urls_stat.keys():
        report.append({
            "count": len(urls_stat[url]),
            "time_avg": round(statistics.fmean(urls_stat[url]), 3),
            "time_max": round(max(urls_stat[url]), 3),
            "time_sum": round(sum(urls_stat[url]), 3),
            "url": url,
            "time_med": round(statistics.median(urls_stat[url]), 3),
            "time_perc": round(sum(urls_stat[url]) / total_request_time * 100, 3),
            "count_perc": round(len(urls_stat[url]) / total_urls_count * 100, 3)
        })
    report.sort(key=lambda x: x["time_sum"], reverse=True)
    logging.info(f'Report completed and sorted')
    if len(report) >= _config["REPORT_SIZE"]:
        return report[0:_config["REPORT_SIZE"]]
    return report


def _write_report(report: list, log: namedtuple, _config: dict) -> None:
    with open('report-template.html', 'r') as f:
        raw = f.read()
    template = string.Template(raw)
    data = template.safe_substitute(table_json=json.dumps(report))
    with open(f'{_config["REPORT_DIR"]}/report-{log.date}.html', 'w') as f:
        f.write(data)
    logging.info(f'Report created at {_config["REPORT_DIR"]}/report-{log.date}.html')


def _setup_and_check(default_config: dict) -> dict:
    new_config = copy(default_config)
    use_external_config = '--config' in sys.argv
    path = 'config.json'
    if use_external_config:
        if sys.argv.index("--config") + 1 < len(sys.argv):
            path = sys.argv[sys.argv.index("--config") + 1]
        if not os.path.exists(path):
            raise FileNotFoundError(f"Отсутствует конфиг {path}")
        with open(path, 'r') as f:
            raw = f.read()
        try:
            imported_config = json.loads(raw)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError("Config is invalid", e.doc, e.pos)
        for k, v in imported_config.items():
            new_config[k] = v
    if not os.path.isdir(new_config["LOG_DIR"]):
        raise FileNotFoundError("LOG_DIR is invalid")
    if not os.path.isdir(new_config["REPORT_DIR"]):
        os.mkdir(new_config["REPORT_DIR"])
    return new_config


def create_report(_config: dict):
    log = _find_newest_log(_config)
    if log is None:
        logging.exception(f"Report doesnt exist")
        return
    if f'report-{log.date}.html' in os.listdir(_config["REPORT_DIR"]):
        logging.exception(f"A report with date {log.date} already exists")
        return
    raw_data = _get_prepared_data(log, _config)
    if raw_data.errors / raw_data.total_urls_count * 100 > _config['ERROR_RATE']:
        logging.exception("The percentage of errors is more than 51, abort")
        return
    report = _calculate_stat(raw_data.total_urls_count, raw_data.total_request_time, raw_data.urls_stat, _config)
    _write_report(report, log, _config)


def main() -> None:
    main_config = _setup_and_check(config)
    logging.basicConfig(
        level=logging.INFO,
        filename=main_config["SCRIPT_LOG_FILE"],
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S"
    )
    try:
        create_report(main_config)
    except Exception as e:
        logging.exception(str(e))
    except KeyboardInterrupt:
        logging.exception("Program stopped by Ctrl+C")


if __name__ == "__main__":
    main()
