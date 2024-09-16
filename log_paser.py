"""MEGA app log parser.

some functions are copied and modified from megacmd_logparser.py in act project
"""
import json
import re
import statistics
from datetime import datetime

import logger

log = logger.get_logger(__file__)


class LogParser:
    """Log parser class."""

    def __init__(self, file_path) -> None:
        self.log_path = file_path
        self.full_events = []
        self.events = []
        self.error_events = []
        self.error_events_type_count = {}
        self.error_event_with_trace = []
        self.events_time_delta = None
        self.order_request_events = []
        self.accepted_order_request_events = []
        self.accepted_order_request_type_count = {}
        self.reject_order_request_events = []
        self.reject_order_request_type_count = {}

    def parse_events(self, start_datetime=None, end_datetime=None):
        """Parse events.

        Note:
            start_datetime and end_datetime needs to be in example "2018-10-11 15:50:42.284" format
        """
        with open(self.log_path, 'r', encoding='utf-8') as f:
            while True:
                line = f.readline()
                if not line:
                    break  # No more lines to read
                event = self._extract_event(line)
                if not event:
                    continue
                if event['parsed_status'] == 'good':
                    self.full_events.append(event)
                elif 'error_trace' in self.full_events[-1]:
                    self.full_events[-1]['error_trace'].append(line)
                else:
                    self.full_events[-1]['error_trace'] = [line]
        start_dt = datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S.%f") if start_datetime else None
        end_dt = datetime.strptime(end_datetime, "%Y-%m-%d %H:%M:%S.%f") if end_datetime else None
        for event in self.full_events:
            if (not start_dt or start_dt <= event['timestamp']) and (not end_dt or end_dt >= event['timestamp']):
                self.events.append(event)
                if event['level'] == 'ERROR':
                    self.error_events.append(event)
                    if event['message'] in self.error_events_type_count:
                        self.error_events_type_count[event['message']] += 1
                    else:
                        self.error_events_type_count[event['message']] = 1
                if 'error_trace' in event:
                    self.error_event_with_trace.append(event)
                if 'type' in event and event['type'] == 'order_request':
                    self.order_request_events.append(event)
                    if event['http_status'] == '200':
                        self.accepted_order_request_events.append(event)
                        if event['order_request_status'] in self.accepted_order_request_type_count:
                            self.accepted_order_request_type_count[event['order_request_status']] += 1
                        else:
                            self.accepted_order_request_type_count[event['order_request_status']] = 1
                    else:
                        self.reject_order_request_events.append(event)
                        key = f"http_status:{event['http_status']} - reject_code:{event['reject_code']} - reject_message:{event['reject_message']}"
                        if key in self.reject_order_request_type_count:
                            self.reject_order_request_type_count[key] += 1
                        else:
                            self.reject_order_request_type_count[key] = 1

        self.events_time_delta = self.events[-1]['timestamp'] - self.events[0]['timestamp']

    def _extract_event(self, line):
        """Exact event."""
        parsed_log = {'parsed_status': 'good'}
        log_pattern = re.compile(r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3})"
                                 r"\[(?P<epoch_time>\d+)\] \| (?P<level>\S+)\s*\| (?P<thread>\S+)\s*\| "
                                 r"(?P<package_namespace>\S+)\s*- (?P<message>.+)")
        match = log_pattern.match(line.strip())
        if not match:
            # cannot parse this, this should be part of the exception trace for previous event
            parsed_log['parsed_status'] = 'bad'
            return parsed_log
        parsed_log.update({
            'timestamp': datetime.strptime(match.group("timestamp"), "%Y-%m-%d %H:%M:%S.%f"),
            'epoch_time': int(match.group("epoch_time")),
            'level': match.group("level"),
            'thread': match.group("thread"),
            'package_namespace': match.group("package_namespace"),
            'message': match.group("message"),
        })
        message = parsed_log['message']
        if 'POST /api/v3/order' in message:
            parsed_log['type'] = 'order_request'

            pattern = r".*httpStatus:(?P<http_status>\d+).*"
            match = re.search(pattern, message)
            if match:
                parsed_log['http_status'] = match.group("http_status")
            else:
                log.warning(f'No order request http_status: {line}')
                parsed_log['http_status'] = 'No http_status'

            pattern = r".*\"status\":\"(?P<status>\w+)\".*"
            match = re.search(pattern, message)
            if match:
                parsed_log['order_request_status'] = match.group("status")
            elif parsed_log['http_status'] == '200':
                parsed_log['order_request_status'] = 'active in future'
            else:
                parsed_log['order_request_status'] = 'reject'
                pattern = r".*\"code\":(?P<code>.+),\"msg\":\"(?P<msg>.+)\".*"
                match = re.search(pattern, message)
                if match:
                    parsed_log['reject_code'] = match.group("code")
                    parsed_log['reject_message'] = match.group("msg")
                else:
                    log.warning(f'Cannot find reject code and reject message: {line}')

            pattern = r".*proc:(?P<process_time>\S+).*"
            match = re.search(pattern, message)
            if match:
                parsed_log['process_time_ms'] = float(match.group("process_time")[:-2])
            else:
                log.warning(f'Cannot find process_time: {line}')

        return parsed_log

    def get_timestamp(self, event_type, last=False):
        """Get timestamp."""
        events = reversed(self.events) if last else self.events
        return next((e['timestamp'] for e in events if e['type'] == event_type), None)

    def get_transfer_total_size(self, event_type, last=False):
        """Get timestamp."""
        events = reversed(self.events) if last else self.events
        return next((e['total_size'] for e in events if e['type'] == event_type), None)

    def get_speed_metrics(self):
        """Get speed metrics."""
        self.parse_events()
        metrics = []
        for e in self.events:
            if e['type'] == 'transfer_in_progress':
                metrics.append({
                    'timestamp': e['timestamp'],
                    'speed': e['speed'],
                    'mean_speed': e['mean_speed'],
                    'transfer_name': e['transfer_name'],
                    'transfer_size': e['transfer_size']
                })
        return metrics

    def get_transfer_request_failure_events(self):
        """Get speed metrics."""
        self.parse_events()
        transfer_request_failure_events = []

        for e in self.events:
            if e['type'] == 'transfer_failure':
                transfer_request_failure_events.append(e['transfer_failure_details'])
        return transfer_request_failure_events

    def show_summary(self):
        """Show log parsing summary."""
        summary = []
        summary.append(f'Parsed file: {self.log_path}')
        summary.append(f'Event count: {len(self.events)}')
        summary.append(f'Error event count: {len(self.error_events)}')
        summary.append((f'Error event type and count: {json.dumps(self.error_events_type_count, indent=4)}'))
        summary.append(f'Error event with exception trace count: {len(self.error_event_with_trace)}')
        summary.append(f'Event duration: {self.events_time_delta}')
        summary.append((f'Order requests count: {len(self.order_request_events)}'))
        summary.append((f'Accepted order requests count: {len(self.accepted_order_request_events)}'))
        summary.append((f'Accepted order requests status type and count: {json.dumps(self.accepted_order_request_type_count, indent=4)}'))
        summary.append((f'Rejected order requests count: {len(self.reject_order_request_events)}'))
        summary.append((f'Rejected order requests status type and count: {json.dumps(self.reject_order_request_type_count, indent=4, sort_keys=True)}'))

        summary.append(f'Average order request rate: {len(self.order_request_events) / self.events_time_delta.total_seconds()} requests per second')
        summary.append(f'Average order request process time: {statistics.mean([x["process_time_ms"] for x in self.order_request_events])} ms')
        # min
        # max

        # sell or buy distribution
        # symbol distribution
        # unique ip address

        # Message Queue Statistics
        #
        # Average Send Queue Size: 0 (based on available data)
        # Average Recv Processor's CallbackProcessQueue Size: 1 (based on available data)
        # Average RecvMsgDistRunnable's ProcessQueue Size: 0 (based on available data)
        log.info('\n'.join(summary))
