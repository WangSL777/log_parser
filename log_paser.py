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
        self.buy_request_events = []
        self.sell_request_events = []
        self.accepted_order_request_events = []
        self.accepted_order_request_type_count = {}
        self.reject_order_request_events = []
        self.reject_order_request_type_count = {}
        self.order_request_symbol_count = {}
        self.order_request_client_ip_count = {}
        self.rmq_send_events = []
        self.rmq_send_queue_size_events = []
        self.rmq_recv_events = []
        self.rmq_recv_callback_process_queue_size_events = []
        self.rmq_recv_msg_dist_runnable_process_queue_size_events = []
        self.thread_count = {}

        self.results_bag = {}

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

                        # side
                        if event['side'] == 'BUY':
                            self.buy_request_events.append(event)
                        elif event['side'] == 'SELL':
                            self.sell_request_events.append(event)
                        else:
                            log.warning(f'Get unknown side type "{event["side"]}"')

                        # symbol
                        if event['symbol'] in self.order_request_symbol_count:
                            self.order_request_symbol_count[event['symbol']] += 1
                        else:
                            self.order_request_symbol_count[event['symbol']] = 1
                    else:
                        self.reject_order_request_events.append(event)
                        key = f"http_status:{event['http_status']} - reject_code:{event['reject_code']} - reject_message:{event['reject_message']}"
                        if key in self.reject_order_request_type_count:
                            self.reject_order_request_type_count[key] += 1
                        else:
                            self.reject_order_request_type_count[key] = 1

                    # client ip
                    if event['client_ip'] in self.order_request_client_ip_count:
                        self.order_request_client_ip_count[event['client_ip']] += 1
                    else:
                        self.order_request_client_ip_count[event['client_ip']] = 1
                elif 'type' in event and event['type'] == 'rmq_sent':
                    self.rmq_send_events.append(event)
                elif 'type' in event and event['type'] == 'rmq_send_queue_size':
                    self.rmq_send_queue_size_events.append(event)
                elif 'type' in event and event['type'] == 'rmq_receive':
                    self.rmq_recv_events.append(event)
                elif 'type' in event and event['type'] == 'rmq_recv_callback_process_queue_size':
                    self.rmq_recv_callback_process_queue_size_events.append(event)
                elif 'type' in event and event['type'] == 'rmq_msg_dist_runnable_process_queue_size':
                    self.rmq_recv_msg_dist_runnable_process_queue_size_events.append(event)

                # thread
                if event['thread'] in self.thread_count:
                    self.thread_count[event['thread']] += 1
                else:
                    self.thread_count[event['thread']] = 1

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

            # "side":"BUY", side:BUY
            pattern = r".*(\")?side(\")?:(\")?(?P<side>\w+)(\")?.*"
            match = re.search(pattern, message)
            if match:
                parsed_log['side'] = match.group("side")
            elif parsed_log['http_status'] == '200':
                log.warning(f'Cannot find side: {line}')

            # "symbol":"1539240640083"
            pattern = r".*\"symbol\":\"(?P<symbol>\w+)\".*"
            match = re.search(pattern, message)
            if match:
                parsed_log['symbol'] = match.group("symbol")
            elif parsed_log['http_status'] == '200':
                log.warning(f'Cannot find symbol: {line}')

            # client ip, [FROM]: [114.236.238.230]
            pattern = r".*\[FROM\]: \[(?P<client_ip>\d+\.\d+\.\d+\.\d+)\].*"
            match = re.search(pattern, message)
            if match:
                parsed_log['client_ip'] = match.group("client_ip")
            else:
                log.warning(f'Cannot find client_ip: {line}')
        elif "SEND THREAD: cId:" in line and "SENT" in line:
            parsed_log['type'] = 'rmq_sent'
        elif "Send thread's send queue size:" in line:
            parsed_log['type'] = 'rmq_send_queue_size'
            pattern = r".*Send thread's send queue size: (?P<rmq_send_queue_size>\d+).*"
            match = re.search(pattern, message)
            if match:
                parsed_log['rmq_send_queue_size'] = int(match.group("rmq_send_queue_size"))
            else:
                log.warning(f'Cannot find rmq_send_queue_size: {line}')
        elif "RECV of Message Envelope cId:" in line:
            parsed_log['type'] = 'rmq_receive'
        elif "Recv Processor's CallbackProcessQueue size:" in line:
            parsed_log['type'] = 'rmq_recv_callback_process_queue_size'
            pattern = r".*Recv Processor's CallbackProcessQueue size: (?P<rmq_recv_callback_process_queue_size>\d+).*"
            match = re.search(pattern, message)
            if match:
                parsed_log['rmq_recv_callback_process_queue_size'] = int(match.group("rmq_recv_callback_process_queue_size"))
            else:
                log.warning(f'Cannot find rmq_recv_callback_process_queue_size: {line}')
        elif "RecvMsgDistRunnable's processQueue size:" in line:
            parsed_log['type'] = 'rmq_msg_dist_runnable_process_queue_size'
            pattern = r".*RecvMsgDistRunnable's processQueue size: (?P<rmq_recv_msg_dist_runnable_process_queue_size>\d+).*"
            match = re.search(pattern, message)
            if match:
                parsed_log['rmq_recv_msg_dist_runnable_process_queue_size'] = int(match.group("rmq_recv_msg_dist_runnable_process_queue_size"))
            else:
                log.warning(f'Cannot find rmq_recv_msg_dist_runnable_process_queue_size: {line}')

        return parsed_log

    def get_timestamp(self, event_type, last=False):
        """Get timestamp."""
        events = reversed(self.events) if last else self.events
        return next((e['timestamp'] for e in events if e['type'] == event_type), None)

    def generate_results(self):
        """Generate results and store in results_bag."""
        self.results_bag = {
            'Parsed file': self.log_path,
            'Event count': len(self.events),
            'Error event count': len(self.error_events),
            'Error event type and count:': self.error_events_type_count,
            'Error event with exception trace count': len(self.error_event_with_trace),
            'Parsed log duration': f'{self.events_time_delta}',

            'Order requests count': len(self.order_request_events),
            'Accepted order requests count': len(self.accepted_order_request_events),
            'Accepted order requests status type and count': self.accepted_order_request_type_count,
            'Rejected order requests count': len(self.reject_order_request_events),
            'Rejected order requests status type and count': self.reject_order_request_type_count,

            'Average order request rate': f'{len(self.order_request_events) / self.events_time_delta.total_seconds()} requests per second'
        }
        process_times = [x["process_time_ms"] for x in self.order_request_events]
        self.results_bag['Average order request process time'] = f'{statistics.mean(process_times)} ms'
        self.results_bag['Min order request process time'] = f'{min(process_times)} ms'
        self.results_bag['Max order request process time'] = f'{max(process_times)} ms'

        # sell or buy
        self.results_bag['Sell requests count'] = len(self.sell_request_events)
        process_times = [x["process_time_ms"] for x in self.sell_request_events]
        self.results_bag['Average sell request rate'] = f'{len(self.sell_request_events) / self.events_time_delta.total_seconds()} requests per second'
        self.results_bag['Min sell request rate'] = f'{min(process_times)} ms'
        self.results_bag['Max sell request rate'] = f'{max(process_times)} ms'

        self.results_bag['Buy requests count'] = len(self.buy_request_events)
        process_times = [x["process_time_ms"] for x in self.buy_request_events]
        self.results_bag['Average buy request rate'] = f'{len(self.buy_request_events) / self.events_time_delta.total_seconds()} requests per second'
        self.results_bag['Min buy request process time'] = f'{min(process_times)} ms'
        self.results_bag['Max buy request process time'] = f'{max(process_times)} ms'

        # symbol distribution
        self.results_bag['Order requests symbol and count'] = self.order_request_symbol_count

        # unique ip address
        self.results_bag['Order requests client IP and count'] = self.order_request_client_ip_count

        # process thread
        self.results_bag['Process thread count'] = self.thread_count

        # Message Queue Stats
        self.results_bag['RMQ sent event count'] = len(self.rmq_send_events)
        queue_size = [x["rmq_send_queue_size"] for x in self.rmq_send_queue_size_events]
        self.results_bag['Average RMQ sent queue size'] = statistics.mean(queue_size)
        self.results_bag['Min RMQ sent queue size'] = min(queue_size)
        self.results_bag['Max RMQ sent queue size'] = max(queue_size)

        self.results_bag['RMQ receive event count'] = len(self.rmq_recv_events)
        queue_size = [x["rmq_recv_callback_process_queue_size"] for x in self.rmq_recv_callback_process_queue_size_events]
        self.results_bag['Average RMQ receive callback process queue size'] = statistics.mean(queue_size)
        self.results_bag['Min RMQ receive callback process queue size'] = min(queue_size)
        self.results_bag['Max RMQ receive callback process queue size'] = max(queue_size)
        queue_size = [x["rmq_recv_msg_dist_runnable_process_queue_size"] for x in self.rmq_recv_msg_dist_runnable_process_queue_size_events]
        self.results_bag['Average RMQ receive msg dist runnable process queue size'] = statistics.mean(queue_size)
        self.results_bag['Min RMQ receive msg dist runnable process queue size'] = min(queue_size)
        self.results_bag['Max RMQ receive msg dist runnable process queue size'] = max(queue_size)

        log.info(f'{json.dumps(self.results_bag, indent=4)}')

    def show_summary(self):
        """Show log parsing summary."""
        summary = []
        summary.append(f'Parsed file: {self.log_path}')
        summary.append(f'Event count: {len(self.events)}')
        summary.append(f'Error event count: {len(self.error_events)}')
        summary.append((f'Error event type and count: {json.dumps(self.error_events_type_count, indent=4)}'))
        summary.append(f'Error event with exception trace count: {len(self.error_event_with_trace)}')
        summary.append(f'Parsed log duration: {self.events_time_delta}')
        summary.append((f'\nOrder requests count: {len(self.order_request_events)}'))
        summary.append((f'Accepted order requests count: {len(self.accepted_order_request_events)}'))
        summary.append((f'Accepted order requests status type and count: {json.dumps(self.accepted_order_request_type_count, indent=4)}'))
        summary.append((f'Rejected order requests count: {len(self.reject_order_request_events)}'))
        summary.append((f'Rejected order requests status type and count: {json.dumps(self.reject_order_request_type_count, indent=4, sort_keys=True)}'))

        summary.append(f'\nAverage order request rate: {len(self.order_request_events) / self.events_time_delta.total_seconds()} requests per second')
        process_times = [x["process_time_ms"] for x in self.order_request_events]
        summary.append(f'Average order request process time: {statistics.mean(process_times)} ms')
        summary.append(f'Min order request process time: {min(process_times)} ms')
        summary.append(f'Max order request process time: {max(process_times)} ms')

        # sell or buy
        summary.append((f'\nSell requests count: {len(self.sell_request_events)}'))
        summary.append(f'Average sell request rate: {len(self.sell_request_events) / self.events_time_delta.total_seconds()} requests per second')
        process_times = [x["process_time_ms"] for x in self.sell_request_events]
        summary.append(f'Average sell request process time: {statistics.mean(process_times)} ms')
        summary.append(f'Min sell request process time: {min(process_times)} ms')
        summary.append(f'Max sell request process time: {max(process_times)} ms')

        summary.append((f'\nBuy requests count: {len(self.buy_request_events)}'))
        summary.append(f'Average buy request rate: {len(self.buy_request_events) / self.events_time_delta.total_seconds()} requests per second')
        process_times = [x["process_time_ms"] for x in self.buy_request_events]
        summary.append(f'Average buy request process time: {statistics.mean(process_times)} ms')
        summary.append(f'Min buy request process time: {min(process_times)} ms')
        summary.append(f'Max buy request process time: {max(process_times)} ms')

        # symbol distribution
        summary.append((f'\nOrder requests symbol and count: {json.dumps(self.order_request_symbol_count, indent=4)}'))

        # unique ip address
        summary.append((f'\nOrder requests client IP and count: {json.dumps(self.order_request_client_ip_count, indent=4)}'))

        # Message Queue Stats
        summary.append((f'\nRMQ sent event count: {len(self.rmq_send_events)}'))
        queue_size = [x["rmq_send_queue_size"] for x in self.rmq_send_queue_size_events]
        summary.append(f'Average RMQ sent queue size: {statistics.mean(queue_size)}')
        summary.append(f'Min RMQ sent queue size: {min(queue_size)}')
        summary.append(f'Max RMQ sent queue size: {max(queue_size)}')

        summary.append((f'\nRMQ receive event count: {len(self.rmq_recv_events)}'))
        queue_size = [x["rmq_recv_callback_process_queue_size"] for x in self.rmq_recv_callback_process_queue_size_events]
        summary.append(f'Average RMQ receive callback process queue size: {statistics.mean(queue_size)}')
        summary.append(f'Min RMQ receive callback process queue size: {min(queue_size)}')
        summary.append(f'Max RMQ receive callback process queue size: {max(queue_size)}')
        queue_size = [x["rmq_recv_msg_dist_runnable_process_queue_size"] for x in self.rmq_recv_msg_dist_runnable_process_queue_size_events]
        summary.append(f'Average RMQ receive msg dist runnable process queue size: {statistics.mean(queue_size)}')
        summary.append(f'Min RMQ receive msg dist runnable process queue size: {min(queue_size)}')
        summary.append(f'Max RMQ receive msg dist runnable process queue size: {max(queue_size)}')

        # unique ip address
        summary.append((f'\nProcess thread count: {json.dumps(self.thread_count, indent=4)}'))
        log.info('\n'.join(summary))

