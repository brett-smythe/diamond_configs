"""Collector for interns requests to twitter's timeline API"""
from datetime import datetime, timedelta

import pytz

import diamond.collector


class InternsTwitterTimelineCollector(diamond.collector.Collector):
    """Collects stats for twitter timeline API requests"""
    # pylint: disable=too-many-locals

    def collect(self):
        """Collect twitter API timeline request metrics"""
        interns_service_logs_dir = '/var/log/interns-service/'
        interns_base_log_name = 'tasks.log'

        def parse_log(log_path, oldest_datetime):
            """When given a log at log_path, parse the log for timeline
            requests
            """
            timeline_req_line = 'Making twitter timeline request'
            date_format = '%Y-%m-%d %H:%M:%S'
            valid_requests = 0
            with open(log_path) as inf:
                for line in inf:
                    line_data = line.split('-')

                    time_data = line_data[0:3]
                    timestamp = '-'.join(time_data).split(',')[0]
                    log_datetime = datetime.strptime(timestamp, date_format)
                    if log_datetime < oldest_datetime:
                        continue
                    log_msg = line_data[-1].strip()
                    if log_msg == timeline_req_line:
                        valid_requests += 1
            return valid_requests

        def is_dst_currently():
            """Check to see if PST is currently in daylight savings"""
            tz = pytz.timezone('America/Los_Angeles')
            now = pytz.utc.localize(datetime.utcnow())
            return now.astimezone(tz).dst() != timedelta(0)

        number_of_tl_reqs = 0
        utc_now = datetime.utcnow()
        utc_hour = utc_now.hour
        poll_window = 60 * 15
        offset = timedelta(seconds=poll_window)
        oldest_dt = utc_now - offset
        if oldest_dt.hour != utc_hour:
            # So for whatever reason the logs are named as if they are in
            # pacific time even thought the log timestamps AND server time is
            # both UTC.
            hour_offset = 8
            if is_dst_currently():
                hour_offset = 7
            log_file_offset = timedelta(hours=hour_offset, seconds=poll_window)
            log_file_dt = utc_now - log_file_offset
            # Have to also pull the previous hour logs as well
            old_log_suffix = log_file_dt.strftime('%Y-%m-%d_%H')
            older_log_name = '{0}{1}.{2}'.format(
                interns_service_logs_dir,
                interns_base_log_name,
                old_log_suffix
            )
            number_of_tl_reqs += parse_log(older_log_name, oldest_dt)

        current_log_name = '{0}{1}'.format(
            interns_service_logs_dir, interns_base_log_name
        )
        number_of_tl_reqs += parse_log(current_log_name, oldest_dt)
        metric_name = 'twitter_api.tl_reqs_past_15'
        metric_value = number_of_tl_reqs

        self.publish(metric_name, metric_value)
