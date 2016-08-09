"""Collector for interns requests to twitter's timeline API"""
import diamond.collector

from datetime import datetime, timedelta


class InternsTwitterTimeline(diamond.collector.Collector):
    """Collects stats for twitter timeline API requests"""
    interns_service_logs_dir = '/var/log/interns-service/'
    interns_base_log_name = 'tasks.log'
    timeline_req_line = 'Making twitter timeline request'
    date_format = '%Y-%m-%d %H:%M:%S'

    def parse_log(self, log_path, oldest_datetime):
        """When given a log at log_path, parse the log for timeline requests"""
        valid_requests = 0
        with open(log_path) as inf:
            for line in inf:
                line_data = line.split('-')

                time_data = line_data[0:3]
                timestamp = '-'.join(time_data).split(',')[0]
                log_datetime = datetime.strptime(timestamp, self.date_format)
                if log_datetime < oldest_datetime:
                    continue
                log_msg = line_data[-1].strip()
                if log_msg == self.timeline_req_line:
                    valid_requests += 1
        return valid_requests

    def collect(self):
        """Collect twitter API timeline request metrics"""
        number_of_tl_reqs = 0
        utc_now = datetime.utcnow()
        utc_hour = utc_now.hour
        poll_window = 60 * 15
        offset = timedelta(seconds=poll_window)
        oldest_dt = utc_now - offset
        if oldest_dt.hour != utc_hour:
            # Have to also pull the previous hour logs as well
            older_log_name = '{0}.{1}-{2}-{3}_{4}'.format(
                self.interns_base_log_name,
                oldest_dt.year,
                oldest_dt.month,
                oldest_dt.day,
                oldest_dt.hour
            )
            number_of_tl_reqs += self.parse_log(older_log_name, oldest_dt)
        current_log_name = '{0}{1}'.format(
            self.interns_service_logs_dir, self.interns_base_log_name
        )
        number_of_tl_reqs += self.parse_log(current_log_name, oldest_dt)

        self.publish('twitter_tl_reqs_past 15', number_of_tl_reqs)
