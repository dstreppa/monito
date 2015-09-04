# Import section

import psutil
import time
import os
import sys
import argparse
from ConfigParser import ConfigParser
from influxdb import InfluxDBClient


# Classes section


class InfluxDBComm:

    """Communication with an InfluxDB server using InfluxDBClient object"""

    influx_client = None
    """ InfluxDb client"""

    configuration_parser = None
    """ Configuration parser"""

    data_points = []
    """ Data points"""

    def __init__(self, parser, influx_client):

        """Constructor
        :param parser: configuration parser
        :type parser: ConfigParser
        :param influx_client: influxDB client
        :type influx_client: InfluxDBClient
        """

        self.influx_client = influx_client
        self.configuration_parser = parser
        self.data_points = []

    def manage(self, host, process, cpu_perc, mem_perc, timestamp):

        """Manage a point relevation, if the maximum length os self.data_points is reached, then send data to InfluxDB server
        :param host: host where the process is running
        :type host: string
        :param process: name of the process | OS
        :type process: string
        :param cpu_perc: %usage of CPU
        :type cpu_perc: float
        :param mem_perc: %usage of RAM memory
        :type mem_perc: float
        :param timestamp: UNIX timestamp related to relevation (UTC)
        :type timestamp: int
        """

        measurement = self.configuration_parser.get('influxdb_section', 'influxdb_measurement')
        max_lines_data = int(self.configuration_parser.get('utils_section', 'max_lines_data_in_post_request'))

        str_to_append = measurement + ",host=" + host + ",process=" + process + " cpu_perc=" + str(cpu_perc) + \
                        ",mem_perc=" + str(mem_perc) + " " + str(timestamp)
        if self.configuration_parser.get('utils_section', 'print_data') == "enabled":
            print str_to_append

        # Create a point
        point = {
                    'time': int(timestamp),
                    'measurement': measurement,
                    'fields':  dict(cpu_perc=float(cpu_perc), mem_perc=float(mem_perc)),
                    'tags': dict(host=host, process=process)
                }
        self.data_points.append(point)

        if len(self.data_points) >= max_lines_data:
            print "\nSending data to socket [" + self.configuration_parser.get('influxdb_section', 'influxdb_host') + \
                  ":" + self.configuration_parser.get('influxdb_section', 'influxdb_port') + "]...",
            res = self.influx_client.write_points(self.data_points, time_precision='s')
            if res is True:
                print "...OK\n"
            else:
                print "...FAIL\n"
            self.data_points = []

# Functions section


def load_process_list(process_names_array):
    """ Send data to InfluxDB using CURL external program
    :param process_names_array: list with the names of the processes to monitor
    :type process_names_array: array of strings
    :returns: list of the processes to monitor
    :rtype: array of Process objects
    """
    print("Loading processes list..."),
    pids_list = []
    for process_name in process_names_array:
        for ps in psutil.process_iter():
            ps_name = ps.as_dict(attrs=['pid', 'name'])
            if ps_name['name'] == process_name:
                pids_list.append(psutil.Process(ps_name['pid']))
                break

    print("...done  ")
    return pids_list


def processes_monitoring(parser, influxdbConn):
    """ Monitor the processes and the OS
    :param parser: Configuration file data
    :type parser: ConfigParser object
    :param influxdbConn: influxDB client
    :type influxdbConn: InfluxDBClient
    """

    # Initialization of the processes' list
    cnt_reload_process_list = 1
    processes_list = load_process_list(parser.get('process_section', 'processes_list').split(";"))

    # Initialization of the InfluxComm object
    influx_com = InfluxDBComm(parser, influxdbConn)

    while 1:

        # OS section
        if parser.get('process_section', 'os_flag') == "enabled":

            influx_com.manage(os.uname()[1], "OS", float(round(psutil.cpu_percent(), 2)),
                              float(round(psutil.virtual_memory()[2], 2)), int(time.time()))

        # Processes section
        if len(processes_list) > 0:
            for process in processes_list:
                try:
                    influx_com.manage(os.uname()[1], process.name, float(round(process.get_cpu_percent(), 2)),
                                      float(round(process.get_memory_percent(), 2)), int(time.time()))

                except psutil.NoSuchProcess:
                    print process.name + " - no process running"
        else:
            print "No process to monitor"

        time.sleep(int(parser.get('utils_section', 'delay_after_request')))

        # Reload of the processes list to update pids
        if cnt_reload_process_list >= int(parser.get('process_section', 'num_cycles_to_reload_processes_list')):
            processes_list = load_process_list(parser.get('process_section', 'processes_list').split(";"))
            cnt_reload_process_list = 1
        else:
            cnt_reload_process_list += 1

    influx_com = None
    del influx_com

# Main section

input_args_parser = argparse.ArgumentParser()
input_args_parser.add_argument("--config_file", help="configuration file path")
input_args = input_args_parser.parse_args()

if not input_args.config_file:
    input_args_parser.print_help()
    sys.exit("")

conf_parser = ConfigParser()
conf_parser.read(input_args.config_file)

influxdbConn = InfluxDBClient(conf_parser.get('influxdb_section', 'influxdb_host'),
                              int(conf_parser.get('influxdb_section', 'influxdb_port')),
                              conf_parser.get('influxdb_section', 'influxdb_password'),
                              conf_parser.get('influxdb_section', 'influxdb_user'),
                              conf_parser.get('influxdb_section', 'influxdb_db'))

processes_monitoring(conf_parser, influxdbConn)
