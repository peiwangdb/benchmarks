# Copyright 2017 Databricks, Inc.
#
# This work (the "Licensed Material") is licensed under the Creative Commons
# Attribution-NonCommercial-NoDerivatives 4.0 International License. You may
# not use this file except in compliance with the License.
#
# To view a copy of this license, visit https://creativecommons.org/licenses/by-nc-nd/4.0/
#
# Unless required by applicable law or agreed to in writing, the Licensed Material is offered
# on an "AS-IS" and "AS-AVAILABLE" BASIS, WITHOUT REPRESENTATIONS OR WARRANTIES OF ANY KIND,
# whether express, implied, statutory, or other. This includes, without limitation, warranties
# of title, merchantability, fitness for a particular purpose, non-infringement, absence of
# latent or other defects, accuracy, or the presence or absence of errors, whether or not known
# or discoverable. To the extent legally permissible, in no event will the Licensor be liable
# to You on any legal theory (including, without limitation, negligence) or otherwise for
# any direct, special, indirect, incidental, consequential, punitive, exemplary, or other
# losses, costs, expenses, or damages arising out of this License or use of the Licensed
# Material, even if the Licensor has been advised of the possibility of such losses, costs,
# expenses, or damages.

import os
import subprocess
import sys
import time
import re
import argparse
import requests
import json
# from sets import Set

# DATABASE SCHEMA HAS TO BE CREATED IN HIVE FIRST.

# ------------------------------ Parameters
catalog = "tpcds"
# Scale factor
scaleFactor = 1
# Hive catalog schema to use (create the database using Spark first!)
databaseName = "sf%s" % (scaleFactor)
# Timeout of a query
timeout = '15m'
# Number of runs.
num_runs = 1
# Location of the folder that contains files with queries we will run
query_dir = "tpcds_2_4_presto"
# Directory to save results.
results_dir = "results/timestamp=%d" % (int(time.time()))
# The following are just for convert_raw_results CSV output:
system = "Hetu 010"
cluster = "Performance"
configuration = "std"
date = time.strftime("%Y-%m-%d")
server="localhost"
port="8080"
cli="presto-cli"
sessions=""
# ------------------------------


# Runs a command on a remote machine and checks the return code if needed
def run_command(cmd, verify_success=False):
    rv = subprocess.call(cmd, shell=True)
    if verify_success:
        assert rv == 0, "Command '%s' failed!" % cmd

def run_command_with_output(cmd, verify_success=False):
    return subprocess.check_output(cmd, shell=True).decode()

def get_query_id(output):
    result = re.search("^\"(.*)\"\\n$", output)
    assert result, "Cannot get query ID!"
    if result:
        return result.group(1)

def convert_time_to_seconds(raw_time):
    if raw_time.find("ms") > -1:
        return float(raw_time[:-2]) / 1000

    if raw_time.find("m") > -1:
        return float(raw_time[:-1]) * 60

    if raw_time.find("h") > -1:
        return float(raw_time[:-1]) * 60 * 60

    return float(raw_time[:-1])

def create_query_result(filename, response):
    query_name = filename[:filename.index(".sql")]

    stats = []
    stats.append(query_name)
    stats.append(response["state"])
    query_stats = response["queryStats"]
    stats.append(str(convert_time_to_seconds(query_stats["elapsedTime"])))
    stats.append(query_stats["queuedTime"])
    stats.append(query_stats["totalPlanningTime"])
    stats.append(str(query_stats["completedDrivers"]))
    stats.append(str(query_stats["physicalInputPositions"]/1000000))
    stats.append(query_stats["physicalInputDataSize"])

    return ",".join(stats)

def write_query_result(result):
    with open("%s/openlookeng_results.csv" % (results_dir), "a") as results:
        results.write("%s\n" % (result))

def run_openlookeng_benchmark(query_dir, results_dir, num_runs):
    # Create helper files.
    run_command("""echo \"""" +
                """select query_id """
                """from system.runtime.queries order by \\"end\\" desc limit 1;" """ +
                """ > __tmp_get_runtime_from_openlookeng.sql""")
    run_command("""echo "set session query_max_run_time = '%s';" > __tmp_openlookeng_configs.sql""" %
                (timeout))

    files = [f for f in os.listdir(query_dir) if f.endswith("sql")]
    files.sort()
    for num_run in range(num_runs):
        for filename in files:
            # Get query plan
            print("Getting plan for " + filename)
            run_command("echo \"explain \" > __tmp_get_query_plan.sql && " +
                        "cat %s/%s >> __tmp_get_query_plan.sql >> __tmp_get_query_plan.sql" %
                        (query_dir, filename))

            explain_cmd = "{} --server {}:{} --catalog {} --schema {} --file __tmp_get_query_plan.sql > {}/plans/{}.plan"
            run_command(explain_cmd.format(cli, server, port, catalog, databaseName, results_dir, filename))
            # Run the query
            print("Running " + filename)
            run_command("""cat __tmp_openlookeng_configs.sql %s/%s > __tmp_current_query.sql""" %
                        (query_dir, filename))
            run_command("%s --server %s:%s --catalog %s --schema %s --file __tmp_current_query.sql"
                        "> %s/runs/%s.run 2>&1" % (cli, server, port, catalog, databaseName, results_dir, filename))
            # Get query runtime
            output = run_command_with_output("%s --server %s:%s --catalog %s --schema %s --file "
                        "__tmp_get_runtime_from_openlookeng.sql" %
                        (cli, server, port, catalog, databaseName))
            query_id = get_query_id(output)

            session = requests.Session()
            session.trust_env = False
            query_url = "http://{}:{}/v1/query/{}".format(server, port, query_id)
            response = json.loads(session.get(query_url).text)
            query_result = create_query_result(filename, response)
            write_query_result(query_result)

    run_command("rm -f __tmp*", True)


def convert_raw_results(raw_results):
    with open("%s/openlookeng_results.csv" % (results_dir), "w") as results:
        results.write(
            'Name,Runtime,System,Cluster,Configuration,Database,Date,Scale,Error\n')
        for line in open("%s/openlookeng_runtimes_raw.csv" % (results_dir)):
            name, time, status = line.split(",")
            # Strip quotes
            name = name[1:-1]
            time = time[1:-1]
            status = status[1:-1]
            if status == "FAILED":
                time = "0"
            elif status == "FINISHED":
                status = ""  # just blank for ok.
            runtime = float(time) / 1000.0
            results.write("%s,%s,%s,%s,%s,%s,%s,%s,%s\n" %
                          (name, runtime, system, cluster, configuration,
                           databaseName, date, scaleFactor, status))    


if __name__ == '__main__':
    # Initialize parser 
    parser = argparse.ArgumentParser() 
    
    # Adding optional argument 
    parser.add_argument("-s", "--server", help = "Server address of Benchmark Hetu cluster, default to localhost") 
    parser.add_argument("-p", "--port", help = "Port of Benchmark Hetu cluster, default to 8080") 
    parser.add_argument("-e", "--cli", help = "Path to the Hetu CLI, default current folder") 
    parser.add_argument("-q", "--queries", help = "Path to the benchmarking queries, default to tpcds_2_4_presto") 
    parser.add_argument("-d", "--schema", help = "Schema for the queries, default to sf1") 
    parser.add_argument("-c", "--catalog", help = "Catalog for the queries, default to tpcds")
    parser.add_argument("-ss", "--session", help = "Sessions to be set for all the queries in the benchmark")
    
    # Read arguments from command line 
    args = parser.parse_args() 
    
    if args.server: 
        server = args.server

    if args.port:
        port = args.port

    if args.cli:
        cli = args.cli

    if args.catalog:
        catalog = args.catalog

    if args.schema:
        databaseName = args.schema

    if args.queries:
        query_dir = args.queries

    run_command("rm -f __tmp*", True)
    run_command("mkdir -p %s/runs" % (results_dir), True)
    run_command("mkdir -p %s/plans" % (results_dir), True)
    with open("%s/openlookeng_results.csv" % (results_dir), "w") as results:
        results.write(
            'query,state,elapsedTime (s),queuedTime,totalPlanningTime,completedDrivers,physicalInputPositions,physicalInputDataSize (M)\n')

    run_openlookeng_benchmark(query_dir, results_dir, num_runs)
    # convert_raw_results("%s/openlookeng_runtimes_raw.csv" % (results_dir))
