import subprocess
import requests
import json
import configparser
from pathlib import Path
import os
import xml.etree.ElementTree as ET

# Read configuration
config = configparser.ConfigParser()
config.read('config.ini')

# Global settings
include_hdfs_files = config.getboolean('global', 'include_hdfs_files')
discovery_approach = config['global']['discovery_approach']

def run_hdfs_command(command):
    """Run an HDFS command and return the output."""
    result = subprocess.run(command, capture_output=True, text=True)
    return result.stdout.strip()

def parse_hdfs_listing(listing_output):
    """Parse HDFS listing output into a JSON format."""
    listing = []
    for line in listing_output.splitlines():
        parts = line.split()
        if len(parts) >= 8:
            permissions = parts[0]
            replication = parts[1]
            owner = parts[2]
            group = parts[3]
            size = parts[4]
            modification_date_str = parts[5] + ' ' + parts[6]
            modification_date = datetime.strptime(modification_date_str, '%Y-%m-%d %H:%M')
            path = ' '.join(parts[7:])
            entry = {
                "permissions": permissions,
                "replication": replication,
                "owner": owner,
                "group": group,
                "size": size,
                "modification_date": modification_date,
                "path": path
            }
            listing.append(entry)
    return listing


def hdfs_cli_discovery():
    """Discover HDFS information using HDFS CLI."""
    hdfs_info = {}

    cli_config = config['cli']
    hdfs_cmd = cli_config['hdfs_cmd']
    username = cli_config['username']

    # Get NameNodes
    hdfs_info['namenodes'] = run_hdfs_command([hdfs_cmd, 'getconf', '-namenodes']).split()

    # Get Secondary NameNodes
    hdfs_info['secondarynamenodes'] = run_hdfs_command([hdfs_cmd, 'getconf', '-secondarynamenodes']).split()

    # Get Backup Nodes
    hdfs_info['backupnodes'] = run_hdfs_command([hdfs_cmd, 'getconf', '-backupnodes']).split()

    # Get Journal Nodes
    hdfs_info['journalnodes'] = run_hdfs_command([hdfs_cmd, 'getconf', '-journalnodes']).split()

    # Get NN RPC Addresses
    hdfs_info['nnRpcAddresses'] = run_hdfs_command([hdfs_cmd, 'getconf', '-nnRpcAddresses']).split()

    # Get HDFS metadata (e.g., fsck report)
    hdfs_metadata = run_hdfs_command([hdfs_cmd, 'fsck', '/'])
    hdfs_info['fsck'] = interpret_fsck_output(hdfs_metadata)

    # Get HDFS runtime state (e.g., df report)
    hdfs_runtime = run_hdfs_command([hdfs_cmd, 'dfsadmin', '-report'])
    hdfs_info['dfsadmin'] = interpret_dfsadmin_report(hdfs_runtime)

    # Get listing of all files if enabled
    if include_hdfs_files:
        file_listing = run_hdfs_command([hdfs_cmd, 'dfs', '-ls', '-R', '/'])
        hdfs_info['files'] = parse_hdfs_listing(file_listing)
    
    # Get all configurations from hdfs-site.xml and core-site.xml
    hdfs_info['all_configurations'] = get_all_hdfs_configurations()

    return hdfs_info

def webhdfs_discovery():
    """Discover HDFS information using WebHDFS."""
    hdfs_info = {}

    webhdfs_config = config['webhdfs']
    webhdfs_url = webhdfs_config['webhdfs_url']
    username = webhdfs_config['username']

    # Get HDFS configuration
    config_url = f"{webhdfs_url}/conf?user.name={username}"
    config_response = requests.get(config_url)
    hdfs_info['configuration'] = config_response.json() if config_response.status_code == 200 else config_response.text

    # Get HDFS metadata (e.g., directory status)
    metadata_url = f"{webhdfs_url}/v1/?op=GETCONTENTSUMMARY&user.name={username}"
    metadata_response = requests.get(metadata_url)
    hdfs_info['metadata'] = metadata_response.json() if metadata_response.status_code == 200 else metadata_response.text

    # Get HDFS runtime state (e.g., node status)
    runtime_url = f"{webhdfs_url}/v1/?op=GETHOMEDIRECTORY&user.name={username}"
    runtime_response = requests.get(runtime_url)
    hdfs_info['runtime'] = runtime_response.json() if runtime_response.status_code == 200 else runtime_response.text

    # Get listing of all files if enabled
    if include_hdfs_files:
        files_url = f"{webhdfs_url}/v1/?op=LISTSTATUS&user.name={username}"
        files_response = requests.get(files_url)
        hdfs_info['file_listing'] = files_response.json() if files_response.status_code == 200 else files_response.text

    return hdfs_info

def cloudera_manager_discovery():
    """Discover HDFS information using Cloudera Manager API."""
    hdfs_info = {}

    cm_config = config['cloudera_manager']
    cm_host = cm_config['cm_host']
    cm_port = cm_config['cm_port']
    username = cm_config['username']
    password = cm_config['password']

    base_url = f"http://{cm_host}:{cm_port}/api/v40/clusters"
    auth = (username, password)

    # Get HDFS configuration
    config_url = f"{base_url}/cluster/services/hdfs/config"
    config_response = requests.get(config_url, auth=auth)
    hdfs_info['configuration'] = config_response.json() if config_response.status_code == 200 else config_response.text

    # Get HDFS metadata (e.g., fs usage)
    metadata_url = f"{base_url}/cluster/services/hdfs/role-config-groups"
    metadata_response = requests.get(metadata_url, auth=auth)
    hdfs_info['metadata'] = metadata_response.json() if metadata_response.status_code == 200 else metadata_response.text

    # Get HDFS runtime state (e.g., health status)
    runtime_url = f"{base_url}/cluster/services/hdfs/health"
    runtime_response = requests.get(runtime_url, auth=auth)
    hdfs_info['runtime'] = runtime_response.json() if runtime_response.status_code == 200 else runtime_response.text

    # Get listing of all files if enabled
    if include_hdfs_files:
        files_url = f"{base_url}/cluster/services/hdfs/files"
        files_response = requests.get(files_url, auth=auth)
        hdfs_info['file_listing'] = files_response.json() if files_response.status_code == 200 else files_response.text

    return hdfs_info

def interpret_fsck_output(fsck_output):
    """Interpret the fsck output into a dictionary."""
    fsck_dict = {}
    current_section = None

    for line in fsck_output.splitlines():
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()

            if value == "":
                # This line is the beginning of a new section
                current_section = key
                fsck_dict[current_section] = {}
            else:
                # This line is a key-value pair within a section
                if current_section is not None:
                    fsck_dict[current_section][key] = value
                else:
                    fsck_dict[key] = value

    return fsck_dict


def interpret_dfsadmin_report(dfsadmin_output):
    """Interpret the dfsadmin report output into a dictionary."""
    dfsadmin_dict = {}
    current_section = None
    current_object_list = []
    current_object = {}
    objects_remaining = 0
    in_object_section = False
    is_array = False

    for line in dfsadmin_output.splitlines():
        line = line.strip()
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip().split(' (')[0].lower().replace(' ', '_').replace('%', '_pct')
            value = value.strip().split(' (')[0] 

            if value == "":
                # This line is the beginning of a new section or end of previous section
                if current_section:
                    if is_array:
                        dfsadmin_dict[current_section] = current_object_list
                    else:
                        dfsadmin_dict[current_section] = current_object
                    objects_remaining = 0

                current_section = key
                objects_remaining = int(line.split('(')[-1].split(')')[0]) if '(' in line.split(':')[0] else 1
                is_array = '(' in line
                current_object_list = []
                current_object = {}
                in_object_section = objects_remaining > 0
            else:
                # This line is a key-value pair within a section or main section
                if in_object_section and objects_remaining > 0:
                    current_object[key] = value
                else:
                    if current_section and current_section not in dfsadmin_dict:
                        dfsadmin_dict[current_section] = {}
                    if current_section:
                        dfsadmin_dict[current_section][key] = value
                    else:
                        dfsadmin_dict[key] = value
        elif line == "":
            # Empty line indicates the end of an object entry
            if in_object_section and current_section is not None:
                if not is_array:
                    dfsadmin_dict[current_section] = current_object
                    current_section = None
                    objects_remaining -= 1
                else:
                    if current_object:
                        current_object_list.append(current_object)
                        objects_remaining -= 1
                current_object = {}

    # Add the last section if it has objects
    if current_section:
        if is_array:
            if current_object:
                current_object_list.append(current_object)
            dfsadmin_dict[current_section] = current_object_list
        else:
            dfsadmin_dict[current_section] = current_object

    return dfsadmin_dict


def get_all_hdfs_configurations():
    """Retrieve all HDFS configurations from hdfs-site.xml and core-site.xml."""
    config_dict = {}

    # Determine the configuration directory
    hadoop_conf_dir = os.environ.get('HADOOP_CONF_DIR', '/etc/hadoop/conf')
    config_files = [
        os.path.join(hadoop_conf_dir, 'hdfs-site.xml'),
        os.path.join(hadoop_conf_dir, 'core-site.xml')
    ]

    for config_file in config_files:
        if Path(config_file).is_file():
            tree = ET.parse(config_file)
            root = tree.getroot()

            for property in root.findall('property'):
                name = property.find('name').text
                value = property.find('value').text
                config_dict[name] = value

    return config_dict

def main():
    hdfs_info = {}

    if discovery_approach in ['cli', 'all']:
        hdfs_info['cli'] = hdfs_cli_discovery()

    if discovery_approach in ['webhdfs', 'all']:
        hdfs_info['webhdfs'] = webhdfs_discovery()

    if discovery_approach in ['cloudera_manager', 'all']:
        hdfs_info['cloudera_manager'] = cloudera_manager_discovery()

    # Save the information to a JSON file
    with open('hdfs_info.json', 'w') as json_file:
        json.dump(hdfs_info, json_file, indent=4)

if __name__ == "__main__":
    main()
