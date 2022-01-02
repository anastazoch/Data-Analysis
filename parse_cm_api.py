#!/usr/bin/python3

import sys, argparse, logging, requests, json, traceback
from requests.exceptions import HTTPError


url = None
file = None
cluster = None
service = None
role = None
rolegroup = None
parcel = None
parse_whole_text = False
print_only_cluster_names = False
print_only_service_names = False
print_only_role_names = False
print_only_rolegroup_names = False
print_only_parcel_names = False


def set_logging():

  logger = logging.getLogger()
  logger.setLevel(logging.DEBUG)
  ch = logging.StreamHandler()
  ch.setLevel(logging.DEBUG)
  formatter = logging.Formatter('%(levelname)s - %(message)s')
  ch.setFormatter(formatter)
  logger.addHandler(ch)

  return logger


def parse_cmd_args():

  parser = argparse.ArgumentParser(description='Cloudera Manager API parser')
  file_url_mutex_group = parser.add_mutually_exclusive_group(required=True)
  file_url_mutex_group.add_argument('-u', '--url', metavar='<URL>', help='URL of the Cloudera Manager API to call')
  file_url_mutex_group.add_argument('-f', '--file', metavar='<file>', help='JSON file to parse')
  cluster_whole_document_mutex_group = parser.add_mutually_exclusive_group(required=False)
  cluster_whole_document_mutex_group.add_argument('-c', '--cluster', metavar='<cluster>', help='cluster to parse')
  cluster_whole_document_mutex_group.add_argument('-a', '--all', dest='parse_whole_text', help="parse whole deployment", action='store_true')
  service_parcel_mutex_group = parser.add_mutually_exclusive_group(required=False)
  service_parcel_mutex_group.add_argument("-s", "--service", metavar='<service>', help="cluster service to parse")
  service_parcel_mutex_group.add_argument("-p", "--parcel", metavar='<parcel>', help="parcel to parse")
  role_rolegroup_mutex_group = parser.add_mutually_exclusive_group(required=False)
  role_rolegroup_mutex_group.add_argument('-r', "--role", metavar='<role>', help="service role to parse", required=False)
  role_rolegroup_mutex_group.add_argument('-rg', "--rolegroup", metavar='<rolegroup>', help="service rolegroup to parse", required=False)
  only_names_mutex_group = parser.add_mutually_exclusive_group(required=False)
  only_names_mutex_group.add_argument('--clusters', dest='print_only_cluster_names', help='print only the names of the clusters', action='store_true')
  only_names_mutex_group.add_argument('--services', dest='print_only_service_names', help='print only the names of the services', action='store_true')
  only_names_mutex_group.add_argument('--roles', dest='print_only_role_names', help='print only the names of the roles', action='store_true')
  only_names_mutex_group.add_argument('--rolegroups', dest='print_only_rolegroup_names', help='print only the names of the role groups', action='store_true')
  only_names_mutex_group.add_argument('--parcels', dest='print_only_parcel_names', help='print only the names of the parcels', action='store_true')

  return parser


def set_vars(cmd_args_dict):
  global file, url, cluster, service, role, parcel, parse_whole_text, print_only_cluster_names, print_only_service_names, print_only_role_names, print_only_rolegroup_names, print_only_parcel_names

  file = cmd_args_dict['file']
  url = cmd_args_dict['url']
  cluster = cmd_args_dict['cluster']
  service = cmd_args_dict['service']
  role = cmd_args_dict['role']
  rolegroup = cmd_args_dict['rolegroup']
  parcel = cmd_args_dict['parcel']
  parse_whole_text = cmd_args_dict['parse_whole_text']
  print_only_cluster_names = cmd_args_dict['print_only_cluster_names']
  print_only_service_names = cmd_args_dict['print_only_service_names']
  print_only_role_names = cmd_args_dict['print_only_role_names']
  print_only_rolegroup_names = cmd_args_dict['print_only_rolegroup_names']
  print_only_parcel_names = cmd_args_dict['print_only_parcel_names']


def check_vars(logger):

  if cluster and print_only_cluster_names:
    logger.error('You cannot set both cluster name and print only cluster names')
    return False

  if service and print_only_service_names:
    logger.error('You cannot set both service name and print only service names')
    return False

  if role and print_only_role_names:
    logger.error('You cannot set both role name and print only role names')
    return False

  if rolegroup and print_only_rolegroup_names:
    logger.error('You cannot set both cluster name and print only cluster names')
    return False

  return True
      

class CMAPIParser:

  def __init__(self, file, url, cluster, service, role, rolegroup, parcel, parse_whole_text, print_only_cluster_names, print_only_service_names, print_only_role_names, print_only_rolegroup_names, print_only_parcel_names):

    self.file = file
    self.url = url
    self.cluster = cluster
    self.service = service
    self.role = role
    self.rolegroup = rolegroup
    self.parcel = parcel
    self.parse_whole_text = parse_whole_text
    self.print_only_cluster_names = print_only_cluster_names
    self.print_only_service_names = print_only_service_names
    self.print_only_role_names = print_only_role_names
    self.print_only_rolegroup_names = print_only_rolegroup_names
    self.print_only_parcel_names = print_only_parcel_names

  def open_file(self, file):

    return open(file, 'r')

  def call_url(self, url):

    print('Enter Cloudera Manager credentials')
    username = input('Username: ')
    password = input('Password: ')

    try:
      response = requests.get(url, auth=(username, password))
      response.raise_for_status()
      return response.json()

    except HTTPError as http_err:
      logger.error('HTTP error occurred: {}'.format(http_err))
      sys.exit(1)
    except Exception as err:
      logger.error('Other error occurred: {}'.format(err))
      sys.exit(1)

  def parse_json(self, file, logger):
    data = json.load(file)

    for key, value in data.items():
      if self.print_only_cluster_names:
        logger.info('Printing cluster names...')
        cluster_names_list = []
        
        for cluster in data['clusters']:
          cluster_names_list.append(cluster['name'])

        return cluster_names_list
      else:
        for cluster in data['clusters']:
          if cluster['name'] == self.cluster:
            if self.service:
              for service in cluster['services']:
                if service['type'] == self.service:
                  if self.role:
                    logger.info('Printing service "{}" role "{}" configuration...'.format(self.service, self.role))
                    role_list = []
                  
                    for role in service['roles']:
                      if role['type'] == self.role:
                        role_list.append(role)

                    return role_list
                  elif self.print_only_role_names:
                    logger.info('Printing service "{}" role names...'.format(self.service))
                    role_names_list = []

                    for role in service['roles']:
                      role_names_list.append(role['name'])

                    return role_names_list
                  elif self.rolegroup:
                    logger.info('Printing service "{}" role group "{}" configuration...'.format(self.service, self.rolegroup))
                    rolegroup_list = []

                    for rolegroup in service['roleConfigGroups']:
                      if rolegroup['name'] == self.rolegroup:
                        rolegroup_list.append(rolegroup)
                    
                    return rolegroup_list
                  elif self.print_only_rolegroup_names:
                    logger.info('Printing service "{}" role group names...'.format(self.service))

                    rolegroup_names_list = []

                    for rolegroup in service['roleConfigGroups']:
                      rolegroup_names_list.append(rolegroup['name'])

                    return rolegroup_names_list                  
                  else:
                    logger.info('Printing cluster "{}" service "{}" configuration'.format(self.cluster, self.service))
                    return service
            elif self.print_only_service_names:
              logger.info('Printing cluster "{}" service names...'.format(self.cluster))
              service_names_list = []

              for service in cluster['services']:
                service_names_list.append(service['type'])

              return service_names_list
            elif self.parcel:
              logger.info('Printing cluster "{}" parcel "{}"...'.format(self.cluster, self.parcel))
              for parcel in cluster['parcels']:
                if parcel['product'] == self.parcel:
                  return parcel
            elif self.print_only_parcel_names:
              logger.info('Printing cluster "{}" parcels...'.format(self.cluster))
              parcel_names_list = []

              for parcel in cluster['parcels']:
                parcel_names_list.append(parcel['product'])

              return parcel_names_list              
            else:
              logger.info('Printing cluster "{}" configuration...'.format(self.cluster))
              return cluster

    file.close()

  def print_info(self, logger):

    if file:
      json_text = self.open_file(self.file)
    elif url:
      json_text = self.call_url(self.url)
    
    if self.parse_whole_text:
      logger.info('Printing whole configuration...')
      print(json.dumps(json.load(json_text), indent=2, sort_keys=True))
      json_file.close()
    else:
      print(json.dumps(self.parse_json(json_text, logger), indent=2, sort_keys=True))
    

def main():
  try:
    logger = set_logging()
    logger.info('Parsing command-line arguments...')
    cmd_arg_parser = parse_cmd_args()
    cmd_args = cmd_arg_parser.parse_args()
    logger.info('Setting variables...')
    set_vars(cmd_args.__dict__)
    logger.info('Checking command-line arguments')
    if check_vars(logger):
      cm_api_parser = CMAPIParser(file, url, cluster, service, role, rolegroup, parcel, parse_whole_text, print_only_cluster_names, print_only_service_names, print_only_role_names, print_only_rolegroup_names, print_only_parcel_names)
      cm_api_parser.print_info(logger)
      return 0
    else:
      cmd_arg_parser.print_help()
      return 1
  except Exception:
    logger.error(traceback.format_exc())


if __name__ == '__main__':
  sys.exit(main())