import os
import json
from collections import OrderedDict
import paramiko
from stat import S_ISDIR
paramiko.util.log_to_file("paramiko.log")


def get_path_out(path_out):
    os.chdir(r'..')
    root = os.getcwd()
    path_out = os.path.join(root, path_out)
    return path_out


def read_connection(input_file):
    try:
        with open(input_file, 'r') as f:
            return json.load(f, object_pairs_hook=OrderedDict)
    except FileNotFoundError:
        print(f'File not found: {input_file} ')
        if input('Do you want to create one (y/n)?') == 'n':
            quit()


def sftp_walk(sftp, remote_path):

    path = remote_path
    files = []
    folders = []
    for f in sftp.listdir_attr(remote_path):
        if S_ISDIR(f.st_mode):
            folders.append(f.filename)
        else:
            files.append(f.filename)
    if files:
        yield path, files
    for folder in folders:
        new_path = os.path.join(remote_path, folder)
        for x in sftp_walk(new_path):
            yield x


def sftp_get_filenames_by_extension(sftp, remote_path, extension):

    path = remote_path
    files = []
    for f in sftp.listdir_attr(remote_path):
        if not S_ISDIR(f.st_mode):
            if f.filename[-3:] == extension:
                files.append(f.filename)
    if files:
        yield path, files


def ssh_connection_password(path):

    connection_params = read_connection(path)
    transport = paramiko.Transport(connection_params['host'], 22)
    transport.connect(username=connection_params['user'], password=connection_params['password'])
    return transport


def get_stfp(connection_params_path):
    transport = ssh_connection_password(connection_params_path)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp


def download_files(local_dir, remote_path, sftp, signature, number_files):
    for path, files in sftp_get_filenames_by_extension(sftp, remote_path, signature):
        if number_files is None:
            files = files
        else:
            files = files[-1*number_files:]

        for file in files:
            print(f'Atopei o ficheiro {file} no cartafol {path}')
            if file.split('.')[-1] == signature:
                remote_file = path + "/" + file
                if not os.path.exists(local_dir):
                    os.makedirs(local_dir)
                local_file = os.path.join(local_dir, file)
                if os.path.exists(local_file) and number_files is  None:
                    print(f'{file} xa está baixado')
                    pass
                else:
                    print(f'Get from {remote_file} to {local_file}')
                    sftp.get(remote_file, local_file)


def get_radar_files(remote_root_path, root_dir, signature, stations, number_of_last_files=None):
    root_dir = os.path.join(root_dir, 'radarhf_tmp', signature)
    sftp = get_stfp(r'pass/combine.json')
    for station in stations:
        remote_path = remote_root_path + station
        local_dir = os.path.join(root_dir, station)
        download_files(local_dir, remote_path, sftp, signature, number_of_last_files)
    sftp.close()


def get_radial_files(root_dir):

    signature = 'ruv'
    stations = ['LPRO', 'SILL', 'VILA', 'PRIO', 'FIST']
    remote_root_path = r'/Codar/SeaSonde/Data/RadialSites/Site_'
    get_radar_files(remote_root_path, root_dir, signature, stations)


def get_total_files(root_dir):

    signature = 'tuv'
    sites = ['GALI']
    remote_root_path =r'/Codar/SeaSonde/Data/Totals/Totals_'
    get_radar_files(remote_root_path, root_dir, signature, sites)


def get_waves_files(stations, root_dir, number_of_last_files=2):
    signature = 'wls'
    remote_root_path = r'/Codar/SeaSonde/Data/Waves/Site_'
    get_radar_files(remote_root_path, root_dir, signature, stations, number_of_last_files)


def main():
    data_folder = r'../datos'
    get_waves_files(data_folder)
    get_radial_files(data_folder)
    # get_total_files(data_folder)


if __name__ == '__main__':
    main()




