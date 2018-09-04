# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 10:10:08 2015
CEPAC Cluster tool library

STD list:
* objectify things
* GUI
* make zipped download work
* fix jobname and folder name thing
* kill jobs

@author: Taige Hou (thou1@partners.org)
@author: Kai Hoeffner (khoeffner@mgh.harvard.edu)
"""
from __future__ import print_function
import os
import sys
import glob
import paramiko
import md5
import re
import zipfile
import threading
import time
from stat import S_ISDIR
import getpass

#A list of clusters
CLUSTER_NAMES = ("MGH", "Orchestra", "Custom")
#Maximum number of concurrent connections
MAX_CONNECTIONS = 8

#Mapping of cluster names to hostname, runfolder path and model folder path
#For run_folder use only relative path from home directory (this is required as lsf and cepac are picky about paths)
#For model_folder can use either absolute path or relative path from home directory
#do not use ~ in path to represent home directory as the ftp client cannot find the directory
CLUSTER_INFO = {"MGH":{'host':'erisone.partners.org',
                       'run_folder':'runs',
                       'model_folder':'/data/cepac/modelVersions',
                       'default_queues':("medium", "long", "vlong", "big")},
                "Orchestra":{'host':'orchestra.med.harvard.edu',
                       'run_folder':'runs',
                       'model_folder':'/groups/freedberg/modelVersions',
                       'default_queues':("freedberg_2h", "freedberg_12h", "freedberg_1d", "freedberg_7d", "freedberg_unlim",
                                         "short", "long")},
                "Custom":{'host':'',
                       'run_folder':'runs',
                       'model_folder':'',
                       'default_queues':()},
                }
#---------------------------------------------
class UploadThread(threading.Thread):
    """Thread used to upload runs and submit jobs"""
    def __init__(self, cluster, dir_local, dir_remote, lsfinfo, update_func, glob_pattern="*.in" ):
        threading.Thread.__init__(self)
        self.cluster = cluster
        self.args = [self, dir_local, dir_remote, lsfinfo, update_func, glob_pattern]
        self.abort = False
    def stop(self):
        self.abort = True
    def run(self):
        while self.cluster.num_connections >= MAX_CONNECTIONS:
            time.sleep(.2)
        self.cluster.num_connections+=1
        jobfiles = self.cluster.sftp_upload(*self.args)
        if not self.abort:
            self.cluster.pybsub(jobfiles)
        self.cluster.num_connections-=1

#---------------------------------------------
class DownloadThread(threading.Thread):
    """Thread used to download runs"""
    def __init__(self, cluster, run_folder,  dir_remote, dir_local, update_func):
        threading.Thread.__init__(self)
        self.cluster = cluster
        self.args = [self, dir_remote, dir_local, update_func]
        self.abort = False
        self.run_folder = run_folder
        #Total number of files to download
        self.total_files = 0
        #current progress of download
        self.curr_files = 0
    def stop(self):
        self.abort = True
    def run(self):
        while self.cluster.num_connections >= MAX_CONNECTIONS:
            time.sleep(.2)
            
        #counts total number of files in folder recursively
        stdin, stdout, stderr =  self.cluster.ssh.exec_command("find {} -type f | wc -l"
                                                       .format(clean_path(self.cluster.run_path+"/"+self.run_folder)))
        #wait for command to finish
        stdout.channel.recv_exit_status()
        self.total_files = int(stdout.read().strip())
        if self.total_files == 0:
            self.total_files = 1
        self.cluster.num_connections+=1
        #self.cluster.sftp_get_compressed(*self.args)
        self.cluster.sftp_get_recursive(*self.args)
        self.cluster.num_connections-=1
        
#---------------------------------------------
class JobInfoThread(threading.Thread):
    """Thread used to get detailed job info"""
    def __init__(self, cluster, jobid, post_func):
        threading.Thread.__init__(self)
        self.cluster = cluster
        self.jobid = jobid
        #function which tells thread how to post results
        self.post_func = post_func
    def run(self):
        while self.cluster.num_connections >= MAX_CONNECTIONS:
            time.sleep(.2)
        self.cluster.num_connections+=1
        job_info = self.cluster.get_job_info(self.jobid)
        self.post_func(jobid = self.jobid, data = job_info)
        self.cluster.num_connections-=1
        
#---------------------------------------------
class CEPACClusterApp:
    """Basic class for the desktop interface with the CEPAC cluster"""
    def __init__(self,):
        self.port = 22

        #SSH Client
        self.ssh = paramiko.SSHClient()

        #Dictionary of available model versions with model type as keys
        self.model_versions = None
        #List of available run queues
        self.queues = None
        #number of currently open connections
        self.num_connections = 0
        #thread for uploading
        self.upload_thread = None
        #threads for downloads
        self.download_threads = []
    def bind_output(self, output=print):
        """
        output is a function used to write messages from the app.
        Defaults to the print function for the console version.
        Any calls to print should use the self.output function instead
        """
        self.output = output
        
        #print initiation message
        self.output("="*40, False)
        self.output("Initiating Cepac Cluster App", False)
    def connect(self, hostname='erisone.partners.org',
                username=None, password=None,
                run_path=None, model_path=None, clustername=None):
        """
        Starts connection to host.
        Should be called once per client.
        """
        #Close any previous connections
        self.close_connection()
        
        #Need to convert to string for paramiko because input could be unicode
        self.hostname = str(hostname)
        self.username = str(username)
        self.password = str(password)
        self.run_path = str(run_path)
        self.model_path = str(model_path)
        self.clustername = str(clustername)

        self.output("\nConnecting to {} as user: {}...".format(self.hostname, self.username), False)

        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.ssh.connect(self.hostname,port=22,username=self.username,password=self.password)
        except paramiko.AuthenticationException:
            #Login failed
            self.output("\tLogin Failed", False)
        else:
            #Get model and queue information
            self.output("\tLogin Succesful", False)
            self.update_cluster_information()
    def create_upload_thread(self, *args, **kwargs):
        self.upload_thread = UploadThread(self, *args, **kwargs)
        self.upload_thread.start()
    def create_download_thread(self, *args, **kwargs):
        thread = DownloadThread(self, *args, **kwargs)
        thread.start()
    def sftp_get_recursive(self, thread, dir_remote, dir_local, progress_func, sftp = None):
        """Recursively Downloads folder including subfolders"""
        if not sftp:
            #Create sftp object. Should only do this once per download.
            with paramiko.Transport((self.hostname, self.port)) as t:
                t.connect(username=self.username, password=self.password)
                t.use_compression()
                #recomended window size from https://github.com/paramiko/paramiko/issues/175
                t.window_size = 134217727
                
                sftp = t.open_session()
                sftp = paramiko.SFTPClient.from_transport(t)
                progress_func(0, thread.run_folder)
                self.output("\nDownloading from folder {} to folder {}...".format(dir_remote, dir_local))
                self.sftp_get_recursive(thread, dir_remote, dir_local, progress_func, sftp)
                self.output("\tDownload Complete")
        else:
            item_list = sftp.listdir(dir_remote)
            dir_local = str(dir_local)
            dir_local = os.path.join(dir_local, os.path.basename(dir_remote))

            if not os.path.isdir(dir_local):
                os.makedirs(dir_local)

            for item in item_list:
                item = str(item)

                if isdir(dir_remote + "/" + item, sftp):
                    self.sftp_get_recursive(thread, dir_remote + "/" + item, dir_local, progress_func, sftp)
                else:
                    sftp.get(dir_remote + "/" + item, os.path.join(dir_local,item))
                    thread.curr_files+=1
                    progress_func(thread.curr_files/float(thread.total_files)*100, thread.run_folder)
#    def sftp_get_compressed(self, dir_remote, dir_local, sftp = None):                    
#        """Download everything as one file"""
#        compfile = "compfile.tar.gz"
#        self.output("\nCompressing {}".format(dir_remote))
#        stdin, stdout, stderr = self.ssh.exec_command("tar -zcf ~/{} ~/{} ".format(compfile, dir_remote))
#        
#        
#        if not stdout.readlines():
#            #Create sftp object
#            with paramiko.Transport((self.hostname, self.port)) as t:
#                t.connect(username=self.username, password=self.password)
#                t.use_compression()
#                #recomended window size from https://github.com/paramiko/paramiko/issues/175
#                t.window_size = 134217727
#                sftp = t.open_session()
#                sftp = paramiko.SFTPClient.from_transport(t)
#                # This should be stored somewhere locally for faster access!
#                dir_temp_local='C:\Temp'
#                
#                sftp.get("~/{}".format(compfile), dir_temp_local+"\CEPACclusterdownload.zip")
#                self.output("\nDownload complete!")
#    
#            stdin, stdout, stderr = ssh.exec_command("rm {}".format(compfile))
#            self.output("\nExtracting files")
#            with zipfile.ZipFile(dir_temp_local+"CEPACclusterdownload.zip", "r") as z:
#                z.extractall(dir_local)
#        else:
#            self.output(stdout.readlines())
            
            


    def sftp_upload(self, thread, dir_local, dir_remote, lsfinfo, progress_func, glob_pattern = "*.in"):
        """Uploads local directory to remote server and generates a job file per subfolder and returns the list of job files."""
        files_copied = 0
        jobfiles = []

        #Create sftp object
        with paramiko.Transport((self.hostname, self.port)) as t:
            t.connect(username=self.username, password=self.password)
            t.use_compression()
            #recomended window size from https://github.com/paramiko/paramiko/issues/175
            t.window_size = 134217727
            sftp = t.open_session()
            sftp = paramiko.SFTPClient.from_transport(t)
            self.output("\nSubmitting runs from folder {} ...".format(dir_local))
            #list of tuples (local_file, remote file) that will be uploaded
            files_to_upload = []
            for dirpath, dirnames, filenames in os.walk(dir_local):
                matching_files = [f for f in glob.glob(dirpath + os.sep + glob_pattern) if not os.path.isdir(f)]

                if not matching_files:
                    continue
                
                # Fix foldername
                remote_base = dir_remote + '/' + os.path.basename(dir_local)
                if not os.path.relpath(dirpath, dir_local)=='.':
                    curr_dir_remote = remote_base + '/' + os.path.relpath(dirpath,dir_local).replace("\\","/")
                else:
                    curr_dir_remote = remote_base
                
                # Create folder and subfolders
                stdin, stdout, stderr = self.ssh.exec_command("mkdir -p '{}'".format(curr_dir_remote))
                #wait for command to finish
                stdout.channel.recv_exit_status()
                self.output("\tCreating {}".format(curr_dir_remote))
                
                # Write and collect job files
                if thread.abort:
                    return None
                self.write_jobfile(curr_dir_remote, lsfinfo, sftp)
                jobfiles.append(curr_dir_remote + '/job.info')                
                
                # Upload files
                for fpath in matching_files:
                    is_up_to_date = False
                    fname = os.path.basename(fpath)

                    local_file = fpath
                    remote_file = curr_dir_remote + '/' + fname
                    # if remote file exists
                    try:
                        sftp.stat(remote_file)
                    except IOError:
                        pass
                    else:
                        local_file_data = open(local_file, "rb").read()
                        remote_file_data = sftp.open(remote_file).read()
                        md1 = md5.new(local_file_data).digest()
                        md2 = md5.new(remote_file_data).digest()
                        if md1 == md2:
                            is_up_to_date = True

                    if not is_up_to_date:
                        files_to_upload.append((local_file, remote_file))

            progress_func(0)
            #upload files
            for local_file, remote_file in files_to_upload:
                if thread.abort:
                    return None
                self.output('\tCopying {} to {}'.format(local_file, remote_file))                     
                sftp.put(local_file, remote_file)
                files_copied += 1
                #update progress bar
                progress_func(files_copied/float(len(files_to_upload))*100)
            self.output('\tFinished Upload')

        return jobfiles
    def write_jobfile(self, curr_dir_remote, lsfinfo, sftp):
        """
        Write job files for the current folder.
        lsfinfo is a dictionary which contains
            queue - the queue to submit to
            email - email address to send upon job completion (optional)
            modeltype - should be either treatm, debug, or transm
            modelversion - name of the model version to run
        """
        self.output('\tWriting Job file: {}'.format(curr_dir_remote + '/job.info'))
        with sftp.open(curr_dir_remote + '/job.info', 'wb') as f:
            jobcommand = "#!/bin/bash\n" +\
                         "#BSUB -J \"" + lsfinfo['jobname'] + "\"\n" +\
            "#BSUB -q " + lsfinfo['queue']   + "\n"
            if 'email' in lsfinfo:
                jobcommand += "#BSUB -u " + lsfinfo['email']   + "\n" + \
                "#BSUB -N\n"
            if lsfinfo['modeltype'] != "smoking":
                jobcommand += self.model_path + "/" + lsfinfo['modeltype'] + "/" + lsfinfo['modelversion'] + " ~/" + clean_path(curr_dir_remote)
            else:
                jobcommand += "/data/cepac/python/bin/python3.6 " + self.model_path + "/" + lsfinfo['modeltype'] + "/"+ \
                                lsfinfo['modelversion'] + "/sim.py" + " ~/" + clean_path(curr_dir_remote) 
            
            
            f.write(jobcommand)
    def pybsub(self, jobfiles):
        """Submit jobs for job list to LSF"""
        for job in jobfiles:      
            stdin, stdout, stderr =  self.ssh.exec_command("bash -lc bsub < '{}'".format(job))
            stdout.read()
            err = stderr.read()
            if err.strip():
                self.output('Error: {}'.format(err))
            self.output('\tSubmitted :{}'.format(job))
    def get_run_folders(self):
        """
        Gets the names of all the folders in the run_folder on the cluster
        and returs as a list
        """
        self.output("\nRetrieving run folders ...", False)
        #use ls -1 {}| awk  '{$1=$2=""; print 0}' to get long form data but not very useful
        stdin, stdout, stderr = self.ssh.exec_command("ls -1 {}".format(self.run_path))
        run_folders = stdout.readlines()

        self.output("\tFound {} run folders".format(len(run_folders)), False)
        return run_folders
    def delete_run_folders(self, folderlist):
        """Deletes the list of folders from the cluster"""
        self.output("\nDeleting Run Folders ...", False)
        for folder in folderlist:
            self.output("\tDeleting {}".format(folder), False)
            stdin, stdout, stderr = self.ssh.exec_command("rm -rf {}".format(self.run_path+"/"+clean_path(folder)))
        self.output("\tFinished Deleting", False)
    def get_job_list(self):
        """
        Gets some basic information about currently running jobs
        Returns jobid, status and queue
        For detailed job info use get_job_info
        """
        self.output("\nGetting job listing ...", False)
        #Get job listing and format the result
        stdin, stdout, stderr = self.ssh.exec_command("bash -lc bjobs | awk '{if (NR!=1) print $1,$3,$4}'")
        #Each entry in Job data will be a list [jobid, status, queue]
        job_data = [line.split() for line in stdout.readlines()]

        return job_data
    def get_job_info(self, jobid):
        """
        Returns detailed job information by running bjobs -l
        Returns a tuple of (jobname, modelname, runfolder)
        """
        stdin, stdout, stderr = self.ssh.exec_command("bash -lc 'bjobs -l {}'".format(jobid))
        #read here to add delay and avoid being blocked by server
        #wait for command to finish
        stdout.channel.recv_exit_status()

        #read job info and get rid of extra spaces
        job_data = re.sub("\n\s*","",stdout.read())
        #get jobname, modelname, runfolder from job info
        re_pattern ="Job Name <(.*?)>.*" +\
                    "Command <.*?{}/.*?/(.*?)".format(self.model_path) +\
                    "~/{}/(.*?)>".format(self.run_path)

        match = re.search(re_pattern, job_data)
        if match:
            job_name, model_version, run_folder = match.groups()
            run_folder = reverse_clean_path(run_folder)
            model_version = model_version.strip()
            return (job_name, model_version, run_folder)
        else:
            return None
    def kill_jobs(self, joblist):
        """Kills jobs with jobids given in joblist"""
        self.output("\nKilling Jobs...", False)
        for jobid in joblist:
            stdin, stdout, stderr = self.ssh.exec_command("bash -lc 'bkill {}'".format(jobid))
            stdout.channel.recv_exit_status()
        self.output("\t {} jobs killed".format(len(joblist)), False)
    def update_cluster_information(self):
        """
        Updates the names of all model versions along with model type(debug, treatm, transm)
        Updates the lists of available queues
        Should be called when logging in
        """

        self.output("\tRetrieving model and queue information...", False)
        stdin, stdout, stderr = self.ssh.exec_command("ls -1 {}".format(self.model_path))
        model_types = [m_type.strip() for m_type in stdout.readlines()]
        model_versions = {}
        for m_type in model_types:
            #For each model type get the associated model versions
            stdin, stdout, stderr = self.ssh.exec_command("ls -1 {}".format(self.model_path+"/"+m_type))
            model_versions[m_type] = [m_version.strip() for m_version in stdout.readlines()]

        self.model_versions = model_versions

        stdin, stdout, stderr = self.ssh.exec_command("ls -1 {}".format(self.model_path))
        model_types = [m_type.strip() for m_type in stdout.readlines()]
        model_versions = {}

        #Gets a list of queues by calling bqueues and filtering the output
        if CLUSTER_INFO[self.clustername]['default_queues']:
            self.queues = CLUSTER_INFO[self.clustername]['default_queues']
        else:
            stdin, stdout, stderr = self.ssh.exec_command("bash -lc bqueues -w | awk '{if (NR!=1) print $1}'")
            self.queues = [q.strip() for q in stdout.readlines()]
        self.output("\tDone", False)

    def close_connection(self):
        self.ssh.close()
    def __del__(self):
        #closes SSH connection upon exit
        self.close_connection()
     
#---------------------------------------------
# Helper function
def isdir(path, sftp):
  try:
    return S_ISDIR(sftp.stat(path).st_mode)
  except IOError:
    #Path does not exist, so by definition not a directory
    return False

#---------------------------------------------
# Helper function
def clean_path(path):
    """Cleans a filepath for use on cluster by adding escape characters"""
    esc_chars = ['&',';','(',')','$','`','\'',' ']
    for c in esc_chars:
        path = path.replace(c, "\\"+c)
    return path

#---------------------------------------------
# Helper function
def reverse_clean_path(path):
    """Removes escape characters from path"""
    return path.replace("\\","")


        
#---------------------------------------------

        
#---------------------------------------------

    
#----------------------------------------------------------------------
if __name__ == "__main__":
    hostname = 'erisone.partners.org'
    username = 'kh398'
    password = getpass.getpass("Password: ")
    port = 22
    glob_pattern='*.*' # can be used to only copy a specific type of file, e.g. '.in'

    lsfinfo = {
    'email'        : "khoeffner@mgh.harvard.edu",
    'modelversion' : "cepac45c",
    'jobname'      : "R6",
    'queue'        : "medium"    
    }

    dir_local = 'Z:\CEPAC - International\Projects\Hoeffner\Ongoing Projects\DTG-1stART-RLS\Analysis\DEV0\Run1_3\R6'
    dir_remote = "runs/" + lsfinfo['jobname']

    if sys.argv[1].lower() == 'upload':
        with paramiko.Transport((hostname, port)) as t:
            t.connect(username=username, password=password)
            sftp = t.open_session()
            sftp = paramiko.SFTPClient.from_transport(t)
            jobfiles = sftp_upload(dir_local, dir_remote, glob_pattern, lsfinfo, sftp)

        if len(sys.argv) > 2 and sys.argv[2].lower() == 'submit':        
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname, username=username, password=password)
                pybsub(jobfiles, ssh)
                
    
    if sys.argv[1].lower() == 'status':
        # Get job status
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname, username=username, password=password)
            stdin, stdout, stderr = ssh.exec_command("bjobs")
            if stdout.readlines():
                for line in stdout.readlines():
                    print(line.split())
            else:
                print(stderr.readlines())  
            
    if sys.argv[1].lower() == 'download':            
    # Download everything - Use this after the runs are done
        with paramiko.Transport((hostname, port)) as t:
            t.connect(username=username, password=password)
            t.use_compression() 
            sftp = t.open_session()
            sftp = paramiko.SFTPClient.from_transport(t)
            sftp_get_recursive(dir_remote, dir_local, sftp)
            print("Download complete!")

    
# Download everything in a zip file - Still needs to be fixed because the path is wrong
#    with paramiko.SSHClient() as ssh:
#        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#        ssh.connect(hostname, username=username, password=password)
#        stdin, stdout, stderr = ssh.exec_command("zip -9 -y -r -q ~/runs/R500.zip "+dir_remote)
#    
#        if not stdout.readlines():
#            with paramiko.Transport((hostname, port)) as t:
#                t.connect(username=username, password=password)
#                t.use_compression() 
#                sftp = t.open_session()
#                sftp = paramiko.SFTPClient.from_transport(t)
#                # This should be stored somewhere locally for faster access!
#                dir_local='C:\MyTemp'
#                sftp.get("runs/R500.zip", dir_local+"\R500.zip")
#                print("Download complete!")
#    
#            stdin, stdout, stderr = ssh.exec_command("rm runs/R500.zip")
#    
#    print("Extracting files")
#    with zipfile.ZipFile(dir_local+"\R500.zip", "r") as z:
#        z.extractall(dir_local)    
    
