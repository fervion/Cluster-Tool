# -*- coding: utf-8 -*-
"""
Graphical Interface for CEPAC Cluster tool Library
Name: CEPACClusterLibGui.py
Author: Taige Hou (thou1@partners.org)
"""

import wx, threading
from wx.lib.agw import aui
import wx.lib.mixins.listctrl as listmix
import wx.lib.agw.ultimatelistctrl as ULC
from wx.lib.embeddedimage import PyEmbeddedImage
import EnhancedStatusBar
from CEPACClusterLib import CEPACClusterApp, CLUSTER_NAMES, CLUSTER_INFO, JobInfoThread

#----------------------------------------------------------------------
MAIN_WINDOW_SIZE = (850,720)
OUTPUT_FONT = (10.5, wx.FONTFAMILY_SWISS,
               wx.FONTSTYLE_NORMAL,
               wx.FONTWEIGHT_NORMAL)

ICON = PyEmbeddedImage(
    "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAABHNCSVQICAgIfAhkiAAAAPlJ"
    "REFUWIVjYBgFo2CkA0YC8ueoZM8vBgaGFQwMDBNI1fifBtiB3g64IS4ufpCdnR1ZTJ6eDuiC"
    "GSYuLv4QKvYLJsZEdFhQAbx48UIJymRlYGBgo7sDGBkZ//Ly8sK4GnR3AAMDA8OPHz9gzG90"
    "d0BxcbHO79+/Ydw7xOihRiJs/f//P5O/v787ExMTTGwLzAJCBdF/YlyJDzAyMjL8/49izCcG"
    "BgZ+GIfmUYBm+Uxky4nSTwW8Ap+l9EiEzxkYGD6Sq/kNA+UhIEe+2yHgGwWWt1NqOV1ACwNt"
    "ql1iMAMDAwMD1wBZvgg5FALobPlZ3BEyCkbBKKAzAABEjv0W0bs6kAAAAABJRU5ErkJggg==")
#----------------------------------------------------------------------


########################################################################
"""Custom event to signal that output should be written"""
(OutputEvent, EVT_OUTPUT) = wx.lib.newevent.NewEvent()
"""Custom event to update the job info"""
(JobEvent, EVT_JOB) = wx.lib.newevent.NewEvent()
"""Custom event to update the progress gauge for uploads"""
(UpdateUploadEvent, EVT_UPDATE_UPLOAD) = wx.lib.newevent.NewEvent()
"""Custom event to update the progress gauge for downloads"""
(UpdateDownloadEvent, EVT_UPDATE_DOWNLOAD) = wx.lib.newevent.NewEvent()
########################################################################
class PanelNotebook(aui.AuiNotebook):
    """Custom class derived from AuiNotebook that handles clicks on tabs"""
    def __init__(self, *args, **kargs):
        aui.AuiNotebook.__init__(self, *args, **kargs)
    def OnTabClicked(self, event):
        aui.AuiNotebook.OnTabClicked(self, event)
        tab_index = self.GetSelection()
        panel = self.GetPage(tab_index)
        #Let the panel decide what to to when it has focus
        if hasattr(panel, "on_focus"):
            panel.on_focus(None)

########################################################################
class MainFrame(wx.Frame):
    """Main Frame class for layout of app"""
    def __init__(self):
        wx.Frame.__init__(self, None, wx.ID_ANY,
                        "CEPAC Cluster Tool",
                        size=MAIN_WINDOW_SIZE)

        self.SetIcon(ICON.GetIcon())
                                  
        #associate with CEPACClusterApp object
        self.cluster = CEPACClusterApp()


        
        #Set up FrameManager
        self._mgr = aui.AuiManager()
        self._mgr.SetManagedWindow(self)

        #main AuiNotebook containing all the panels
        style = aui.AUI_NB_TOP|aui.AUI_NB_TAB_SPLIT|aui.AUI_NB_TAB_MOVE|\
                aui.AUI_NB_SCROLL_BUTTONS|aui.AUI_NB_CLOSE_ON_ALL_TABS|\
                aui.AUI_NB_DRAW_DND_TAB
        self.notebook = PanelNotebook(self, -1,
                                        agwStyle = style)
        
        #Panels in notebook
        self.login_panel = LoginPanel(self, self.cluster)
        self.upload_panel = UploadPanel(self, self.cluster)
        self.download_panel = DownloadPanel(self, self.cluster)
        self.status_panel = StatusPanel(self, self.cluster)
        
        self.notebook.AddPage(self.login_panel, "login")
        self.notebook.AddPage(self.upload_panel, "upload")
        self.notebook.AddPage(self.download_panel, "download")
        self.notebook.AddPage(self.status_panel, "status")
        
        #Hide close buttons
        for page_num in range(self.notebook.GetPageCount()):
            self.notebook.SetCloseButton(page_num,False)

        #Text box used to print messages from Cluster App
        self.output_box = wx.TextCtrl(self, -1,
                                      style=wx.TE_MULTILINE|wx.TE_READONLY|wx.TE_DONTWRAP)
        self.output_box.SetFont(wx.Font(*OUTPUT_FONT))
        
        #Bind output box to Cluster App.
        #If called from worker thread, we post an event to the output box
        #Otherwise write directly to output box
        def gen_evt_func(text, is_thread = True):
            if is_thread:
                evt = OutputEvent(message = text)
                wx.PostEvent(self, evt)
            else:
                self.on_output(None, text)
        #print_func = lambda text:self.output_box.AppendText(text+"\n")
        self.cluster.bind_output(gen_evt_func)
        
        self._mgr.AddPane(self.notebook, aui.AuiPaneInfo().Name("notebook_content").CenterPane().CloseButton(False))
        self._mgr.AddPane(self.output_box, aui.AuiPaneInfo().Name("output").
                          Bottom().CloseButton(False).CaptionVisible(False).
                          BestSize(-1,230))


        #status bar
        self.setup_statusbar()
        
        #commit changes
        self._mgr.Update()

        self.Bind(EVT_OUTPUT, self.on_output)
    def setup_statusbar(self):
        self.statusbar = EnhancedStatusBar.EnhancedStatusBar(self)
        self.statusbar.GetParent().SendSizeEvent()
        self.statusbar.SetFieldsCount(3)
        self.statusbar.SetStatusWidths([55,150,40])
        self.upload_gauge = wx.Gauge(self.statusbar, -1, size = (150,-1))
        #Add progress gauge to upload panel
        self.upload_panel.add_gauge(self.upload_gauge)
        self.statusbar.SetFont(wx.Font(9,wx.FONTFAMILY_DEFAULT, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL))
        self.abort_upload_btn = wx.Button(self.statusbar, -1, "Abort", size=(50,-1))
        self.statusbar.AddWidget(wx.StaticText(self.statusbar, -1, "Upload"))
        self.statusbar.AddWidget(self.upload_gauge)
        self.statusbar.AddWidget(self.abort_upload_btn)
        self.SetStatusBar(self.statusbar)

        self.Bind(wx.EVT_BUTTON, self.on_abort_upload, self.abort_upload_btn)
    def on_output(self, event, text = ""):
        """Called to print text to the output box"""
        if event:
            self.output_box.AppendText(event.message+"\n")
        else:
            self.output_box.AppendText(text+"\n")
    def on_abort_upload(self, event):
        if self.cluster.upload_thread:
            self.cluster.upload_thread.stop()
            self.output_box.AppendText("\tUpload Stopped\n")
########################################################################
class LoginPanel(wx.Panel):
    """Panel that handles login to the cluster"""
    def __init__(self, parent, cluster):
        wx.Panel.__init__(self, parent)
        self.cluster = cluster
        self.parent = parent
        #List of clusters to choose from.  Custom allows user to manually input hostname
        self.cluster_cb = wx.ComboBox(self, -1, value=CLUSTER_NAMES[0],
                                          choices=list(CLUSTER_NAMES),
                                          style=wx.CB_READONLY)
        self.hostname_tc = wx.TextCtrl(self, -1, CLUSTER_INFO[CLUSTER_NAMES[0]]['host'], size=(200,-1))
        self.runfolder_tc = wx.TextCtrl(self, -1, CLUSTER_INFO[CLUSTER_NAMES[0]]['run_folder'], size=(170,-1))
        self.modelfolder_tc = wx.TextCtrl(self, -1, CLUSTER_INFO[CLUSTER_NAMES[0]]['model_folder'], size=(200,-1))
                                        
        self.username_tc = wx.TextCtrl(self, -1,)
        self.password_tc = wx.TextCtrl(self, -1, style=wx.TE_PASSWORD)
        login_btn = wx.Button(self, 10, "Login")
        
        #Sets the default cluster information on init
        self.on_change_host(None)

        #Layout
        gbs = wx.GridBagSizer(10,20)
        gbs.Add(wx.StaticText(self, -1, "hostname"),(0,2))
        gbs.Add(wx.StaticText(self, -1, "run folder"),(0,3))
        gbs.Add(wx.StaticText(self, -1, "model folder"),(0,4))
        gbs.Add(self.cluster_cb, (1,1))
        gbs.Add(self.hostname_tc, (1,2))
        gbs.Add(self.runfolder_tc, (1,3))
        gbs.Add(self.modelfolder_tc, (1,4))
        gbs.Add(wx.StaticText(self, -1, "Username:"), (2,0))
        gbs.Add(self.username_tc, (2,1))
        gbs.Add(wx.StaticText(self, -1, "Password:"), (3,0))
        gbs.Add(self.password_tc, (3,1))
        gbs.Add(login_btn, (4,2))

        self.Bind(wx.EVT_COMBOBOX, self.on_change_host, self.cluster_cb)
        self.Bind(wx.EVT_BUTTON, self.on_login,login_btn)
        self.password_tc.Bind(wx.EVT_KEY_UP, self.on_keypress)
        self.SetSizer(gbs)

    def on_change_host(self, event):
        """Changes the displayed cluster information depending on which host is selected"""
        cluster_name = self.cluster_cb.GetValue()
        self.hostname_tc.SetValue(CLUSTER_INFO[cluster_name]['host'])
        self.runfolder_tc.SetValue(CLUSTER_INFO[cluster_name]['run_folder'])
        self.modelfolder_tc.SetValue(CLUSTER_INFO[cluster_name]['model_folder'])
    def on_login(self, event):
        """
        Calls the ClusterApp connect function
        """

        hostname = self.hostname_tc.GetValue()
        username = self.username_tc.GetValue()
        password = self.password_tc.GetValue()
        run_path = self.runfolder_tc.GetValue()
        model_path = self.modelfolder_tc.GetValue()
        clustername = self.cluster_cb.GetValue()
        self.cluster.connect(hostname, username, password, run_path, model_path, clustername)

        #refill fields on other tabs with new cluster information
        self.parent.upload_panel.refill_fields()
    def on_keypress(self, event):
        """Binds enter key to login button"""
        keycode = event.GetKeyCode()
        if keycode == wx.WXK_RETURN:
            self.on_login(None)
        event.Skip()
            
########################################################################        
class UploadPanel(wx.Panel):
    """Panel that handles creation of jobs and uploading folders to the cluster"""
    def __init__(self, parent, cluster):
        wx.Panel.__init__(self, parent)
        self.cluster = cluster
        
        self.model_type_cb = wx.ComboBox(self, -1,  style=wx.CB_READONLY)
        self.model_version_cb = wx.ComboBox(self, -1, size=(300,-1),style=wx.CB_READONLY)
        self.queue_cb = wx.ComboBox(self, -1, style=wx.CB_READONLY)
        self.email_tc = wx.TextCtrl(self, -1, size=(200,-1))
        self.jobname_tc = wx.TextCtrl(self, -1, size=(170,-1))
        self.local_dir_tc = wx.TextCtrl(self, -1, size=(600,-1))
        browse_btn = wx.Button(self, 20, "...")                         
        upload_btn = wx.Button(self, 10, "Submit")

        #Layout
        gbs = wx.GridBagSizer(10,20)
        gbs.Add(wx.StaticText(self, -1, "Model Type"),(0,0))
        gbs.Add(wx.StaticText(self, -1, "Model Version"),(1,0))
        gbs.Add(wx.StaticText(self, -1, "Queue"),(2,0))
        gbs.Add(wx.StaticText(self, -1, "Email"),(3,0))
        gbs.Add(wx.StaticText(self, -1, "Job Name"),(4,0))
        gbs.Add(wx.StaticText(self, -1, "Input Directory"),(5,0))

        gbs.Add(self.model_type_cb, (0,1))
        gbs.Add(self.model_version_cb, (1,1))
        gbs.Add(self.queue_cb, (2,1))
        gbs.Add(self.email_tc, (3,1))
        gbs.Add(self.jobname_tc, (4,1))
        gbs.Add(self.local_dir_tc, (5,1))
        gbs.Add(browse_btn, (5,2))

        gbs.Add(upload_btn, (6,0))

        self.Bind(wx.EVT_COMBOBOX, self.on_select_model_type, self.model_type_cb)
        self.Bind(wx.EVT_BUTTON, self.on_browse, browse_btn)
        self.Bind(wx.EVT_BUTTON, self.on_upload, upload_btn)
        self.Bind(EVT_UPDATE_UPLOAD, self.on_update_progress)
        self.SetSizer(gbs)
    def add_gauge(self, progress_gauge):
        """associates a gauge widget with this panel"""
        self.progress_gauge = progress_gauge
    def refill_fields(self):
        #Fill the model types combo box
        if self.cluster.model_versions:
            model_types = self.cluster.model_versions.keys()
            self.model_type_cb.Set(model_types)

            default_value = model_types[0]
            if "treatm" in model_types:
                default_value = "treatm"

            self.model_type_cb.SetStringSelection(default_value)

            #Fill model versions
            self.on_select_model_type(None)
        #Fill the queues combo box
        if self.cluster.queues:
            self.queue_cb.Set(self.cluster.queues)
            self.queue_cb.SetStringSelection(self.cluster.queues[0])
    def on_select_model_type(self, event):
        """
        Called when a model type is selected
        Fills in model versions for that model type
        """
        model_type = self.model_type_cb.GetValue()
        versions = self.cluster.model_versions[model_type]
        self.model_version_cb.Set(versions)

        if versions:
            self.model_version_cb.SetStringSelection(versions[-1])
    def on_upload(self, event):
        """Calls cluster app to upload folders and submit jobs"""
        dir_local = self.local_dir_tc.GetValue()
        dir_remote = self.cluster.run_path

        lsfinfo = {'jobname': self.jobname_tc.GetValue(),
                   'queue': self.queue_cb.GetValue(),
                   'modeltype': self.model_type_cb.GetValue(),
                   'modelversion': self.model_version_cb.GetValue()}
        if self.email_tc.GetValue():
            lsfinfo['email'] = self.email_tc.GetValue()

        def update_func(progress):
            evt = UpdateUploadEvent(progress = progress)
            wx.PostEvent(self, evt)

        pattern = "*.in"
        if lsfinfo['modeltype']=="smoking":
            pattern="*.xlsx"
        self.cluster.create_upload_thread(dir_local, dir_remote,
                                          lsfinfo, update_func,pattern)
        #jobfiles = self.cluster.sftp_upload(dir_local, dir_remote, lsfinfo)
        
        #submit jobs
        #self.cluster.pybsub(jobfiles)
    def on_browse(self, event):
        """Handles browsing for local dir"""
        dlg = wx.DirDialog(self, "Choose a directory:")
        if dlg.ShowModal() == wx.ID_OK:
            self.local_dir_tc.SetValue(dlg.GetPath())
        dlg.Destroy()
    def on_update_progress(self, event):
        """Updates progress bar for uploads"""
        self.progress_gauge.SetValue(event.progress)
        
        
########################################################################        
class DownloadPanel(wx.Panel):
    """Panel that handles downloading of folders from the cluster"""
    def __init__(self, parent, cluster):
        wx.Panel.__init__(self, parent)
        self.cluster = cluster
        
        #This is required to fix a bug with GenericDirCtrl in this version of wx
        self.local = wx.Locale(wx.LANGUAGE_ENGLISH)

        #dict of progress gauges mapping folder names to gauges
        self.gauges = {}
        
        #List Control of base run folder on cluster
        self.remote_browser = ULC.UltimateListCtrl(self, -1, size = (-1,300),
                                                   agwStyle=wx.LC_REPORT|wx.LC_VRULES|wx.LC_HRULES
                                                   |wx.LC_SINGLE_SEL|ULC.ULC_AUTO_CHECK_CHILD) 
        self.refresh_remote_btn = wx.Button(self, 10, "Refresh")
        self.download_btn = wx.Button(self, 20, "Download")
        self.delete_btn = wx.Button(self, 30, "Delete")
        
        #Layout
        flex = wx.FlexGridSizer(cols = 1)
        flex.Add(self.remote_browser, 0, wx.EXPAND)
        flex.Add(self.refresh_remote_btn, 0)
        flex.Add(self.download_btn,0)
        flex.Add(self.delete_btn,0)
        flex.AddGrowableCol(0)

        self.Bind(wx.EVT_BUTTON, self.on_refresh, self.refresh_remote_btn)
        self.Bind(wx.EVT_BUTTON, self.on_download, self.download_btn)
        self.Bind(wx.EVT_BUTTON, self.on_delete, self.delete_btn)
        self.Bind(EVT_UPDATE_DOWNLOAD, self.on_update_download)
        self.SetSizer(flex)
    def on_refresh(self, event):
        """Refresh the list of Run folders on the cluster"""
        self.remote_browser.ClearAll()

        #Add Column Headers
        info = ULC.UltimateListItem()
        info._mask = ULC.ULC_MASK_CHECK
        info._kind = 1
        info._footerFont = None
        self.remote_browser.InsertColumnInfo(0, info)
        
        info = ULC.UltimateListItem()
        info._format = wx.LIST_FORMAT_RIGHT
        info._mask = wx.LIST_MASK_TEXT
        info._text = "Run Folder"
        self.remote_browser.InsertColumnInfo(1, info)
        
        info = ULC.UltimateListItem()
        info._format = wx.LIST_FORMAT_RIGHT
        info._mask = wx.LIST_MASK_TEXT
        info._text = "Progress"
        self.remote_browser.InsertColumnInfo(2, info)
        
        #Add data
        for index,entry in enumerate(self.cluster.get_run_folders()):
            run_folder = entry.strip()
            #checkbox
            self.remote_browser.InsertStringItem(index, "", it_kind=1)
            #Directory name
            self.remote_browser.SetStringItem(index, 1, run_folder)
            self.remote_browser.SetStringItem(index, 2, "")
            
            self.gauges[run_folder] = wx.Gauge(self.remote_browser, -1, 100, style=wx.GA_HORIZONTAL|wx.GA_SMOOTH)
            item = self.remote_browser.GetItem(index,2)
            item.SetWindow(self.gauges[run_folder])
            self.remote_browser.SetItem(item)
            
        #item = self.remote_browser.GetItem(1,1)
        #item.SetWindow(self.gauge)
        #self.remote_browser.SetItem(item)
        self.remote_browser.SetColumnWidth(0,wx.LIST_AUTOSIZE)
        self.remote_browser.SetColumnWidth(1,wx.LIST_AUTOSIZE)
        self.remote_browser.SetColumnWidth(2,wx.LIST_AUTOSIZE)
    def on_update_download(self, event):
        """Handles updates to progress bars for downloads"""
        run_folder = event.run_folder
        if run_folder in self.gauges:
            self.gauges[run_folder].SetValue(event.progress)
    def on_download(self, event):
        """Recursively Downloads the directories selected by user"""
        #Get paths of checked items
        items_to_download = []
        for row_index in range(self.remote_browser.GetItemCount()):
            if self.remote_browser.GetItem(row_index, 0).IsChecked():
                remote_path = self.remote_browser.GetItem(row_index,1).GetText()
                items_to_download.append(remote_path)
    
        #Create Dir Dialog to pick local dir
        dir_local = None
        if items_to_download:
            dlg = wx.DirDialog(self, "Choose a folder:")
            if dlg.ShowModal() == wx.ID_OK:
                dir_local = dlg.GetPath()
            dlg.Destroy()
            
        def update_func(progress, run_folder):
            evt = UpdateDownloadEvent(progress = progress, run_folder = run_folder)
            wx.PostEvent(self, evt)
            
        #Download selected folders
        if dir_local:
            for run_folder in items_to_download:
                dir_remote = self.cluster.run_path+"/"+run_folder
                self.cluster.create_download_thread(run_folder, dir_remote, dir_local, update_func)
    def on_delete(self, event):
        """Deletes the directories selected by user"""
        #Get paths of checked items
        items_to_delete = []
        indices_to_delete = []
        for row_index in range(self.remote_browser.GetItemCount()):
            if self.remote_browser.GetItem(row_index, 0).IsChecked():
                remote_path = self.remote_browser.GetItem(row_index,1).GetText()
                items_to_delete.append(remote_path)
                indices_to_delete.append(row_index)
    
        #Confirm Delete
        if items_to_delete:
            dlg = wx.MessageDialog(self, "Deleting:\n"+"\n".join(items_to_delete),
                                   "Deleting Folders",
                                   wx.OK | wx.CANCEL)
            if dlg.ShowModal() == wx.ID_OK:
                self.cluster.delete_run_folders(items_to_delete)
                #reverse sort the indices so we dont run into trouble while deleting from for loop
                indices_to_delete.reverse()
                for index in indices_to_delete:
                    self.remote_browser.DeleteItem(index)
            dlg.Destroy()

########################################################################        
class StatusPanel(wx.Panel):
    """Panel that displays status of currently running jobs on the cluster"""
    def __init__(self, parent, cluster):
        wx.Panel.__init__(self, parent)
        self.cluster = cluster
        
        #List Control of base run folder on cluster
        self.job_browser = ULC.UltimateListCtrl(self, -1, size = (-1,300),
                                                   agwStyle=wx.LC_REPORT|wx.LC_VRULES|wx.LC_HRULES
                                                   |wx.LC_SINGLE_SEL|ULC.ULC_AUTO_CHECK_CHILD)
        self.refresh_btn = wx.Button(self, 10, "Refresh")
        self.kill_btn = wx.Button(self, 30, "Kill")
        #Layout
        flex = wx.FlexGridSizer(cols = 1)
        flex.Add(self.job_browser, 0, wx.EXPAND)
        flex.Add(self.refresh_btn, 0)
        flex.Add(self.kill_btn, 0)
        flex.AddGrowableCol(0)

        self.Bind(wx.EVT_BUTTON, self.on_refresh, self.refresh_btn)
        self.Bind(wx.EVT_BUTTON, self.on_kill, self.kill_btn)
        self.Bind(EVT_JOB, self.on_job)
        
        self.SetSizer(flex)
    def on_refresh(self, event):
        """Refresh the list of jobs"""
        self.job_browser.ClearAll()

        #Add Column Headers
        info = ULC.UltimateListItem()
        info._mask = ULC.ULC_MASK_CHECK
        info._kind = 1
        info._footerFont = None
        self.job_browser.InsertColumnInfo(0, info)
        
        info = ULC.UltimateListItem()
        info._format = wx.LIST_FORMAT_RIGHT
        info._mask = wx.LIST_MASK_TEXT
        info._text = "ID"
        self.job_browser.InsertColumnInfo(1, info)

        info = ULC.UltimateListItem()
        info._format = wx.LIST_FORMAT_RIGHT
        info._mask = wx.LIST_MASK_TEXT
        info._text = "Status"
        self.job_browser.InsertColumnInfo(2, info)

        info = ULC.UltimateListItem()
        info._format = wx.LIST_FORMAT_RIGHT
        info._mask = wx.LIST_MASK_TEXT
        info._text = "Queue"
        self.job_browser.InsertColumnInfo(3, info)

        info = ULC.UltimateListItem()
        info._format = wx.LIST_FORMAT_RIGHT
        info._mask = wx.LIST_MASK_TEXT
        info._text = "Job Name"
        self.job_browser.InsertColumnInfo(4, info)

        info = ULC.UltimateListItem()
        info._format = wx.LIST_FORMAT_RIGHT
        info._mask = wx.LIST_MASK_TEXT
        info._text = "Model"
        self.job_browser.InsertColumnInfo(5, info)
        jobids = []
        
        info = ULC.UltimateListItem()
        info._format = wx.LIST_FORMAT_RIGHT
        info._mask = wx.LIST_MASK_TEXT
        info._text = "Folder"
        self.job_browser.InsertColumnInfo(6, info)
        
        #Add basic data
        for index,job_data in enumerate(self.cluster.get_job_list()):
            jobid, status, queue = job_data
            jobids.append(jobid)
            #checkbox
            self.job_browser.InsertStringItem(index, "", it_kind=1)
            self.job_browser.SetStringItem(index, 1, jobid)
            self.job_browser.SetStringItem(index, 2, status)
            self.job_browser.SetStringItem(index, 3, queue)

        #Function to be passed to the job thread
        def job_evt_func(jobid, data):
            evt = JobEvent(jobid=jobid, data = data)
            wx.PostEvent(self, evt)
            
        #Add detailed data
        for index, jobid in enumerate(jobids):
            #create Job thread
            job_thread = JobInfoThread(self.cluster, jobid, job_evt_func)
            job_thread.start()
            import time
            time.sleep(.01)

    def on_job(self, event):
        """Updates display with detailed job info"""
        jobid = event.jobid
        job_info = event.data
        if job_info:
            for index in range(self.job_browser.GetItemCount()):
                if str(self.job_browser.GetItem(index, 1).GetText()) == str(jobid):
                    job_name, model_version, run_folder = job_info
                    self.job_browser.SetStringItem(index, 4, job_name)
                    self.job_browser.SetStringItem(index, 5, model_version)
                    self.job_browser.SetStringItem(index, 6, run_folder)
        for i in range(self.job_browser.GetColumnCount()):
            self.job_browser.SetColumnWidth(i,wx.LIST_AUTOSIZE)
    def on_kill(self, event):
        """Deletes the directories selected by user"""
        #Get paths of checked items
        jobs = []
        job_indices = []
        for row_index in range(self.job_browser.GetItemCount()):
            if self.job_browser.GetItem(row_index, 0).IsChecked():
                jobid = self.job_browser.GetItem(row_index,1).GetText()
                jobs.append(jobid)
                job_indices.append(row_index)
    
        #Confirm Delete
        if jobs:
            dlg = wx.MessageDialog(self, "Kill Selected Jobs?",
                                   "Kill Jobs",
                                   wx.OK | wx.CANCEL)
            if dlg.ShowModal() == wx.ID_OK:
                self.cluster.kill_jobs(jobs)
                #reverse sort the indices so we dont run into trouble while deleting from for loop
                job_indices.reverse()
                for index in job_indices:
                    self.job_browser.DeleteItem(index)
            dlg.Destroy()

if __name__ == "__main__":
    #Run the program
    app = wx.App()
    frame = MainFrame()
    frame.Show()
    app.MainLoop()
    
