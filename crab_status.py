import glob,json,os,sys,pprint
pp = pprint.PrettyPrinter(indent=4)
import CRABClient
from CRABAPI.RawCommand import crabCommand

class TaskTracker(object):
    """docstring for TaskTracker"""
    def __init__(self, crab_dir):
        super(TaskTracker, self).__init__()
        self.crab_dir = crab_dir
        self.name = self.crab_dir.split('/')[-1] 

    def _evalStatus(self):
        self.task_stats = crabCommand('status',self.crab_dir)
        self.crab_status = self.task_stats['status']
        self.errors = {}

        if self.crab_status == 'COMPLETED':
            self.status = 'DONE'
        elif self.crab_status == 'SUBMITTED':
            self.status = 'RUNNING'
        elif self.crab_status == 'FAILED':
            self.status = 'FAILED'
            self.errors = self.__getJobErrors()
        else:
            self.status = 'UNKOWN'

    def __getJobErrors(self):
        errorInfo= {}
        for pair in self.task_stats['jobList']:
            jobState = pair[0]
            jobNum = pair[1]

            if jobState == 'failed':
                jobDict = self.task_stats['jobs'][jobNum]
                if 'Error' not in jobDict.keys():
                    print ('!!!WARNING!!! "Error" key not in "jobs" dict for %s'%self.name)
                    errorInfo[jobNum] = 'UNKOWN'
                else:
                    errorInfo[jobNum] = jobDict['Error'][0] # error code

        return errorInfo #{<jobId>: <error code>}

class CrabTracker(object):
    """docstring for CrabTracker"""
    def __init__(self, jobdirs='crab_jobs/*'):
        super(CrabTracker, self).__init__()
        self.jobdirs = jobdirs
        self.tracking_file = 'status_tracking.json'

        if os.path.exists(self.tracking_file):
            self.crab_tracking = openJson(self.tracking_file)
        else:
            self.crab_tracking = {'DONE':[],'RUNNING':[],'FAILED':{}}
        
        self.done_data = self.crab_tracking['DONE'] # list of completed tasks
        self.running_data = self.crab_tracking['RUNNING'] # list of running tasks
        self.failed_data = self.crab_tracking['FAILED'] # {<task>:{
                                                        #   <jobId>: <error code>
                                                        #   }
                                                        # }
        self.__getAllTasks()        

    def __getAllTasks(self):
        self.unfinishedTasks = {}
        
        for d in self.__getTaskDirs():
            tracker = TaskTracker(d)

            if not self.__isDone(tracker):
                tracker._evalStatus()
                self.__add(tracker)

            if not self.__isDone(tracker):
                self.unfinishedTasks[tracker.name] = tracker

    def __getTaskDirs(self):
        if isinstance(self.jobdirs,list):
            task_dirs = self.jobdirs
        elif isinstance(self.jobdirs,str):
            task_dirs = glob.glob(self.jobdirs)
        else:
            raise TypeError('Cannot parse crab task directories\n\t= %s'%self.jobdirs)
        return task_dirs

    # Main way to add
    def __add(self,task):
        self.__cleanup(task) # remove from existing running and failed first
        if task.status == 'DONE':
            self.done_data.append(task.name)
        elif task.status == 'RUNNING':
            self.running_data.append(task.name)
        elif task.status == 'FAILED':
            self.failed_data[task.name] = task.errors
        else:
            raise Exception('Task status %s not supported for %s'%(task.status,task.name))        
            
    # Test if crab tracking knows about task
    def __isDone(self,task):
        return (task.name in self.done_data) 

    def __isRunning(self,task):
        return (task.name in self.running_data) 

    def __isFailed(self,task):
        return (task.name in self.failed_data.keys()) 

    def __cleanup(self,task):
        if self.__isRunning(task):
            self.running_data.pop(self.running_data.index(task.name))
        if self.__isFailed(task):
            del self.failed_data[task.name]        

    def Save(self):
        print '# -------------------------'
        print '# Crab Task Tracker Summary'
        print '# -------------------------'
        self.crab_tracking['DONE'] = self.done_data
        self.crab_tracking['RUNNING'] = self.running_data
        self.crab_tracking['FAILED'] = self.failed_data
        saveJson(self.tracking_file,self.crab_tracking)

    def SuggestResubmit(self):
        memoryErrorCodes = [50660]
        fileReadErrorCodes = [139]
        print '\n# --------------------------------'
        print '# Suggested resubmission commands:'
        print '# --------------------------------'
        late_print = []
        for taskname in self.failed_data.keys():
            task = self.unfinishedTasks[taskname]
            taskdir = task.crab_dir
            resubmit = 'crab resubmit '+taskdir
            print_last = False
            for job in self.failed_data[taskname].keys():
                if self.failed_data[taskname][job] in memoryErrorCodes:
                    print_last = True
                    if resubmit not in late_print:
                        late_print.append(resubmit)
            if not print_last:
                print resubmit               

        print '# ------------------------------------------------------------'
        print '# The following commands failed with memory related error code.\n# Consider adding --maxmemory option on resubmit.'
        print '# ------------------------------------------------------------'
        for c in late_print: print c

def openJson(filename):
    return json.loads(open(filename,'r').read())

def saveJson(filename,outdict):
    out_json = json.dumps(outdict,sort_keys=True,indent=4, separators=(',', ': '))
    print out_json
    out_file = open(filename,'w')
    out_file.write(json.dumps(outdict))
    out_file.close()

if __name__ == '__main__':
    # User input of directories to glob
    jobdirs = 'crab_jobs/*' if len(sys.argv)==1 else sys.argv[1:]

    crab_tracking = CrabTracker(jobdirs)
    crab_tracking.Save()
    crab_tracking.SuggestResubmit()