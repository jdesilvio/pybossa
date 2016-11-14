# -*- coding: utf8 -*-
# This file is part of PyBossa.
#
# Copyright (C) 2014 SF Isle of Man Limited
#
# PyBossa is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# PyBossa is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with PyBossa.  If not, see <http://www.gnu.org/licenses/>.
# Cache global variables for timeouts
"""
JSON Exporter module for exporting tasks and tasks results out of PyBossa
"""
import logging
logging.basicConfig(level=logging.DEBUG)

from pybossa.exporter import Exporter
import json
import tempfile
from pybossa.core import uploader, task_repo
from pybossa.uploader import local
from flask import url_for, safe_join, send_file, redirect
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
from sqlalchemy.sql import text
from pybossa.core import db

class JsonExporter(Exporter):
    def _gen_json(self, table, id):
        n = getattr(task_repo, 'count_%ss_with' % table)(project_id=id)
        sep = ", "
        yield "["
        for i, tr in enumerate(getattr(task_repo, 'filter_%ss_by' % table)(project_id=id, yielded=True), 1):
            item = json.dumps(tr.dictize())
            if (i == n):
                sep = ""
            yield item + sep
        yield "]"

    def _respond_json(self, ty, id):    # TODO: Refactor _respond_json out?
        # TODO: check ty here
        return self._gen_json(ty, id)

    def _make_zip(self, project, ty):
        name = self._project_name_latin_encoded(project)
        json_task_generator = self._respond_json(ty, project.id) 
        if json_task_generator is not None:
            datafile = tempfile.NamedTemporaryFile()
            try:
                for line in json_task_generator:
                    datafile.write(str(line))
                datafile.flush()
                zipped_datafile = tempfile.NamedTemporaryFile()
                try:
                    zip = self._zip_factory(zipped_datafile.name)
                    zip.write(datafile.name, secure_filename('%s_%s.json' % (name, ty)))
                    zip.close()
                    container = "user_%d" % project.owner_id
                    file = FileStorage(filename=self.download_name(project, ty), stream=zipped_datafile)
                    uploader.upload_file(file, container=container)
                finally:
                    zipped_datafile.close()
            finally:
                datafile.close()

    def download_name(self, project, ty):
        return super(JsonExporter, self).download_name(project, ty, 'json')


    def pregenerate_zip_files(self, project):
        print "%d (json)" % project.id
        self._make_zip(project, "task")
        self._make_zip(project, "task_run")

    def create_cust_exp_zip(self, project):
        print "**** custom export task zip %d (json) ****" % project.id
        return self._make_cust_exp_zip(project, "task_run")

    def _respond_cust_exp_json(self, proj_id):
        return self._gen_cust_exp_json(proj_id)

    def _create_user_info_cache(self):
        user_cache = {}
        sql = text('''SELECT id, name, email_addr FROM "user"''')
        results = db.slave_session.execute(sql)
        for row in results:
            key = row.id
            val = 'user_name: ' + row.name + ', user_email: ' + row.email_addr 
            user_cache[key] = val
        return user_cache
            
    def _gen_consensus(self, proj_id):
        """
        generate consensus statistics for each completed
        task for the project, proj_id. this will be recorded
        under "task_run.calibration" field
        1. get consensus threshold for the project, proj_id
        2. get all completed tasks for the project, proj_id
        3. compare count of completed tasks in task_run against
        task.n_answers (task redundancy, i.e. allowed contributors)
        4. if #3 is true, stamp task_run.calibration with value
        based on matching answers from different contributors
        For example:
        if there are 3 contributors permitted (n_answers = 3) and
        answer of 1st contributor matches with one another contributor,
        the task_run.calibration will be 2/3 for two contributors
        having same answers and task_run.calibration will be 1/3 for
        the other contributor whose answer did not matched with the
        rest two contributors
        """
        # get project consensus threshold
        sql = text('''
                   SELECT calibration_frac FROM project 
                   WHERE id =:projid
                   ''')
        results = db.slave_session.execute(sql, dict(projid=proj_id))
        consensus_threshold = 60 # default
        for row in results:
            consensus_threshold = row.calibration_frac

        # get all completed tasks for which consensus has not been built
        # i.e the ones with task.calibration as zero
        task_obj = dict(proj_id=None,
                        task_id=None) # key = task_id, value = n_answers
        sql = text('''
                   SELECT id, project_id, n_answers FROM task 
                   WHERE project_id =:projid AND calibration = 0 
                   AND state LIKE 'completed' 
                   ''')
        results = db.slave_session.execute(sql, dict(projid=proj_id))
        logging.debug('****** fetching completed task with no consensus built *****')
        consensus_list = {}
        for row in results:
            task_obj['proj_id'] = proj_id
            task_obj[row.id] = row.n_answers
            logging.debug('project_id: %d, task_id: %d, n_answers: %d',
                           row.project_id, row.id, row.n_answers)
            taskid = row.id
            n_answers = row.n_answers
            sql = text('''
                       SELECT id, task_id, info FROM task_run 
                       WHERE project_id =:projid AND task_id =:taskid
                       AND calibration IS NULL ORDER BY task_id
                       ''') 
            res2 = db.slave_session.execute(sql, dict(projid=proj_id, taskid=taskid))
            nrecs = res2.rowcount
            if (nrecs == n_answers):
                logging.debug('MATCH: task run numrows with n_answers')
            else:
                logging.debug('MISMATCH: task run numrows(%d) with n_answers(%d)',
                               nrecs, n_answers)
                continue

            logging.debug('****** fetching task runs for consensus *****')
            info = []   # list of answers for each task id
            l = []      # list of taskrun ids (and not task ids)
            consensus_dict = {} # clean consensus dict to be populated with actual consensus
            for r in res2:
                logging.debug('task run id: %d, task_id: %d, info %s',
                              r.id, r.task_id, r.info)                          
                l.append(r.id)
                info.append(r.info)
                consensus_dict[r.id] = 0
            logging.debug('clean consensus_dict: %r', consensus_dict)

            # build consensus
            freq_dict = {}
            contrib_dict = {}
            num_ans = len(info)
            if num_ans <= 0:
                continue
            
            print '**** info to freq_dict : %r ****' % info
            multiplier = 100.0 / num_ans
            for i,l in zip(info,l):
                freq_dict[i] = freq_dict.get(i, 0.0) + multiplier
                contrib_dict.setdefault(i, []).append(l)

            # if its a yes/no type of question, consensus
            # would be build based on % of yes answers
            # make sure its not a yes/no type of question 
            # with all no answers
            print '**** freq_dict : %r ****' % freq_dict
            yes_answers = None
            no_answers = None
            #import pdb
            #pdb.set_trace()
            for key in freq_dict:
                print 'freq_dict - key: %r, value: %r' %(key, freq_dict[key])
                if (key in ['"yes"', 'yes', "yes", '"Yes"', 'Yes', "Yes"]):
                    yes_answers = freq_dict.get(key)
                if (key in ['"no"', 'no', "no", '"No"', 'No', "No"]):
                    no_answers = freq_dict.get(key)
                    
            print 'yes_answers: %r. no_answers: %r' %(yes_answers, no_answers)
            if yes_answers == None and no_answers is None:
                print '*** sorted consensus ***'
                best_consensus = sorted(freq_dict.items(), key=lambda kv: kv[1])[-1]
            else:
                if yes_answers == None: 
                    yes_answers = -1    # -1 is to mark consensus calculated but no record selected
                                        # as value 0 means consensus has not been calculated
                print '**** consensus from Yes answers. yes_answers : %r****' % yes_answers
                best_consensus = ('Yes', yes_answers)
                
            if best_consensus[1] >= consensus_threshold:
                print 'consensus for %s is %g%%' % best_consensus
                #print 'contributors for this answer are:', contrib_dict[best_consensus[0]]
            else:
                print 'No consensus found'

            print 'consensus: %r' % contrib_dict
            # perform db update with consensus figures
            # db.session.execute('BEGIN')
            for key in contrib_dict:
                taskruns = contrib_dict[key]
                count = len(taskruns)
                if count <= 0:
                    continue
                
                strtaskruns = '('
                i = 1
                for v in taskruns:
                    strtaskruns += str(v)
                    if (i < count):
                        strtaskruns += ', '
                    i += 1

                strtaskruns += ')'
                
                print strtaskruns
                consensus = freq_dict[key]
                print "Task runs: %s  has consensus %d" % (taskruns, consensus)
                sql = 'UPDATE task_run SET calibration =' \
                      + str(consensus) + ' WHERE id IN ' + strtaskruns 
                print sql
                db.session.execute(sql)
            # update task table with best(max) consensus
            sql  = 'UPDATE task SET calibration = ' + str(best_consensus[1]) + ' WHERE id = ' + str(taskid) 
            print sql
            db.session.execute(sql)
            db.session.execute('COMMIT')
# TODO : Commented for testing : Uncomment later : END 

            # ###### Generate Consensus : BEGIN ########
            # total = len(l)
            # for i, val in enumerate(info):
               # match_count = 1
               # taskrun_id = l[i]
               # #pdb.set_trace()
               # if consensus_dict[taskrun_id] != 0:
                  # continue

               # matched_taskruns = []
               # matched_taskruns.append(taskrun_id)
               # print "finding consensus for taskrun_id: %d, info: %s" %(taskrun_id, val)
               # idx = i + 1
               # for j, val2 in enumerate(info[i+1:]):
                  # #print "comparing %s with %s" %(val, val2)
                  # if val == val2:
                     # print "** MATCH FOUND : [i=%d:%s],[idx=%d:%s] **" % (i, val, idx, val2)
                     # matched_taskruns.append(l[idx])
                     # match_count += 1
                  # idx += 1
               # print "matched taskruns for taskrun %d is : %r" % (taskrun_id, matched_taskruns)
               # #print "match_count: %d, total_count: %d" % (match_count, total)
               # consensus = (match_count * 100 ) / total
               # #update consensus_dict with consensus for matched taskruns
               # for tid in matched_taskruns:
                  # consensus_dict[tid] = consensus
            # print "consensus for %r is: %r" % (matched_taskruns, consensus_dict)
            # ###### Generate Consensus : END ########
            
            
        
    def _gen_cust_exp_json(self, proj_id):
        """Return all completed tasks"""
        self._gen_consensus(proj_id)
        logging.debug('******* entered _gen_cust_exp_json ******')
        user_cache = self._create_user_info_cache()
        for k,v in user_cache.items():
            print "user_cache : key = %s, val = %s" %(k, user_cache[k])
        
        # get project consensus threshold
        sql = text('''
                   SELECT id, name, short_name, calibration_frac FROM project 
                   WHERE id =:projid
                   ''')
        results = db.slave_session.execute(sql, dict(projid=proj_id))
        consensus_threshold = 0 
        for row in results:
            consensus_threshold = row.calibration_frac

        if consensus_threshold < 60:
            consensus_threshold = 60 # default
            
        # setup project details for export
        project_details = '{"project_details" : {"id" : ' + \
                            str(row.id) + ', "name": "' + row.name + \
                            '", "shortname": "' + row.short_name + \
                            '", "consensus_threshold": "' + str(row.calibration_frac) + '"}'
        print '********** project_details : %r' % project_details        
        sql = text('''
                   SELECT id, info, created, calibration FROM task 
                   WHERE project_id =:projid AND calibration >=:calibration
                   ''') # TODO : modify query to consider recently calibrated tasks only 
                   
        results = db.slave_session.execute(sql, dict(projid=proj_id, calibration=consensus_threshold))
        if results is None:
            logging.debug('No completed tasks for Project.id %d' % proj_id)
            return
        
        n = results.rowcount
        if n == 0:
            print '*** rowcount zero from task table ***'
            print 'sql : %r', sql
            return

        logging.debug('number of tasks to export : %d', n)
        json_export = project_details
        json_export += ', "data": ['
        i = 1
        #n -= 1 # to avoid extra comma at the end
        sep = ", "
        data = ""
        for row in results:
            taskid = row.id
            calibration = row.calibration
            sql = text('''
            SELECT id, info, user_id, finish_time FROM task_run 
            WHERE project_id =:projid AND task_id =:taskid AND calibration >=:calibration 
            ORDER BY calibration DESC, id ASC
            ''')
            res2 = db.slave_session.execute(sql, dict(projid=proj_id, taskid=taskid, calibration=calibration))
            k = res2.rowcount
            if k <= 0:
                continue
                
            jstr1 = row.info    # to hold task details: task.info
            jstr2 = ""          # to hold task response: task_run.info
            print '******* jstr1 : %r' % jstr1
            jstr3 = '{"consensusPercentage": "' + str(row.calibration) + '"' + \
                    ', "task_created_on": "' + row.created + '"'
            rnum = 1
            j = 1
            #k -= 1 # to avoid extra comma at the end
            jstr3 += ', "metadata": "{'
            userinfo = ""
            for row2 in res2:
                # comma sep strings except the last one
                sep2 = ("" if (j == k) else ", ")              
                if rnum == 1:
                    # include answer only once
                    #jobj2 = json.loads(json.loads(row2.info)) # task_run.info
                    #print 'jobj2 : %r' % jobj2
                    jstr2 = row2.info
                    #data += row2.info 
                rnum += 1
                userinfo += '{' + user_cache[row2.user_id] + \
                            ', task_completed_on: ' + row2.finish_time + '}'
                userinfo += sep2
                j += 1
            jstr3 += userinfo + '}"}'
            print '******* jstr2 : %r' % jstr2
            print '******* jstr3 : %r' % jstr3
            sep = ("" if (i == n) else ", ")
            
            # club all json strings
            jobj1 = json.loads(jstr1)
            jobj2 = json.loads(json.loads(jstr2))          
            jobj3 = json.loads(jstr3)
            print 'jobj1 : %r ' % jobj1
            print 'jobj2 : %r ' % jobj2            
            print 'jobj3 : %r ' % jobj3
            
            jobj4 = dict(jobj1.items() + jobj2.items())
            print 'jobj4 : %r ' % jobj4
            datajsobj = dict(jobj1.items() + jobj2.items() + jobj3.items())
            json_export += json.dumps(datajsobj) + sep
            i += 1
        json_export += ']}'
        print '******* json_export: %r' % json_export
        return json_export
            
        # sql = text('''
                   # SELECT DISTINCT tr.info taskrun_info, tr.finish_time finish_time, 
                   # tr.user_id userid, t.info task_info, t.created create_time 
                   # FROM task_run tr JOIN task t ON tr.task_id = t.id  
                   # WHERE tr.calibration >=:calibration AND tr.task_id IN
                   # (SELECT id FROM task WHERE tr.project_id =:projid AND calibration >=:calibration)
                   # ''')
        # results = db.slave_session.execute(sql, dict(projid=proj_id, calibration=consensus_threshold))
            
        # #import pdb
        # #pdb.set_trace()
            
        
        # yield "["
        # i = 0
        # n -= 1 # to avoid extra comma at the end
        # for row in results:
            # userinfo = '{' + user_cache[row.userid] + \
                       # ', "task_created_on": "' + row.create_time + \
                       # '", "task_completed_on": "' + row.finish_time + '"}'
            # # comma sep strings except the last one
            # sep = ("" if (i == n) else ", ")  
            # # taskrun_info can contain user inputs in json form field, value pair
            # # if the project tasks are not just yes/no types. in such cases, during
            # # export, append user inputs from taskrun_info to the task info from task_info
            # item = ''
            # jobj = []
            # if (row.taskrun_info is not None):
                # taskrun = row.taskrun_info.lower()              # unicode taskrun ans
                # taskrun = taskrun.encode('ascii', 'ignore')     # ascii taskrun ans
                # print "taskrun: %r, row.task_info: %r" % (taskrun, row.task_info)                
                # if (taskrun in ['"yes"', 'yes', "yes", "Yes"]):
                    # item = row.task_info
                # elif (taskrun in ['"no"', 'no', "no", "No"]):
                    # i = i + 1
                    # continue
                # else:
                    # # to append task_info and taskrun_info,
                    # # convert to json object and revert to string
                    # print 'row.task_info: %r' % row.task_info
                    # jobj = json.loads(row.task_info)
                    # print 'jobj : %r' % jobj
                    # print 'row.taskrun_info: %r' % row.taskrun_info
                    # jobj2 = json.loads(json.loads(row.taskrun_info))
                    # print 'jobj2 : %r' % jobj2
                    # jobj.update(jobj2)
                    # print 'jobj : %r' % jobj
                    # print 'userinfo : %r' % userinfo
                    # jobj.update(userinfo)                    
                    # item = json.dumps(jobj)
            # yield item + sep
            # i = i + 1
        # yield "]"
        # logging.debug('****** exit gen_cust_exp_json : *******')

    def _make_cust_exp_zip(self, project, ty):
        logging.debug('****** entered create_zip *******')
        json_buffer = self._respond_cust_exp_json(project.id)
        if json_buffer is None:
            logging.debug('buffer is null')
            logging.debug('****** exit create_zip *******')
            return

        name = self._project_name_latin_encoded(project)			
        datafile = tempfile.NamedTemporaryFile()
        try:
            for line in json_buffer:
                datafile.write(str(line))
            datafile.flush()
            zipped_datafile = tempfile.NamedTemporaryFile()
            try:
                zip = self._zip_factory(zipped_datafile.name)
                logging.debug('zip filename: %s', zip)
                zip.write(datafile.name, secure_filename('%s_%s.json' % (name, ty)))
                zip.close()
                container = "user_%d" % project.owner_id
                file = FileStorage(filename=self.download_name(project, ty), stream=zipped_datafile)
                uploader.upload_file(file, container=container)
                logging.debug('****** zip file %s upload complete *******', file)
            finally:
                zipped_datafile.close()
        finally:
            datafile.close()
            
        # create response; copied from Exporter:get_zip()
        filename = self.download_name(project, ty)
        if isinstance(uploader, local.LocalUploader):
            filepath = self._download_path(project)
            res = send_file(filename_or_fp=safe_join(filepath, filename),
                            mimetype='application/octet-stream',
                            as_attachment=True,
                            attachment_filename=filename)
            return res
        else:
            return redirect(url_for('rackspace', filename=filename,
                                    container=self._container(project),
                                    _external=True))
        


