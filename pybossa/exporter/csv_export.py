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
CSV Exporter module for exporting tasks and tasks results out of PyBossa
"""

from pybossa.exporter import Exporter
import tempfile
from pybossa.core import uploader, task_repo
from pybossa.model.task import Task
from pybossa.model.task_run import TaskRun
from flask.ext.babel import gettext
from pybossa.util import UnicodeWriter
import pybossa.model as model
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
from flask import abort

from sqlalchemy.sql import text
from pybossa.core import db
import tempfile, json
from pybossa.util import UnicodeWriter
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage
from pybossa.core import uploader
from pybossa.uploader import local
from flask import url_for, safe_join, send_file, redirect

class CsvExporter(Exporter):

    def _format_csv_properly(self, row, ty=None):
        tmp = row.keys()
        task_keys = []
        for k in tmp:
            k = "%s__%s" % (ty, k)
            task_keys.append(k)
        if (type(row['info']) == dict):
            task_info_keys = []
            tmp = row['info'].keys()
            for k in tmp:
                k = "%sinfo__%s" % (ty, k)
                task_info_keys.append(k)
        else:
            task_info_keys = []

        keys = sorted(task_keys + task_info_keys)
        values = []
        _prefix = "%sinfo" % ty
        for k in keys:
            prefix, k = k.split("__")
            if prefix == _prefix:
                if row['info'].get(k) is not None:
                    values.append(row['info'][k])
                else:
                    values.append(None)
            else:
                if row.get(k) is not None:
                    values.append(row[k])
                else:
                    values.append(None)

        return values

    def _handle_task(self, writer, t):
        writer.writerow(self._format_csv_properly(t.dictize(), ty='task'))

    def _handle_task_run(self, writer, t):
        writer.writerow(self._format_csv_properly(t.dictize(), ty='taskrun'))

    def _get_csv(self, out, writer, table, handle_row, id):
        for tr in getattr(task_repo, 'filter_%ss_by' % table)(project_id=id,
                                                              yielded=True):
            handle_row(writer, tr)
        out.seek(0)
        yield out.read()

    def _respond_csv(self, ty, id):
        try:
            # Export Task(/Runs) to CSV
            types = {
                "task": (
                    model.task.Task, self._handle_task,
                    (lambda x: True),
                    gettext(
                        "Oops, the project does not have tasks to \
                        export, if you are the owner add some tasks")),
                "task_run": (
                    model.task_run.TaskRun, self._handle_task_run,
                    (lambda x: True),
                    gettext(
                        "Oops, there are no Task Runs yet to export, invite \
                         some users to participate"))}
            try:
                table, handle_row, test, msg = types[ty]
            except KeyError:
                return abort(404)  # TODO!

            out = tempfile.TemporaryFile()
            writer = UnicodeWriter(out)
            t = getattr(task_repo, 'get_%s_by' % ty)(project_id=id)
            if t is not None:
                if test(t):
                    tmp = t.dictize().keys()
                    task_keys = []
                    for k in tmp:
                        k = "%s__%s" % (ty, k)
                        task_keys.append(k)
                    if (type(t.info) == dict):
                        task_info_keys = []
                        tmp = t.info.keys()
                        for k in tmp:
                            k = "%sinfo__%s" % (ty, k)
                            task_info_keys.append(k)
                    else:
                        task_info_keys = []
                    keys = task_keys + task_info_keys
                    writer.writerow(sorted(keys))

                return self._get_csv(out, writer, ty, handle_row, id)
            else:
                pass  # TODO
        except:  # pragma: no cover
            raise

    def _make_zip(self, project, ty):
        name = self._project_name_latin_encoded(project)
        csv_task_generator = self._respond_csv(ty, project.id)
        if csv_task_generator is not None:
            # TODO: use temp file from csv generation directly
            datafile = tempfile.NamedTemporaryFile()
            try:
                for line in csv_task_generator:
                    datafile.write(str(line))
                datafile.flush()
                csv_task_generator.close()  # delete temp csv file
                zipped_datafile = tempfile.NamedTemporaryFile()
                try:
                    zip = self._zip_factory(zipped_datafile.name)
                    zip.write(
                        datafile.name, secure_filename('%s_%s.csv' % (name, ty)))
                    zip.close()
                    container = "user_%d" % project.owner_id
                    file = FileStorage(
                        filename=self.download_name(project, ty), stream=zipped_datafile)
                    uploader.upload_file(file, container=container)
                finally:
                    zipped_datafile.close()
            finally:
                datafile.close()

    def download_name(self, project, ty):
        return super(CsvExporter, self).download_name(project, ty, 'csv')

    def pregenerate_zip_files(self, project):
        print "%d (csv)" % project.id
        self._make_zip(project, "task")
        self._make_zip(project, "task_run")

    _goldans_resp_details = []
    _goldans_resp_summary = {}
    def _create_user_info_cache(self):
        print '******** inside _create_user_info_cache'
        user_cache = {}
        sql = text('''SELECT id, name, email_addr FROM "user"''')
        results = db.slave_session.execute(sql)
        for row in results:
            key = row.id
            val = '"ID": ' + str(row.id) + ', "Name": ' + row.name + ', "Email": ' + row.email_addr 
            user_cache[key] = val
        return user_cache
        
    def generate_goldtask_reports(self, project):
        print '******** inside generate_reports'
        # identify total golden tasks
        user_cache = self._create_user_info_cache()
        proj_id = project.id
        sql = text('''
                   SELECT id, info, state FROM task 
                   WHERE project_id =:projid
                   ''')
        results = db.slave_session.execute(sql, dict(projid=proj_id))
        gold_questions = {}
        gold_answers = {}
        gold_tasks = []
        total_gold_tasks = 0
        print '******** fetching golden tasks info'
        for row in results:
            task_id = row.id
            info = json.loads(row.info)
            gold_ans = info.get('goldenAnswer')
            if  gold_ans is not None and gold_ans != "":
                # its a golden task
                gold_tasks.append(task_id)
                gold_questions[task_id] = row.info
                gold_answers[task_id] = gold_ans
        total_gold_tasks = len(gold_answers)
        print '******** Number of golden questions : %d' % total_gold_tasks
        
        if total_gold_tasks > 0:
            # find golden tasks correctness
            print '******** identify golden tasks correctness'
            gold_task_str = str(gold_tasks) 
            # replace [] with ()
            gold_task_str = gold_task_str.replace('[', '(').replace(']', ')')
            print '******** gold_task_str %r' % gold_task_str 
            sql = "SELECT user_id, task_id, info FROM task_run " \
                   "WHERE task_id IN " + gold_task_str + " ORDER BY task_id ASC"
            print '******** golden task sql: %r' % sql           
            results2 = db.slave_session.execute(sql)           
            
            self._goldans_resp_details = []
            self._goldans_resp_summary = {}
            for row2 in results2:
                userid = row2.user_id
                task_id = row2.task_id
                
                jobj = None
                if gold_answers.get(task_id) is not None:
                    gold_answer = gold_answers[task_id]
                    actual_answer = ""
                    if json.loads(row2.info) is not None:
                        jobj = json.loads(json.loads(row2.info))
                        print '******** jobj type %r, value %r' %(type(jobj), jobj)
                if jobj is not None:
                    actual_answer = jobj.values()[0]
                answer_status = ""
                count = 0
                '''
                _goldans_resp_summary is dict. values are stored as
                key = userid, value = (count of correct ans, userid)
                use get() operation on _goldans_resp_summary to avoid
                keyerror for elements getting added for first time
                '''
                valobj = self._goldans_resp_summary.get(userid)
                if  valobj != None:
                    print '******** type(valobj) %r, value: %r' %(type(valobj), valobj)
                    if valobj.get('Correct Golden Answers') is not None:
                        count = valobj.get('Correct Golden Answers')
                    print '******** count: %r' % count
                    
                print '******** actual_answer type %r, value: %r' % (type(actual_answer), actual_answer)
                print '******** gold_answer type %r, value: %r' % (type(gold_answer), gold_answer)
                if actual_answer == gold_answer:
                    answer_status = "correct"
                    count += 1
                    print '******** golden answer match found'
                else:
                    print '******** golden answer mismatch'
                    answer_status = "incorrect"
                full_response = {"Gold Question ID": task_id, "Gold Question": gold_questions[task_id],
                                 "Gold Answer Status": answer_status, "User ID": userid, 
                                 "Gold Answer (Actual)": actual_answer}
                self._goldans_resp_details.append(full_response)
                self._goldans_resp_summary[userid] = {"User Details": user_cache[userid], "Correct Golden Answers": count}

            if len(self._goldans_resp_details) > 0:    
                print '******** golden answer full details'
                for resp in self._goldans_resp_details:
                    print resp
            if len(self._goldans_resp_summary) > 0:        
                print '******** golden answer summary'
                for key in self._goldans_resp_summary:
                    print 'userid: %r has total correct answers: %r' % (key, self._goldans_resp_summary[key]['Correct Golden Answers'])
                return self._make_gold_zip(project, "golden_tasks")
        return redirect(url_for('.index'))
        
            
    def _write_csv_header(self, writer, row):
       if row is not None:
          if (type(row) == dict):
             keys = row.keys() #keys = sorted(row.keys())
             writer.writerow(keys)
             return keys
          else:
             pass
       else:
          pass


    def _write_csv_value(self, writer, row):
       if row is not None:
          if (type(row) == dict):
             keys = row.keys() #keys = sorted(row.keys())
             values = []
             if keys is not None:
                for k in keys:
                   if row.get(k) is not None:
                      values.append(row[k])
                   else:
                      values.append(None)
                writer.writerow(values)
             else:
                pass
          else:
             pass
       else:
          pass

    def _write_csv(self, writer):
       jlist = self._goldans_resp_details
       #jlist = json.loads(jbuffer)
       assert(jlist)

       row1 = jlist[0]
       if row1 is not None:
          self._write_csv_header(writer, row1)
          for row in jlist:
             self._write_csv_value(writer, row)

    def _write_csv_summary(self, writer):
       sdict = self._goldans_resp_summary
       size = len(sdict)
       if size > 0:
          k = sdict.keys()[0]
          row1 = sdict[k]
       if row1 is not None:
          self._write_csv_header(writer, row1)
          for key in sdict:
             row = sdict[key]
             self._write_csv_value(writer, row)
             
             
    def _make_gold_zip(self, project, ty):
        print '****** entered _make_zip *******'
        # detailed csv    
        file_full = tempfile.NamedTemporaryFile()
        wfull = UnicodeWriter(file_full)
        self._write_csv(wfull)
        file_full.flush()
        
        #summarized csv
        file_summary = tempfile.NamedTemporaryFile()
        wfull = UnicodeWriter(file_summary)
        self._write_csv_summary(wfull)
        file_summary.flush()
        
        print '********** file names, file_full: %s, file_summary: %s' % (file_full, file_summary)
        name = self._project_name_latin_encoded(project)			
        #datafile = tempfile.NamedTemporaryFile()
        try:
            # for line in json_buffer:
                # datafile.write(str(line))
            # datafile.flush()
            zipped_datafile = tempfile.NamedTemporaryFile()
            try:
                zip = self._zip_factory(zipped_datafile.name)
                print '******** zip filename: %s' % zip
                #zip.write(datafile.name, secure_filename('%s_%s.json' % (name, ty)))
                zip.write(file_full.name, secure_filename('%s_%s_full.csv' % (name, ty)))
                zip.write(file_summary.name, secure_filename('%s_%s_summary.csv' % (name, ty)))
                zip.close()
                container = "user_%d" % project.owner_id
                file = FileStorage(filename=self.download_name(project, ty), stream=zipped_datafile)
                uploader.upload_file(file, container=container)
                print '****** zip file %s upload complete *******' % file
            finally:
                zipped_datafile.close()
        finally:
            file_full.close()
            file_summary.close()
            
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