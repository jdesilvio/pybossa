from pybossa.exporter import Exporter
import json
from sqlalchemy.sql import text
from pybossa.core import db
import tempfile
from pybossa.util import UnicodeWriter
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage
from pybossa.core import uploader
from pybossa.uploader import local
from flask import url_for, safe_join, send_file, redirect

class GoldenTaskReports(Exporter):
    _goldans_resp_details = []
    _goldans_resp_summary = {}
    def _create_user_info_cache(self):
        print '******** inside _create_user_info_cache'
        user_cache = {}
        sql = text('''SELECT id, name, email_addr FROM "user"''')
        results = db.slave_session.execute(sql)
        for row in results:
            key = row.id
            val = 'user_name: ' + row.name + ', user_email: ' + row.email_addr 
            user_cache[key] = val
        return user_cache
        
    def generate_reports(self, project):
        print '******** inside generate_reports'
        # identify total golden tasks
        user_cache = self._create_user_info_cache()
        proj_id = project.id
        sql = text('''
                   SELECT id, info, state FROM task 
                   WHERE project_id =:projid
                   ''')
        results = db.slave_session.execute(sql, dict(projid=proj_id))
        gold_answers = {}
        gold_tasks = []
        print '******** fetching golden tasks info'
        for row in results:
            task_id = row.id
            info = json.loads(row.info)
            gold_ans = info.get('goldenAnswer')
            print '******** gold_ans: %r' % gold_ans
            if  gold_ans is not None and gold_ans != "":
                # its a golden task
                #self._gold_count += 1;
                gold_tasks.append(task_id)
                gold_answers[task_id] = gold_ans
        total_gold_tasks = len(gold_answers)
        
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
        
        #_goldans_resp_details = [] # {{214, user1, correct}, {214, user2, incorrect})
        #_goldans_resp_summary = {} #{{user1, total:10, correct: 10}, {user2, total:10, correct: 8}}
        for row2 in results2:
            userid = row2.user_id
            task_id = row2.task_id
            gold_answer = gold_answers[task_id]
            actual_answer = ""
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
                count = valobj['count']
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
            full_response = {"taskid": task_id, "userid": userid, "gold_ans_status": answer_status, "answer": actual_answer}
            self._goldans_resp_details.append(full_response)
            self._goldans_resp_summary[userid] = {"userid": userid, "count": count} #(count, userid)

        print '******** golden answer full details'
        for resp in self._goldans_resp_details:
            print resp
        print '******** golden answer summary'
        #for k,(count, userid) in self._goldans_resp_summary.items():
        for key in self._goldans_resp_summary:
            print 'userid: %r has total correct answers: %r' % (key, self._goldans_resp_summary[key]['count'])
        self._make_zip(project, "golden_tasks")
        
            
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
             
    def download_name(self, project, ty):
        return super(GoldenTaskReports, self).download_name(project, ty, 'csv`')
             
    def _make_zip(self, project, ty):
        print '****** entered _make_zip *******'
        # json_buffer = self._respond_cust_exp_json(project.id)
        # if json_buffer is None:
            # logging.debug('buffer is null')
            # logging.debug('****** exit create_zip *******')
            # return

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
            #datafile.close()
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
            