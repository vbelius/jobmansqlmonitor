#import numpy
#from string import Template
import matplotlib
matplotlib.use('agg')
import pylab as plt
#import sys
#import cPickle
#import theano
#import os
#import scipy.io.wavfile
#import itertools
import threading
import SimpleHTTPServer
import BaseHTTPServer
#import logging
import io
import traceback
#import os
#import hashlib
import time
#import base64
import urlparse
import jinja2
import psycopg2
import jobman.sql
import datetime
import sys
import urllib

from collections import OrderedDict

import json

server = {} # Filled in from command line

def jobman_status_string( i ):
    d = {jobman.sql.START: 'QUEUED', jobman.sql.RUNNING: 'RUNNING',
         jobman.sql.DONE: 'DONE',   jobman.sql.ERR_START: 'ERR_START',
         jobman.sql.ERR_SYNC: 'ERR_SYNC', jobman.sql.ERR_RUN: 'ERR_RUN',
         jobman.sql.CANCELED: 'CANCELED'}
    if i in d.keys():
        return d[i]
    else:
        return str(i)



# From https://github.com/liudmil-mitev/experiments/blob/master/time/humanize_time.py
INTERVALS = [1, 60, 3600, 86400, 604800, 2419200, 29030400]
NAMES = [('second', 'seconds'),
         ('minute', 'minutes'),
         ('hour',   'hours'),
         ('day',    'days'),
         ('week',   'weeks'),
         ('month',  'months'),
         ('year',   'years')]
def humanize_time(amount, units):
   '''
      Divide `amount` in time periods.
      Useful for making time intervals more human readable.

      >>> humanize_time(173, "hours")
      [(1, 'week'), (5, 'hours')]
      >>> humanize_time(17313, "seconds")
      [(4, 'hours'), (48, 'minutes'), (33, 'seconds')]
      >>> humanize_time(90, "weeks")
      [(1, 'year'), (10, 'months'), (2, 'weeks')]
      >>> humanize_time(42, "months")
      [(3, 'years'), (6, 'months')]
      >>> humanize_time(500, "days")
      [(1, 'year'), (5, 'months'), (3, 'weeks'), (3, 'days')]
   '''
   result = []

   unit = map(lambda a: a[1], NAMES).index(units)
   # Convert to seconds
   amount = amount * INTERVALS[unit]

   for i in range(len(NAMES)-1, -1, -1):
      a = amount // INTERVALS[i]
      if a > 0: 
         result.append( (a, NAMES[i][1 % a]) )
         amount -= a * INTERVALS[i]

   return result

class JobmanMonitorServer(SimpleHTTPServer.SimpleHTTPRequestHandler):    
    def do_GET(self):
        path = urlparse.urlparse(self.path).path
        args = urlparse.parse_qs(urlparse.urlparse(self.path).query)
        if path=='/':
            commands = filter( lambda x: x[0:3]=="do_" and x!="do_HEAD" and x!="do_GET", dir(self) )
            self.send_response(200, 'OK')
            self.send_header('Content-type', 'html')
            self.end_headers()
            self.wfile.write( "Available commands: " )
            map( lambda x: self.wfile.write(x[3:] + " "), commands )
        else:
            command = "do_" + path[1:]
            if command in dir(self):
                func = getattr( self, command )
                try:
                    func(args)
                except:
                    self.send_python_error()
            else:
                self.send_error(404, "File not found")
                return None

    def do_delete_experiment( self, args ):
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()
        if not 'experimentid' in args.keys():
            self.wfile.write('Need to supply job id, e.g. delete_experiment?experimentid=0')
            return
        eid = int(args['experimentid'][0])
        
        cur = self.get_cursor()
        global server
        query1 = "delete from %skeyval where dict_id=%d;"%( server['tablename'], eid )
        query2 = "delete from %strial where id=%d;"%( server['tablename'], eid )
        cur.execute(query1 )
        self.conn.commit()
        self.wfile.write( "Deleted %d rows\n"%cur.rowcount )
        cur.execute(query2 )
        self.conn.commit()
        self.wfile.write( "Deleted %d rows"%cur.rowcount )       
    
    def do_reschedule_experiment( self, args ):
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()
        if not 'experimentid' in args.keys():
            self.wfile.write('Need to supply job id, e.g. reschedule_experiment?experimentid=0')
            return
        eid = int(args['experimentid'][0])
        if 'force' in args.keys():
            force = args['force'][0]
            if force=='true':
                force = True
            else:
                force = False
        else:
            force = False
        
        cur = self.get_cursor()
        cur.execute( "select status from %strial where id=%d;"%(server['tablename'],eid) )
        rows = cur.fetchall()
        if len(rows)==0:
            self.wfile.write("No job with ID %d"%eid)
            return
        if int(rows[0][0])==jobman.sql.START:
            self.wfile.write("Job with ID %d is already queued"%eid)
            return
        if int(rows[0][0])==jobman.sql.RUNNING and (not force):
            self.wfile.write("Job with ID %d is running. Are you sure you want to restart? If so ?force=true "%eid)
            return
        
        query1 = "update %strial set status=%d where id=%d;"%(server['tablename'],jobman.sql.START,eid)
        cur.execute( query1 )
        self.conn.commit()
        self.wfile.write( "Updated %d rows\n"%cur.rowcount )
        query2 = "update %skeyval set ival=%d where dict_id=%d and name='jobman.status';"%(server['tablename'],jobman.sql.START,eid)
        cur.execute( query2 )
        self.conn.commit()
        self.wfile.write( "Updated %d rows"%cur.rowcount )
        
        print query1
        print query2

    def do_experiment_yaml_template( self, args ):
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()
        if not 'experimentid' in args.keys():
            self.wfile.write('Need to supply job id, e.g. experiment_yaml_template?experimentid=0')
        eid = int(args['experimentid'][0])
        
        cur = self.get_cursor()
        cur.execute( "select yamltemplate from " + server['view'] + " where id=" + str(eid) + ";")
        rows = cur.fetchall()
        if len(rows)==0:
            self.wfile.write('The view does not have a job with id ' + str(eid))
            return
        yaml = rows[0][0]
        self.wfile.write(yaml)
        
    
    def get_cursor( self ):
        if not hasattr(self, 'conn' ):
            try:
                self.conn = psycopg2.connect("dbname='" + server['dbname'] +
                                        "' user='" + server['user'] +
                                        "' host='" + server['host'] + 
                                        "' password='"+ server['password'] + "'")
            except:
                self.send_python_error()
                raise Exception("Could not connect to database")
        if not hasattr( self, 'cursor' ):
            self.cursor = self.conn.cursor()
        return self.cursor
    
    def do_render_graph( self, args ):
        eid = int(args['experimentid'][0])
        colname = args['colname'][0]
        if 'from_epoch' in args.keys():
            from_epoch = int(args['from_epoch'][0])
        else:
            from_epoch = 0
        #graph_spec = args['graph_spec'][0]
        #graph_spec=urllib.unquote_plus( graph_spec ) 
        
        cur = self.get_cursor()
        cur.execute( "select results_%s from %s where id=%d;"%(colname, server['view'], eid) )
        rows = cur.fetchall()
        if len(rows)>0:
            graph_spec = rows[0][0]
            graph_spec = json.loads( graph_spec )
            assert graph_spec[0]=='graph'
            yaxis = graph_spec[1]
            xaxis = graph_spec[2]
            curves = graph_spec[3]
            
            plt.figure()
            plt.xlabel( xaxis )
            plt.ylabel( yaxis )
            for label,curve in curves.items():
                print len(range(from_epoch, len(curve))), len(curve)
                plt.plot( range(from_epoch, len(curve)), curve[from_epoch:], label=label )
            plt.legend()
                
            #buf = io.BytesIO()
            plt.savefig(self.wfile, format = 'png')
            #buf.seek(0)
            #buf.close()
    
    def do_monitor(self, args):
        global server
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()
        
        colstoget = ["id", "jobman_status", "jobman_sql_hostname", "jobman_sql_hostworkdir", "jobman_starttime", "jobman_endtime", "jobman_runtime", "jobman_lastupdatetime"]
        colnames = self.get_column_names()
        
        result_columns = filter( lambda x: x.startswith("results_"), colnames )
        hyperparam_columns = filter( lambda x: x.startswith("hyperparameters_"), colnames )
        
        colstoget = colstoget + result_columns + hyperparam_columns
        result_columns = map( lambda x: x[len("results_"):], result_columns )
        hyperparam_columns = map( lambda x: x[len("hyperparameters_"):], hyperparam_columns )
        
        print hyperparam_columns
        
        cur = self.get_cursor()
        query = "select " + ",".join(filter( lambda x: x in colnames, colstoget )) + " from " + server['view'] + " order by id;"

        cur.execute( query )
        rows = cur.fetchall()
        for i in range(len(rows)):
            rows[i] = list(rows[i])
            for j in range(len(colstoget)):
                if not colstoget[j] in colnames:
                    rows[i].insert(j, None )
        for i in range(len(rows)):
            rows[i][1] = jobman_status_string( rows[i][1] ) # status
            if rows[i][4]!=None: # start time
                rows[i][4] = datetime.datetime.fromtimestamp(float(rows[i][4])).strftime('%Y-%m-%d %H:%M:%S')
            if rows[i][5]!=None: # end time
                rows[i][5] = datetime.datetime.fromtimestamp(float(rows[i][5])).strftime('%Y-%m-%d %H:%M:%S')
            if rows[i][6]!=None: # run time
                rows[i][6] = " ".join( map( lambda unit: " ".join( map( lambda x: str(x), unit ) ), humanize_time(int(rows[i][6]), "seconds") ) )
            if rows[i][7]!=None: # last update time
                rows[i][7] = " ".join( map( lambda unit: " ".join( map( lambda x: str(x), unit ) ), humanize_time(int(time.time()-float(rows[i][7])), "seconds") ) ) + " ago"
                if rows[i][0]!='RUNNING': # Don't display last update time if experiment has ended
                    rows[i][7] = None
            therest = rows[i][8:]
            if len(therest)>0:
                rows[i][8] = {} # results
                rows[i][9] = {} # hparams
                for j,col in enumerate(result_columns):
                    try:
                        rows[i][8][col] = json.loads(therest[j])
                    except:
                        rows[i][8][col] =  therest[j]
                    if isinstance( rows[i][8][col], list):
                        if rows[i][8][col][0]=='graph':
                            #imgurl = "/render_graph?graph_spec=%s"%(urllib.quote_plus( results[j] ))
                            imgurl = "/render_graph?experimentid=%d&colname=%s"%(rows[i][0],col)
                            scale = "10%"
                            rows[i][8][col] = "<a href=\"%s\"><img height=\"%s\" src=\"%s\"></a>"%(imgurl,scale,imgurl)
                for j,col in enumerate(hyperparam_columns):
                    rows[i][9][col] = therest[len(result_columns) + j]

        template = """
                   <html>
                     <body>
                       <center><h1>Table: {{ server['tablename'] }}</h1></center>
                       Missing information in the table? Try running jobman sqlview (see below)<br>
                       <table border="1">
                          <tr>
                              <td colspan=2><center><b>Control</b></center></td>
                              <td colspan=9><center><b>Jobman data</b></center></td>
                              <td colspan={{ hyperparam_columns|length }}><center><b>Hyperparams</b></center></td>
                              <td colspan={{ result_columns|length }}><center><b>Results</b></center></td>
                          </tr>
                          <tr>
                              <td><b>Del</b></td>
                              <td><b>Resched</b></td>
                          
                              <td><b>ID</b></td>
                              <td><b>Status</b></td>
                              <td><b>Yaml<br>template</b></td>
                              <td><b>Execution<br>host</b></td>
                              <td><b>Host<br>work dir</b></td>
                              <td><b>Start<b>time</b></td>
                              <td><b>End<b>time</b></td>
                              <td><b>Run<b>time</b></td>
                              <td><b>Last<b>update time</b></td>
                              
                              {% for col in hyperparam_columns %}
                                 <td><b>{{ col }}</b></td>
                              {% endfor %}
                                                            
                              {% for col in result_columns %}
                                 <td><b>{{ col }}</b></td>
                              {% endfor %}
                          </tr>
                          {% for row in rows %}
                             <tr>
                                <td> <a href="/delete_experiment?experimentid={{ row[0] }}">x</a></td>
                                <td> <a href="/reschedule_experiment?experimentid={{ row[0] }}">o</a></td>
                             
                                <td>{{ row[0] }}</td>
                                <td>{{ row[1] }}</td>
                                <td><a href="/experiment_yaml_template?experimentid={{ row[0] }}">yaml</a></td>
                                <td>{{ row[2] }}</td>
                                <td><span title="{{ row[3] }}">here</span></td>
                                <td>{{ row[4] }}</td>
                                <td>{{ row[5] }}</td>
                                <td>{{ row[6] }}</td>
                                <td>{{ row[7] }}</td>

                                {% for col in hyperparam_columns %}
                                   <td>{{ row[9][col] }}</td>
                                {% endfor %}                                
                                
                                {% for col in result_columns %}
                                   <td>{{ row[8][col] }}</td>
                                {% endfor %}
                             </tr>
                          {% endfor %}
                        </table>
                        <br>
                        To create the view needed by this script:<br>
                        jobman sqlview postgresql://{{ server['user'] }}:{{ server['password'] }}@localhost/{{ server['dbname'] }}?table={{ server['tablename'] }} {{ server['view'] }} 
                        <br>
                        To schedule a job:<br>
                        jobman -f sqlschedule postgresql://{{ server['user'] }}:{{ server['password'] }}@localhost/{{ server['dbname'] }}?table={{ server['tablename'] }} experiment.train_experiment [conf file]<br>
                        To run a job:<br>
                        jobman sql postgresql://{{ server['user'] }}:{{ server['password'] }}@localhost/{{ server['dbname'] }}?table={{ server['tablename'] }} .<br>
                     </body>
                   </html>"""
        
        env = jinja2.Environment( loader=jinja2.DictLoader( {'output':  template} ) )
        self.wfile.write( env.get_template('output').render(rows=rows, result_columns=result_columns, server=server, hyperparam_columns=hyperparam_columns) )
    
    def get_column_names( self ):
        cur = self.get_cursor()
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='%s'"%server['view'])
        return map( lambda x: x[0], cur.fetchall() )
        
#    def send_ascii_encoded_array( self, arr ):
#        self.send_response(200, 'OK')
#        self.send_header('Content-type', 'html')
#        self.end_headers()
#        ascii = base64.b64encode( cPickle.dumps( arr ) )
#        self.wfile.write( ascii )
#
    def send_python_error(self):
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()
        self.wfile.write( traceback.format_exc() )
        print traceback.format_exc()


def start_web_server( wait = True ):
    def web_server_thread():
        httpd = None
        for port in range(8000, 8010):
            server_address = ('', port)
            try:
                httpd = BaseHTTPServer.HTTPServer(server_address, JobmanMonitorServer)
                break
            except:
                print "Could not start on port",port,", trying next"
        assert httpd!=None
        sa = httpd.socket.getsockname()
        print "Serving HTTP on", sa[0], "port", sa[1], "..."
        httpd.serve_forever()

    t = threading.Thread( target = web_server_thread) #, args = (self,) )
    t.daemon = True
    t.start()
    if wait:
        try:
            while True:
                time.sleep( 10000 )
        except KeyboardInterrupt:
            return

if __name__ == "__main__":
    if len(sys.argv)<6:
        print "Need arguments host user password dbname tablename [view]"
        print "(View is optional, if omitted view is set to [tablename]view"
    else:
        server['host'] = sys.argv[1]#'localhost'
        server['user'] = sys.argv[2]#'belius'
        server['password'] = sys.argv[3]#'9ee2c33138'
        server['dbname'] = sys.argv[4]#'belius_db'
        server['tablename'] = sys.argv[5]#'newtest'
        if len(sys.argv)>=7:
            server['view'] = sys.argv[6]#'newtestview'
        else:
            server['view'] = server['tablename'] + "view"        
        start_web_server()