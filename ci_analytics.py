from paramiko import SSHClient, AutoAddPolicy
from stat import S_ISDIR, S_ISREG
import xml.etree.ElementTree as ET
import psycopg


class GatherTestRunData:
    def __init__(self):
        self.result_server_hostname = "magna002.ceph.redhat.com"
        self.result_server_uname = "prprakas"
        self.result_server_conn = self._establish_results_server_conn()
        self.sftp = self.result_server_conn.open_sftp()
        self.test_run_results_path = "/ceph/cephci-jenkins/results"

        # DB details
        self.db_name = "postgres"
        self.db_user = "postgres"
        self.db_password = "admin"
        self.db_host = "10.0.209.12"
        self.db_port = "5432"
        self._initialise_db_conn()

    def gather_test_run_data(self):
        """
        Gathers the test run data
        Returns:

        """
        try:
            self._parse_dir(self.test_run_results_path)
        except Exception as e:
            print(f"Failed to get run data : {e}")
        finally:
            self.db_conn.close()

    def _parse_dir(self, path):
        dirs = []
        file = []
        for entry in self.sftp.listdir_attr(path):
            mode = entry.st_mode
            if S_ISDIR(mode):
                dirs.append(entry.filename)
            elif S_ISREG(mode):
                file.append(entry.filename)
        if "xunit.xml" in file:
            self._parse_xunit_details(path)
            return dirs, file
        for dir in list(dirs):
            sf, f = self._parse_dir(path + "/" + dir)
            dirs.extend(sf)
            file.extend(f)
        return dirs, file

    def _parse_xunit_details(self, path):
        test_data = self._get_supporting_data(path)
        file = self.sftp.open(path+"/xunit.xml")
        data = (file.readlines())
        data = "".join(data)

        parser = ET.XMLParser(encoding="utf-8")
        tree = ET.ElementTree(ET.fromstring(data, parser=parser))
        # get root element
        root = tree.getroot()
        for tc in root.findall("testsuite/testcase"):
            test_data["result"] = "passed"
            test_data["duration"], test_data["name"] = tc.attrib.get("time"), tc.attrib.get("name")
            for properties in tc:
                # Check for failed message
                if properties.tag == "failure":
                    test_data["result"] = "failed"
            # Inserting to DB
            self._insert_into_db(test_data)
            print(test_data)

    def _get_supporting_data(self, path):
        a = path.split("results")[-1].strip()
        fields = a.split("/")[1::]
        print(fields)
        test_data = {"env": fields[0], "build_type": fields[1], "ceph_version": fields[2], "rhel_version": fields[3],
                     "run_type": fields[4], "build_num": fields[5]}

        if test_data['run_type'] in ['Sanity', 'Upgrade', 'Stage', 'Interop']:
            test_data['fg'] = "NA"
        else:
            test_data['fg'] = fields[6]
        test_data['job_id'] = fields[-2]
        test_data['suite'] = fields[-1]
        return test_data

    def _get_dirs_in_path(self, path):
        dirs = []
        sftp = self.result_server_conn.open_sftp()
        for entry in sftp.listdir_attr(path):
            mode = entry.st_mode
            if S_ISDIR(mode):
                dirs.append(entry.filename)
        return dirs

    def _get_files_in_path(self, path):
        files = []
        sftp = self.result_server_conn.open_sftp()
        for entry in sftp.listdir_attr(path):
            mode = entry.st_mode
            if S_ISREG(mode):
                files.append(entry.filename)
        return files

    def _establish_results_server_conn(self):
        """
        Establishes ssh conn to results server
        Returns:
            conn (SSHClient): Connection object
        """
        try:
            conn = SSHClient()
            conn.set_missing_host_key_policy(AutoAddPolicy())
            conn.connect(hostname=self.result_server_hostname, username=self.result_server_uname)
        except Exception as e:
            raise ConnectionError(f"Failed to establish connection to {self.result_server_hostname}. Error : {e}")
        return conn

    def _initialise_db_conn (self):
        self.db_conn = psycopg.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )
        self.db_conn.autocommit = True

    def _insert_into_db (self, test_data):
        insert_query = f'''INSERT INTO cephci_analytics (env, build_type, ceph_version, rhel_version, run_type, build_num, fg, 
            job_id, suite, result, duration, name) VALUES ('{test_data['env']}', '{test_data['build_type']}', 
            '{test_data['ceph_version']}', '{test_data['rhel_version']}', '{test_data['run_type']}', 
            '{test_data['build_num']}', '{test_data['fg']}', '{test_data['job_id']}',
             '{test_data['suite']}', '{test_data['result']}', '{test_data['duration']}', 
             '{test_data['name'].replace("'", "")}')'''
        print(insert_query)
        self.db_conn.cursor().execute(insert_query)
        print("1 row inserted.")


obj = GatherTestRunData()
obj.gather_test_run_data()