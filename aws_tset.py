import datetime,time
import boto3
import os
import threading
import sys
import json
import configparser
from multiprocessing.dummy import Pool as ThreadPool
from PyQt5 import QtWidgets,QtGui
from PyQt5.QtWidgets import QDialog, QInputDialog, QMessageBox
from show import Ui_widget
from PyQt5.QtGui import *
import csv
class Myqt(QtWidgets.QWidget,Ui_widget):
    def __init__(self):
        super(Myqt,self).__init__()
        self.setupUi(self)
        self.pushButton.clicked.connect(self.s3_list)
        self.pushButton_2.clicked.connect(self.s3_search)
        self.pushButton_3.clicked.connect(self.s3_upload)
        self.pushButton_7.clicked.connect(self.s3_del)
        self.pushButton_4.clicked.connect(self.snowball_list)

        self.pushButton_8.clicked.connect(self.snow_ball)


        self.pushButton_5.clicked.connect(self.ec2_search)
##try:
##    s3=boto3.client('s3',region_name='cn-northwest-1')
##
##    location={'LocationConstraint':'cn-northwest-1'}
##    ###创建存储桶，需要指定区域，桶名要唯一
##    s3.create_bucket(Bucket='apmms-magana-dataa',CreateBucketConfiguration=location)
##    
##except ClientError as e:
##    print(e)

##访问存储桶，获取存储桶名
#response=s3.list_buckets()

#for bucket in response['Buckets']:
#    print(bucket['Name'])

##获取snowball作业状态
    def snowball_list(self):
        cmd ='aws snowball list-jobs'
        data=os.popen(cmd)
        m = data.read()
        print(m)
        self.plainTextEdit.appendPlainText('Snowball_Job列表：')
        self.plainTextEdit.appendPlainText(m)


    def snow_ball(self):
        text, ok = QInputDialog.getText(self, 'Snowball作业查询', '输入作业名称：')
        if ok:
            cmd = 'aws snowball list-jobs'
            job = os.popen(cmd, 'r')
            jobs = json.loads(job.read())
            statu = jobs['JobListEntries']
            print(jobs)
            for i in range(0, len(statu)):

                if statu[i]['Description'] == str(text):
                    print(statu[i]['JobState'], statu[i]['Description'])
                    self.plainTextEdit.appendPlainText('JobName:' +statu[i]['Description'] )
                    self.plainTextEdit.appendPlainText('JobState:' +statu[i]['JobState'])
                    if statu[i]['JobState'] == 'Complete':
                        self.plainTextEdit.appendPlainText('已完成上传')
                        print('已完成上传')
                        return True
                    else:
                        self.plainTextEdit.appendPlainText('上传还未完成')
                        print('上传还未完成')
                        # time.sleep(3600)
    def s3_list(self):

        cmd='aws s3 ls'
        data=os.popen(cmd)
        m=data.read()
        print(m)
        self.plainTextEdit.appendPlainText('存储桶列表：')
        self.plainTextEdit.appendPlainText(m)
    def s3_search(self):
        text, ok = QInputDialog.getText(self, '数据查询', '输入要查询的S3 URL：')
        if ok:
            cmd ='aws s3 ls '+str(text)
            data = os.popen(cmd)
            m = data.read()
            print(m)
            self.plainTextEdit.appendPlainText('查询数据如下：')
            self.plainTextEdit.appendPlainText(m)
    def s3_upload(self):
        text, ok = QInputDialog.getText(self, '数据上传', '输入所上传数据的地址：')
        if ok:
            text1, ok = QInputDialog.getText(self, '数据上传', '输入上传位置地址：')
            if ok:
                cmd = 'aws s3 cp ' + str(text)+ ' ' +str(text1)
                print(cmd)
                data = os.popen(cmd)
                m = data.read()
                print(m)

                self.plainTextEdit.appendPlainText(m)
                self.plainTextEdit.appendPlainText('上传完成！')

    def s3_del(self):
        text, ok = QInputDialog.getText(self, '数据删除', '输入所要删除数据的地址：')
        if ok:
            cmd = 'aws s3 rm ' + str(text)
            print(cmd)
            data = os.popen(cmd)
            m = data.read()
            print(m)
            self.plainTextEdit.appendPlainText(m)
            self.plainTextEdit.appendPlainText('删除完成！')

#启动，运行ec2实例
    def ec2_search(self):
        ec22=boto3.resource('ec2')
        instance=ec22.Instance('i-00c749cb5ee919cee')
        start_=instance.start()
        time.sleep(1)
        self.state = instance.state
        self.public=instance.public_dns_name
        self.plainTextEdit.appendPlainText('当前实例: '+'i-00c749cb5ee919cee')
        self.plainTextEdit.appendPlainText('实例状态: '+self.state['Name'])
        self.plainTextEdit.appendPlainText('公有DNS: '+self.public)
        print(self.public, self.state)
        text, ok = QInputDialog.getText(self, '数据查询', '输入客户桶S3 URL：')
        if ok:
            with open('command1.txt','w') as f:
                f.write('aws s3 ls --profile ME '+str(text))
            if self.state['Name'] == 'running':
                cmd1 = r'putty -i D:\PuTTY\hxy.ppk -l ec2-user -m E:\AWS云\command1.txt ' + self.public

                data1 = os.popen(cmd1, 'r')
                print(type(data1))

                m = data1.read()

                self.plainTextEdit.appendPlainText('查询数据如下：')
                self.plainTextEdit.appendPlainText(m)

    def con_instance(self):
        pass
            # with open('E:\mipifile.txt','r')as f:
            #     print(f.read())
                # self.plainTextEdit_3.appendPlainText(f.read())
##从S3存储桶数据同步到ME桶
    def run_sync(self,m):
        # p=self.run_instance()
        # if p['Name']=='running':
        #     # with os.popen(r'putty -i ')+ppk_dir+' -l ec2-user -m '+sync_command+' '+public,'r') as f:
        #     with os.popen(r'putty -i D:\PuTTY\hxy.ppk -l ec2-user -m E:\command.txt ' + self.public, 'r') as f:
        #         text = f.read()
        #         print(text)
        print(m)
        # res=os.popen(m,'r')
        # print(res.read())

    def move_data(self,bucket1,bucket2):
        cmdlist=[]
        transname = os.listdir(self.curdir)
        for dd in range(0, (len(transname))):
            pathd = os.path.join(self.curdir, transname[dd])
            if os.path.isdir(pathd) and not '.idea' in pathd:

                cmd='aws s3 sync s3://'+bucket1+'/'+transname[dd]+"/"+' s3://'+bucket2+'/'+transname[dd]+"/"
                cmdlist.append(cmd)
                print(cmd)
        pool=ThreadPool(len(cmdlist))
        pool.map(self.run_sync,cmdlist)
        pool.close()
        pool.join()

##访问ME存储桶数据####
    def get_bucket_data(self,bucket):

        s33=boto3.resource('s3')
        obj=s33.Bucket(bucket)

        alls=obj.objects.filter()
        print("=====")
        print(alls)

        list8=[]
        size=0
        json2 = {}
        json2['SensorList'] = []
        json2['FileDuration'] = '10sec'
        json2['SessionSizeGB'] = '28.8'
        json2['SessionDurationMinutes'] = '1.8'
        json2['Recording_type'] = 'DataCollection'
        json2['Path'] = 'SC_data_collection/tfl-mipi/20210114/'
        json2['FileList'] = []
        for i in alls:
            json2['FileList'].append(i.key)

            # p=(i.key,round(i.size/1024,1))
            p = (i.key, i.size)
            size += i.size
            list8.append(p)
            aws_data = dict(list8)

            print('总文件数为%d,总数据量为%sKB' % (len(list8), round(size / 1024, 1)))
            print(aws_data)
            return len(list8), size, aws_data  ##返回总文件数，总大小，数据文件
        json2_data = json.dumps(json2, indent=3, separators=(',', ':'))

        with open(datetime.datetime.strftime(datetime.datetime.now(),'%Y_%m_%d_%H_%M_%S_')+'Upload_Report.json', 'w') as f:
            f.write(json2_data)


####删除存储桶数据

    def del_data(self,a):
        s33 = boto3.resource('s3')
        del1 = s33.Object('apttv', a)
        del1.delete()

####对比桶内文件和本地json2文件

    def check_file(self):
        #num,size0,bucketfile=self.get_bucket_data(self)

        #测试用数据
        test_data={'F:\\Test\\20201025_0003\\ADCAM_WBACR6103LLJ30939_20201025_065922_tap_0087.MF4':22510416,'F:\\Test\\20201025_0003\\TAPI_REGULAR\\ADCAM_WBACR6103LLJ30939_20201025_065922_tap_0088.MF4':22521832}
     

        with open(self.curdir+'\\'+'filename.json','r')as f:
            datas=json.load(f)
            selec_data=datas['Filelist']
            numer=datas['Files_Num']     ###本地总文件数，总size
            sizer=datas['Files_Size']


        ##    for i in range(0,len(selec_data)):
        ##        #print(selec_data[i]['File'])
        ##        list1.append(selec_data[i]['File'])
        ##        list2.append(selec_data[i]['Size'])
        ##    print(list2)
        ##        
            #
            # if num==numer and size0==sizer:
            #     print('总文件数和总文件大小对比一致：OK')

            json_com={}
            json_com['CompareResult']=[]
            for j in range(0,len(selec_data)):
                local_file=os.path.split(selec_data[j]['File'])[-1]
                local_size=selec_data[j]['Size']
                
                print(local_file,local_size)
                list1=[]
                list2=[]
                for k,v in test_data.items():
                #for k,v in bucketfile.items():
                    pro_file1=os.path.split(k)[-1]
                    pro_size1=v
                    list1.append(pro_file1)
                    list2.append(pro_size1)

                    
                    if local_file in pro_file1 and local_size==pro_size1:
                    
                        print('对比成功')
                        json_com['CompareResult'].append({'File':selec_data[j]['File'],'Status':'OK'})
                       ####对比成功，删除该文件
                        self.del_data(k)
                print(list1)
                        
                if not local_file in list1:
                    print('对比失败')
                    json_com['CompareResult'].append({'File':selec_data[j]['File'],'Status':'NG'})
                    
            json_com_data=json.dumps(json_com,indent=3,separators=(',',':'))
                        
                        
            with open(self.curdir+'\\'+'比对结果.json','w')as f:
                f.write(json_com_data)
###生成文件json,推送到ME桶内
    def load_json(self,mebucket):
        datas = os.listdir(self.curdir)

        for j in range(0, len(datas)):
            dir1 = os.path.join(self.curdir, datas[j])  ###日期文件夹

            if os.path.isdir(dir1) and not '.idea' in dir1:
                data1 = os.listdir(dir1)
                json_name = dir1.split('\\')[-1]
                json2 = {}
                json2['SensorList'] = []
                json2['FileDuration'] = '10sec'
                json2['SessionSizeGB'] = '28.8'
                json2['SessionDurationMinutes'] = '1.8'
                json2['Recording_type'] = 'DataCollection'
                dir1 = dir1.replace('\\', '/')
                json2['Path'] = dir1
                json2['FileList'] = []

                for k in range(0, len(data1)):
                    if os.path.isdir(dir1) and not '.idea' in dir1:
                        dir2 = os.path.join(dir1, data1[k])
                        if os.path.isdir(dir2):

                            data2 = os.listdir(dir2)
                            # print(data2)
                            for s in data2:
                                s1 = data1[k] + '\\' + s
                                s1 = s1.replace('\\', '/')

                                json2['Filelist'].append(s1)
                json2_data = json.dumps(json2, indent=3, separators=(',', ':'))
                os.chdir(dir1)
                filedir = datas[j] + '.json'
                with open(filedir, 'w') as f:
                    f.write(json2_data)
                s3 = boto3.client('s3', region_name='cn-northwest-1')
                response = s3.upload_file(filedir, mebucket, filedir.split('\\')[-1])
                print(filedir+' :已上传完毕')

        print('json推送完毕！！')
        input('按回车退出！')


if __name__=='__main__':

    app=QtWidgets.QApplication(sys.argv)
    myqt=Myqt()
    myqt.show()
    sys.exit(app.exec_())


    # #aws_run.snow_ball(job_name)
    # if aws_run.snow_ball(job_name):     ##监控snowball状态
    #     print('snowball上传已完成')
    #     print('-----')
    #
    #     aws_run.move_data(bucket,me_bucket) #数据同步
    #     aws_run.get_bucket_data(me_bucket)
    #     aws_run.check_file()
    #     aws_run.load_json(me_bucket)


    

##回调参数，返回进度，文件大小
##class ProgressPercentage(object):
##
##    def __init__(self, filename):
##        self._filename = filename
##        self._size = float(os.path.getsize(filename))
##        self._seen_so_far = 0
##        self._lock = threading.Lock()
##
##    def __call__(self, bytes_amount):
##  
##        with self._lock:
##            self._seen_so_far += bytes_amount
##            percentage = (self._seen_so_far / self._size) * 100
##            sys.stdout.write(
##                "\r%s  %s / %s  (%.2f%%)" % (
##                    self._filename, self._seen_so_far, self._size,
##                    percentage))
##            sys.stdout.flush()

###上传，下载数据（upload_file,download_file）
##up_dir=r'E:\Aroad\ES'
##
##    
##files=r'E:\Aroad\ES\20200528_ES\Label_XmlInfo(Deal).xlsx'
##try:
##    download1=s3.upload_file(files,'apmms-magana-dataa',files,Callback=ProgressPercentage(files))
##except ClientError as e:
##    print(e)

   
# ec22=boto3.resource('ec2')
# inst=ec22.Instance('i-00c749cb5ee919cee')
#start_=inst.start()
# start_.public_dns_name

    
