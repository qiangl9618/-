
import os
import json

curdir=os.getcwd()

####生成本地json1,json2####
def get_files(path=curdir,rule='.MF4'):
    files=[]
   
    for fpath,dirs,fs in os.walk(path):
        for f in fs:
            
            filename=os.path.join(fpath,f)
            if rule in filename:
                files.append(filename)             
    
    return files

if __name__=='__main__':
    b=get_files()
 
    json1={}
    json1['FileList']=[]
     
    size=0
    for i in b:
        dirname=i.split('\\')[-3]
        p=(i,os.path.getsize(i))
        size+=os.path.getsize(i)
        mm=i.replace('\\','/')

        json1['Filelist'].append({'File':mm,'Size':os.path.getsize(i)})

    json1['Files_Num']=len(b)
    json1['Files_Size']=size
    json1_data=json.dumps(json1,indent=3)
    with open('filename.json','w') as f:
        f.write(json1_data)

        
