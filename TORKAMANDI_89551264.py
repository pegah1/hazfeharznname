#########################
## PEGAH TORKAMANDI #####
####### 89551264 ########
#########################

############# nahve run kardane proje ##############

# 1- ebteda tamamie mail haye ke mikhahid check shavad ba tag baz <BODY> v tag baste </BODY> dar file DOCUMENT.txt be surate seri copy karde
# 2- ba tavojo be inke tedad mail hayee ke mikhahd baresi shavad az tedad mail haye ke copy nemudeid bishtar nabashad!!!! 
# 3- be manzure amalkarde sari cod dar surati ke hajme file shamele mailha ziad bashad dar ebteda 2 tabe aval ke comment nemudeam ra run konid
# 4- bad az anjame marhale ghabl hal mitavan an 2 tabe ra comment karde v barname ra emtehan konid


StopWord="a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your"
# number_of_document-->[tedade tamame sanadha,(position start sanad,toole sanad)]
# size-unmerge-inverted-index-->tedad tamae file haye taghsimshode az invertedindex
# unmerge-inverted-index-->term shoro har unmerge file ra darad(size(term),term)
#farz bar in ast ke tamame mail ha dar yek file txt be name DOCUMENT gharar dadeshode ast ke har mail ba tag <BODY> baz shode v ba tag </BODY> baste shode ast.
list_stop=StopWord.split(",")
import easygui as eg
from easygui import *
import tokenize
import porter
import re
import math
import cmath
import heapq
import struct
import time
import os.path
import os
import sys
from heapq import*
from collections import OrderedDict
black_list=[]
def create_black_list():
    fr=None
    fr=open("balcklist_word.txt",'rb')
    s=[]
    for line in fr:
        line= re.findall(r"[\w']+",line)
        for i in range(0,len(line)):
            line[i]=line[i].lower()
            line[i] = porter.stem(line[i])
            if(line[i] in list_stop):
                continue
            black_list.append(line[i])
            
    s=list(OrderedDict.fromkeys(black_list))
    #print s
    return s
def unpack_list_of_unmerge_file():
    pm=[]
    with open('size-unmerge-inverted-index.dat','rb') as g:
        num=struct.unpack('i',g.read(4))[0]
        g.close()
    with open('unmerge-inverted-index.dat','rb') as f:
        for i in range(num):
            str_c=struct.unpack('i',f.read(4))[0]
            if str_c>0:
                _str=f.read(str_c)
                pm.append(_str)
            
    f.close()
    return pm
    
def read_start_unmerge_file(k):
    m=[]
    _str=str()
    with open('unmerge-inverted-index-'+str(k)+'.dat','rb') as f:
        for i in range(struct.unpack('i',f.read(4))[0]):
            str_c=struct.unpack('i',f.read(4))[0]
            if str_c>0:
                _str=f.read(str_c)
                break
    f.close()
    m.append((str_c,_str))
    return m
def write_start_unmergefile():
    tmp=str()
    m=[]
    with open('size-unmerge-inverted-index.dat','rb') as g:
        num=struct.unpack('i',g.read(4))[0]
        g.close()
    with open('unmerge-inverted-index.dat','wb') as p:
        for k in range(1,num+1):
            m=read_start_unmerge_file(k)
            p.write(struct.pack('i',m[0][0]))
            p.write((''.join(chr(j) for j in map(ord,m[0][1]))).encode('ascii'))
    p.close()
    
def write_file(oname,m,s,l):
    with open(oname, 'wb') as f:
        f.write(struct.pack('i',l))
        for i in range(s,l):
            f.write(struct.pack('i',len(m[i][0])))
            if len(m[i][0])>0:
                f.write((''.join(chr(j) for j in map(ord,m[i][0]))).encode('ascii'))
                f.write(struct.pack('i',len(m[i][1])))
                for j in range(len(m[i][1])):
                    f.write(struct.pack('i',m[i][1][j][0]))
                    f.write(struct.pack('i',m[i][1][j][1]))

def sort_data(m):
    m.sort()        
    for i in range(len(m)):
        j=1
        while( i+j < len(m) and m[i][0] == m[i+j][0]):
            m[i][1].extend(m[i+j][1])
            j=j+1
        m.__delslice__(i+1,i+j)
    return m

def merge_files(file1, file2, output):
    f1=open(file1,"rb")
    f2=open(file2,"rb")
    o=open(output,"wb")
    read_i = lambda f: struct.unpack('i',f.read(4))[0]
    read_s = lambda f: f.read(read_i(f))
    write_i = lambda f,x: f.write(struct.pack('i',x))
    def write_s(f,x):
        write_i(f,len(x))
        f.write((''.join(chr(j) for j in map(ord,x))).encode('ascii'))
    f1_s=read_i(f1)
    f2_s=read_i(f2)
    write_i(o,f1_s+f2_s)
    i=j=f1_v=f2_v=next_doc=0
    if f1_s > 0: f1_v=read_s(f1)
    if f2_s > 0: f2_v=read_s(f2)
    gen=0   
    while i<f1_s and j<f2_s:
        if f1_v=='':
            write_s(o,'')
            i=i+1
            if i<f1_s: f1_v=read_s(f1)
        elif f2_v=='':
            write_s(o,'')
            j=j+1
            if j<f2_s: f2_v=read_s(f2)
        elif f1_v<f2_v:
            c=read_i(f1)
            write_s(o,f1_v)
            write_i(o,c)
            for k in range(c):
                docid=read_i(f1)
                pos=read_i(f1)
                write_i(o,docid)
                write_i(o,pos)
            i=i+1
            if i<f1_s: f1_v=read_s(f1)
        elif f2_v<f1_v:
            c=read_i(f2)
            write_s(o,f2_v)
            write_i(o,c)
            for k in range(c):
                docid=read_i(f2)
                pos=read_i(f2)
                write_i(o,docid)
                write_i(o,pos)
            j=j+1
            if j<f2_s: f2_v=read_s(f2)
        else:
            c1=read_i(f1)
            c2=read_i(f2)
            c=c1+c2
            write_s(o,f1_v)
            write_i(o,c)
            for k in range(c1):
                docid=read_i(f1)
                pos=read_i(f1)
                write_i(o,docid)
                write_i(o,pos)
            for k in range(c2):
                docid=read_i(f2)
                pos=read_i(f2)
                write_i(o,docid)
                write_i(o,pos)
            i=i+1
            j=j+1
            if i<f1_s: f1_v=read_s(f1)
            if j<f2_s: f2_v=read_s(f2)
            gen=gen+1
    while i<f1_s:
        if f1_v=='':
            write_s(o,'')
            i=i+1
            if i<f1_s: f1_v=read_s(f1)
        else:
            c=read_i(f1)
            write_s(o,f1_v)
            write_i(o,c)
            for k in range(c):
                docid=read_i(f1)
                pos=read_i(f1)
                write_i(o,docid)
                write_i(o,pos)
            i=i+1
            if i<f1_s: f1_v=read_s(f1)
    while j<f2_s:
        if f2_v=='':
            write_s(o,'')
            j=j+1
            if j<f2_s: f2_v=read_s(f2)
        else:
            c=read_i(f2)
            write_s(o,f2_v)
            write_i(o,c)
            for k in range(c):
                docid=read_i(f2)
                pos=read_i(f2)
                write_i(o,docid)
                write_i(o,pos)
            j=j+1
            if j<f2_s: f2_v=read_s(f2)
    for k in range(gen): write_s(o,'')
    f1.close()
    f2.close()
    o.close()
def unmerged_files():
    mm=[]
    num=0
    with open('inverted-index.dat','rb') as f:
        for i in range(struct.unpack('i',f.read(4))[0]):
            str_c=struct.unpack('i',f.read(4))[0]
            if str_c>0:
                _str=f.read(str_c)
                _list=[]
                for j in range(struct.unpack('i',f.read(4))[0]):
                    a=struct.unpack('i',f.read(4))[0]
                    b=struct.unpack('i',f.read(4))[0]
                    _list.append( (a,b) )
                mm.append( (_str,_list) )
            if sys.getsizeof(mm)> ((334*334)/5) :
                num=num+1
                ffname='unmerge-inverted-index-'+str(num)+'.dat'
                print 'unmerge-file'+str(num)
                write_file(ffname,mm,0,len(mm))
                mm=[]
    num=num+1    
    ffname='unmerge-inverted-index-'+str(num)+'.dat'
    print 'unmerge-file'+str(num)
    write_file(ffname,mm,0,len(mm))
    with open('size-unmerge-inverted-index.dat','wb') as g:
        g.write(struct.pack('i',num))
    f.close()
    g.close()
def num_document(m):
    with open('number_of_document.dat','wb') as p:
        p.write(struct.pack('i',len(m)))
        for i in range(len(m)):
            p.write(struct.pack('i',m[i][0]))
            p.write(struct.pack('i',m[i][1]))
    p.close()
def main_dictionary():
    m=[]
    current_milli_time = lambda: int(round(time.time() * 1000))
    num_doc=[]
    doc_id=1
    b_start = 0
    b_end = 0
    tmp=str()
    total_temp_files=0
    #num_line=0
    last_tick=current_milli_time()
    print 'reading document file...'
    total_records=0
    
    fr = open('DOCUMENT.txt','r')
    ss=fr.tell()
    num_line=len(fr.readlines())
    fr.seek(ss)
    position=ss
    line=fr.readline()
    _num=0
    while(_num<num_line):
        if (b_start==0):
            start=line.find("<BODY>")
            if (start > -1):
                tmp = tmp + line[start+6:]
                pp=position
                b_start = 1
                
        if (b_start==1):
            end=line.find("</BODY>")
            if(end == -1):
                tmp = tmp + line
            if (end != -1):
                tmp = tmp + line[:end]
                b_start = 0
                b_end =1
            
        if(b_end == 1):
            tmp = re.findall(r"[\w']+",tmp)
            nd=0
            for i in range(0,len(tmp)):
                tmp[i]=tmp[i].lower()
                tmp[i] = porter.stem(tmp[i])
                if(tmp[i] in list_stop):
                    continue
                m.append((tmp[i],[(doc_id,i)]))
                nd+=1
            num_doc.append((pp,nd))
            b_end=0
            tmp=str()
            doc_id = doc_id + 1
        if sys.getsizeof(m)>4*1024*1024:
            total_temp_files=total_temp_files+1
            fname='partial-temp-'+str(total_temp_files)+'.dat'
            print 'saving temporary file ',fname,'...'
            m=sort_data(m)
            write_file(fname,m,int(0),len(m))
            total_records=total_records+len(m)
            m=[]
        position=fr.tell()
        line=fr.readline()
        _num+=1
    total_temp_files=total_temp_files+1
    fname='partial-temp-'+str(total_temp_files)+'.dat'
    print 'saving temporary file ',fname,'...'
    m=sort_data(m)
    write_file(fname,m,int(0),len(m))
    total_records=total_records+len(m)
    m=[]

    now_tick=current_milli_time()
    print 'read ',total_records,' records in ',(now_tick-last_tick),' ms.',' and saved ',total_temp_files,' temp files...'
    last_tick=now_tick

    if total_temp_files==1:
        os.rename('partial-temp-1.dat','inverted-index.dat')
    else:
        merge_files('partial-temp-1.dat','partial-temp-2.dat','merge-temp.dat')
        os.remove('partial-temp-1.dat')
        os.remove('partial-temp-2.dat')
        for i in range(3,total_temp_files+1):
            os.rename('merge-temp.dat','merge-temp-2.dat')
            print 'merging partial-temp-',str(i),'.dat...'
            merge_files('merge-temp-2.dat','partial-temp-'+str(i)+'.dat','merge-temp.dat')
            os.remove('merge-temp-2.dat')
            os.remove('partial-temp-'+str(i)+'.dat')
        os.rename('merge-temp.dat','inverted-index.dat')
        now_tick=current_milli_time()
        print 'merged inverted-index.dat in ',(now_tick-last_tick),' ms.'
        last_tick=now_tick
            
        
    print 'done!'

    unmerged_files()
    num_document(num_doc)
    print 'done'

    #mm=[]
    #with open('inverted-index.dat','rb') as f:
     #   for i in range(struct.unpack('i',f.read(4))[0]):
      #      str_c=struct.unpack('i',f.read(4))[0]
       #     if str_c>0:
        #        _str=f.read(str_c)
         #       _list=[]
          #      for j in range(struct.unpack('i',f.read(4))[0]):
           #         a=struct.unpack('i',f.read(4))[0]
            #        b=struct.unpack('i',f.read(4))[0]
             #       _list.append( (a,b) )
              #  mm.append( (_str,_list) )
               # print mm[i]

def binary_search(t,m):
    a=[]
    if(m[0][0]==t): return m[0][1]
    if(m[len(m)-1][0]==t): return m[len(m)-1][1]
    else:
        high=len(m)-1
        low=0
        while(high-low>1):
            mid=int(((high+low)/2))
            if(m[mid][0]==t):
                return m[mid][1]
            if(m[mid][0]>t): high=mid
            if(m[mid][0]<t): low=mid
        return [10000*10000]
def search_doc_id(t):
    m=[]
    mm=[]
    m=unpack_list_of_unmerge_file()
    for i in range(len(m)):
        if(t<m[i]):
            num_file=i
            break
    if (i==len(m)-1): num_file=len(m)
    with open('unmerge-inverted-index-'+str(num_file)+'.dat','rb') as f:
        a=[]
        _len=struct.unpack('i',f.read(4))[0]
        for i in range(_len):
            str_c=struct.unpack('i',f.read(4))[0]
            if str_c>0:
                _str=f.read(str_c)
                _list=[]
                for j in range(struct.unpack('i',f.read(4))[0]):
                    a=struct.unpack('i',f.read(4))[0]
                    b=struct.unpack('i',f.read(4))[0]
                    _list.append(a)
                mm.append( (_str,_list) )
    a = binary_search(t,mm)
    f.close()
    return a
def read_doc(position):
    _str=str()
    with open('DOCUMENT.txt','rb') as f:
        f.seek(position)
        line=f.readline()
        end=line.find("</BODY>")
        while(end==-1):
            _str = _str + line
            line=f.readline()
            end=line.find("</BODY>")
        _str = _str + line[:end]
    f.close()
    _string =[]   
    _str = re.findall(r"[\w']+",_str)
    #print len(_str)
    for i in range(0,len(_str)):
        _str[i]= _str[i].lower()
        _str[i] = porter.stem(_str[i])
        if(_str[i] in list_stop):
            continue
        _string.append(_str[i])       
    return _string
def TF_bm25(t,doc_id,m):
    _str=[]
    _str=read_doc(m[doc_id-1][0])
    j=0
    ld=0
    for i in range(len(_str)):
        if _str[i]==t: j+=1
    ld=ld+m[doc_id-1][1]
    lavg=0
    for i in range(len(m)):
        lavg+=m[i][1]
    lavg/=len(m)
    a=float(j)+(1.2*(0.34+(0.75*(ld/lavg))))
    b=j*2.2
    tf=b/a
    #print "tf25 done"
    return float(tf)
def rankBM25_DocumentAtATime_WithHeap(q,k):
    results=[]
    term=[]
    a=[]
    Nt=[]
    t=str()
    ld=0
    with open('number_of_document.dat','rb') as p:
        ld=struct.unpack('i',p.read(4))[0]
        for i in range(ld):
            start=struct.unpack('i',p.read(4))[0]
            lt=struct.unpack('i',p.read(4))[0]
            Nt.append((start,lt))
    p.close()
    for i in range(1,k+1):
        #result[1]-->doc_id
        #result[0]-->score
        results.append((0,0))
    for i in range(len(q)):
        a=search_doc_id(q[i])
        doc_id=a[0]
        term.append((doc_id,q[i],len(a),a))
    term.sort()
    doc_id=0
   # print term
    while (len(term)!=0 and term[0][0]<10000*10000):
        doc_id=term[0][0]
        b=[]
        while term[0][0]==doc_id:
            t=term[0][1]
            _p=math.log10(ld/term[0][2])
            _q=TF_bm25(t,doc_id,Nt)
            score=float(_p * _q)
            nt=term[0][2]
            b=term[0][3]
            b.remove(b[0])
            if(len(b)==0):
                b=[10000*10000]
            term[0]=(b[0],t,nt,b)
            heapify(term)
        if score>results[0][0]:
            results[0]=(score,doc_id)
            heapify(results)
    for i in range(len(results)):
        if results[0][0]==0: results.remove(results[0])
    results.sort()
    results.reverse()
    _result=(results,Nt)
    #print "rank25 done"
    return _result
def increase_score(q,doc,start):
    a=[]
    a=read_doc(start)
    check=False
    score=0
    for i in range(len(a)):
        if(a[i]==q[0] and len(a)-i>=len(q)):
            check=True
            k=i+1
            if (len(q)>1):
                for j in range(1,len(q)):
                    if(a[k]!=q[j]):
                        check=False
                        break
                    k+=1
        if(check==True):
            score+=1
            check=False
    #print "increase_score done"
    return score
        
def score_phrase(q,x):
    k=x
    result=[]
    doc_start=[]
    exe=rankBM25_DocumentAtATime_WithHeap(q,k)
    result=exe[0]
    doc_start=exe[1]
    for i in range(len(result)):
        j=increase_score(q,result[i][1],doc_start[(result[i][1])-1][0])
        a=result[i][1]
        b=result[i][0]
        result[i]=((j+b,a))
    result.sort()
    result.reverse()
    mm=(result,doc_start)
    return mm
def user(m):
    score=m[0]
    start=m[1]
    i=0
    if(len(score)==0):
        print "Not found any MAIL!!!!!"
        return
    with open("DOCUMENT.txt",'rb') as f:
        while(i<len(score)):
            print "DOC"+str(i+1)+":"
            print"---------------------------------------------------------------------------"
            f.seek(start[(score[i][1])-1][0])
            line=f.readline()
            p=line.find("<BODY>")
            q=line.find("</BODY>")
            if (p>-1 and q>-1): print line[p+6:end]
            if (p>-1 and q==-1): print line[p+6:]
            line=f.readline()
            q=line.find("</BODY>")
            if(q==-1): print line
            line=f.readline()
            q=line.find("</BODY>")
            if (q==-1): print line
            print"------------------------------------------------------------------------------"
            i+=1
        x=int(input("Which MAIL do you like show??"+'\n'+"if yo don't like please enter -1"+'\n'))
        if(x>len(score)): print "not fuond any MAIL"
        _string=str()
        while(x!=-1):
            if(x>len(score)):
                print "not fuond any MAIL"
                print "please enter valid number"
            if(x<=len(score)):
                f.seek(start[(score[x-1][1])-1][0])
                line=f.readline()
                s=line.find("<BODY>")
                end=line.find("</BODY>")
                if(s>-1 and end>-1):
                    _string+=str(line[s+6:end])
                else:
                    _string+=str(line[s+6:])
                    line=f.readline()
                    end=line.find("</BODY>")
                    while(end==-1):
                        _string+=str(line)
                        line=f.readline()
                        end=line.find("</BODY>")
                    _string+=str(line[:end])
                codebox(msg='GOOD LOCK', title='MAIL', text=_string)
                _string=str()
            print"--------------------------------------------------------------------------------------"
            x=int(input("Which MAIL do you like show??"+'\n'+"if yo don't like please enter -1"+'\n'))
            
    
    f.close()
    #print "userdone"
def input_query():
    _result=[]
    x=int(raw_input("pleas enter how many MAIL id you want to search?"+'\n'))
    qq=create_black_list()
    #print"--------------------------------"
    #print len(qq)
    _result=score_phrase(qq,x)               
    user(_result)
    
#main_dictionary()
#write_start_unmergefile()
#input_query()
