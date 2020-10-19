import csv
import re
from multiprocessing import Process, Queue, cpu_count
import time
import os

class CBSA:
    def __init__(self):
        self.cbsa_dict = {}
        
    def has_cbsa09(self, cbsa09):
        return cbsa09 in self.cbsa_dict

    def new_cbsa_t(self,cbsa09, cbsa_t):
        if self.has_cbsa09:
            self.cbsa_dict[cbsa09] = [cbsa_t, 0, 0, 0, 0.00]
    
    def get_cbsa_t(self, cbsa09):
        return self.cbsa_dict[cbsa09][0]
        
    def get_tol_num_tract(self, cbsa09):
        if self.has_cbsa09(cbsa09):
            return self.cbsa_dict[cbsa09][1]
    
    def get_tol_pop_00(self, cbsa09):
        if self.has_cbsa09(cbsa09):
            return self.cbsa_dict[cbsa09][2]
        
    def get_tol_pop_10(self, cbsa09):
        if not self.has_cbsa09(cbsa09): return
        return self.cbsa_dict[cbsa09][3]
        
    def add_tol_num_tract(self,cbsa09, n=1):
        self.cbsa_dict[cbsa09][1] += n

    def add_tol_pop_00(self,cbsa09,n):
        self.cbsa_dict[cbsa09][2] += n
        
    def add_tol_pop_10(self,cbsa09,n):
        self.cbsa_dict[cbsa09][3] += n
        
    def get_cbsa_dict(self):
        return self.cbsa_dict
        
    def join(self, other_cbsa):
        other_cbsa_dict = other_cbsa.get_cbsa_dict()
        for key in other_cbsa_dict.keys():
            if not self.has_cbsa09(key):
                self.new_cbsa_t(key,other_cbsa.get_cbsa_t(key))
            self.add_tol_num_tract(key,other_cbsa.get_tol_num_tract(key))
            self.add_tol_pop_00(key,other_cbsa.get_tol_pop_00(key))
            self.add_tol_pop_10(key,other_cbsa.get_tol_pop_10(key))
    def calc_growth_rate(self):
        for key, value in self.cbsa_dict.items():
            pop_change = float(value[3] - value[2])/ float(value[2])
            self.cbsa_dict[key][4] = round(pop_change,2)
                    

def read_line(data):
    
    cols = re.split(',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', data)
    return cols

def process_line(cbsa, cols):
    cbsa09, cbsa_t, pop_00, pop_10 = cols[7],cols[8],cols[12],cols[14]
    if not cbsa09: return 
    if not cbsa.has_cbsa09(cbsa09):
        cbsa.new_cbsa_t(cbsa09,cbsa_t)
    cbsa.add_tol_num_tract(cbsa09)
    cbsa.add_tol_pop_00(cbsa09,int(pop_00))
    cbsa.add_tol_pop_10(cbsa09,int(pop_10))

def calc_growth_rate(d):
    pass

def save_report(cbsa):
    cbsa_dict = cbsa.get_cbsa_dict()
    with open("output/report2.csv","w") as f:
        #writer = csv.writer(f)
        for key, value in cbsa_dict.items():
            print(value)
            f.write('{}\n'.format(','.join([key] +['"'+value[0]+'"'] + [str(v) for v in value[1:]])))
            #writer.writerow([key]+value)

def worker_entry(q_out, file_name, split_file_size, index):
    cbsa = CBSA()
    start = split_file_size * index
    end = start + split_file_size - 1
    print(split_file_size)
    lines = []
    with open(file_name,"r") as f:
        f.seek(start)
        f.readline()
        #reader = csv.reader(f)
        while(f.tell() <= end):
            line = f.readline()
            lines.append(line)
            #cols = list(csv.reader([line]))[0]
            #cols = next(reader)
#            l = f.readline()
#            lines.append(l)
            #lines.append(cols)
#            line = f.readline()
#            cols = read_line(line)
#            process_line(cbsa, cols)

    print("read done")
    csvlines = csv.reader(lines)
    for l in csvlines:
        cols = l
        process_line(cbsa, cols)
#    for l in lines:
#        cols = read_line(l)
#        process_line(cbsa, cols)

    print("qout")
    q_out.put(cbsa)
    print("work done")
    
def spawn_workers(file_name):
    """
    create processes and give a queue to each process
    """
    cpu = 1
    total_file_size = os.stat(file_name).st_size
    split_file_size = total_file_size // cpu
    q_out_list = [Queue() for _ in range(cpu)]
    proc_list = [Process(target = worker_entry, args = (q_out_list[index], file_name, split_file_size, index)) for index in range(cpu)]
    for proc in proc_list:
        proc.start()
    return proc_list, q_out_list
    
def split_data(file_name):
    size = os.stat(file_name).st_size
    split_list = []
    
def send_data(file_name,q_data_list):
    """
    send data to worker
    if all data are sent out, add a signal at the end
    """
    cpu = cpu_count()
    with open(file_name,"r") as f:
        next(f)
        n = 0
        for line in f:
            i = n % cpu
            q_data_list[i].put(line)
            n += 1
            i += 1
#            if n == 20:
#                break
    for q_data in q_data_list:
        q_data.put(1)
    print("data sent")
   
def aggregate(proc_list,q_out_list):
    print("start_merge")
    cbsa = CBSA()
    
    cpu = cpu_count()
    for q_out in q_out_list:
        temp_cbsa = q_out.get(block=True)
        cbsa.join(temp_cbsa)
#    while(not q_out.empty()):
#        temp_cbsa = q_out.get()
#        cbsa.join(temp_cbsa)
    for proc in proc_list:
        proc.join() 
    cbsa.calc_growth_rate()
    print("done")
    return cbsa
    
if __name__ == "__main__":
    start = time.time()
    file_name = "input/censustract-00-10.csv"
    proc_list, q_out_list = spawn_workers(file_name)
    cbsa = aggregate(proc_list, q_out_list)
    
    save_report(cbsa)
    cbsa = cbsa.get_cbsa_dict()
    print(cbsa[list(cbsa.keys())[0]])
    print(time.time() - start)
