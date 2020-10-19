from multiprocessing import Process, Queue, cpu_count
import argparse
import time
import csv
import re
import os
from CBSA import CBSA

def process_line(cbsa, cols):
    cbsa09, cbsa_t, pop_00, pop_10 = cols[7], cols[8], cols[12], cols[14]
    if cbsa09:
        if not cbsa.has_cbsa09(cbsa09):
            cbsa.new_cbsa_t(cbsa09,cbsa_t)
        cbsa.add_tol_num_tract(cbsa09)
        cbsa.add_tol_pop_00(cbsa09, int(pop_00))
        cbsa.add_tol_pop_10(cbsa09, int(pop_10))

def process_lines(cbsa, lines):
    csvlines = csv.reader(lines)
    for cols in csvlines:
        process_line(cbsa, cols)

def worker_entry(q_out, filename, chunk_size, buf_size, index):
    print('Worker #{0} started, file size: {1}'.format(index, chunk_size))
    
    # Unfortunately we have to open the file as "rb" since it uses \r\n
    # as line terminator, which is a bit problematic with Python's len() func
    cbsa = CBSA()
    with open(filename, "rb") as f:
        buf_lines = []
        
        # Start reading my chunk, skip the first partial line
        # For the corner where we start right at the start of a line,
        # we still skip the line. Later the end condition makes sure we always
        # read one extra line, i.e. the starting line of the next chunk
        f.seek(chunk_size * index)
        cur_read_size = len(next(f))
        
        for l in f:
            # Keep track of bytes read
            cur_read_size += len(l)
            l = l.decode("utf-8").strip()
            
            # Parse CSV every buf_size (default, 1000) lines
            # Just to avoid the overhead of object creation,
            # Results in about 0.5s faster with 4 CPUs, compare to no line buf
            # However, this can be reduce to save some memory
            buf_lines.append(l)
            if len(buf_lines) >= buf_size:
                process_lines(cbsa, buf_lines)
                buf_lines.clear()
            
            # Check if we are done
            if chunk_size and cur_read_size > chunk_size:
                break
        
        # Finish up the remaining lines
        if len(buf_lines):
            process_lines(cbsa, buf_lines)
    
    # Send results back to main process
    q_out.put(cbsa)
    print('Worker #{0} done'.format(index))

def spawn_workers(filename, num_cpus, buf_size):
    """
    create processes and give a queue to each process
    """
    total_size = os.path.getsize(filename)
    if total_size < 1024 * 10 : num_cpus = 1
    split_size = total_size // num_cpus if num_cpus else 0
    
    proc_list = []
    for i in range(num_cpus):
        # Take care of the last chunk, which has to process to the end
        chunk_size = split_size
        if i == num_cpus - 1:
            chunk_size += total_size % num_cpus
        
        # Create and start a worker process
        q = Queue()
        p = Process(target=worker_entry, args=(q, filename, chunk_size, buf_size, i))
        p.start()
        proc_list.append((p, q))
    
    # Don't create worker process if num_cpus == 0
    # Still use a queue to transmit results
    if not len(proc_list):
        q = Queue()
        worker_entry(q, filename, split_size, buf_size, 0)
        proc_list.append((None, q))
    
    return proc_list

def aggregate(proc_list):
    cbsa = CBSA()
    
    # Loop through all workers to get their results from the queue
    for (proc, q_out) in proc_list:
        temp_cbsa = q_out.get(block=True)
        
        # Aggregate results
        cbsa.join(temp_cbsa)
        
        # Wait for the worker to finish, if there's a worker
        if proc:
            proc.join()
    
    print("Joined")
    return cbsa


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str,
                        default="input/censustract-00-10-aug.csv",
                        help="input file name")
    parser.add_argument("-o", "--output", type=str,
                        default="output/report2.csv",
                        help="input file name")
    parser.add_argument("-c", "--num-cpus", type=int, default=0,
                        help="number of cpus for parallel processing, 0: single process, >0: number of worker processes, <0: number of workers equal to number of CPUs")
    parser.add_argument("-b", "--buf-size", type=int, default=1000,
                        help="number of lines buffered before the CSV parsing, just a minor performance optimization")
    args = parser.parse_args()
    
    num_cpus = cpu_count() if args.num_cpus < 0 else args.num_cpus
    buf_size = args.buf_size
    
    start_time = time.time()
    
    # First create worker processes, they process data in parallel
    # And then wait for them finish and aggregate results
    # Finally calculate growth rate serially
    proc_list = spawn_workers(args.input, num_cpus, buf_size)
    cbsa = aggregate(proc_list)
    cbsa.calc_growth_rate()
    
    # Save final results
    cbsa.save_report(args.output)
    
    end_time = time.time()
    print('Time usage: {0}'.format(end_time - start_time))

