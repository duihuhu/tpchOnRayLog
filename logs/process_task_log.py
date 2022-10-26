import os

complish_read_time = 1666745533.3318584

def process_line_sign(sign, data):
    data_tmp = []
    if sign in line:
        get_object_mem_time = line.split("\n")[0].split(" ")
        start_time, end_time =  float(get_object_mem_time[-1]) / 1000000, float(get_object_mem_time[-2][:-1]) / 1000000
        if start_time > complish_read_time:
            data_tmp.append(start_time)
            data_tmp.append(end_time)
    return data_tmp

def merge(intervals):
    """
    :type intervals: List[Interval]
    :rtype: List[Interval]
    """
    intervals_sorted = sorted(intervals, key=lambda x : x[0])
    result = []
    for interval in intervals_sorted:
        # result中最后一个区间的右值>=新区间的左值，说明两个区间有重叠
        if result and result[-1][1] >= interval[0]:
            # 将result中最后一个区间更新为合并之后的新区间
            result[-1][1] = max(result[-1][1], interval[1])
        else:
            result.append(interval)
    return result

def calculate_time(intervals):
    sum_time = 0
    for inter in intervals:
        sum_time = sum_time + inter[1] - inter[0]
    return sum_time
if __name__ == "__main__":
    core_worker_file_sign = "python-core-worker"
    get_object_mem_sign = "hucc time for get from memory"
    get_object_plasma_sign = "hucc time for get object from plasma"
    exec_task_sign = "hucc time for exec task time"
    datapath = "../logs"

    mem_interval_time = []
    plasma_interval_time = []
    exec_task_interval_time = []
    for file in os.listdir(datapath):
        if core_worker_file_sign in file:
            with open(file, "r") as fd:
                for line in fd.readlines():
                    mem_interval = process_line_sign(get_object_mem_sign, line)
                    if mem_interval:
                        mem_interval_time.append(mem_interval)
                    plasma_interval = process_line_sign(get_object_plasma_sign, line)
                    if plasma_interval:
                        plasma_interval_time.append(plasma_interval)
                    task_interval = process_line_sign(exec_task_sign, line)
                    if task_interval:
                        exec_task_interval_time.append(task_interval)

    mem_time = merge(mem_interval_time)
    plasma_time = merge(plasma_interval_time)
    task_time = merge(exec_task_interval_time)
    # print(mem_time)
    print(calculate_time(mem_time))
    print(calculate_time(plasma_time))
    print(calculate_time(task_time))

