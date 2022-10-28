import os

complish_read_time = 1666760205.0933344

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

mem = []
plasma = []
exec_task = []
if __name__ == "__main__":
    core_worker_file_sign = "python-core"
    core_driver_file_sign = "python-core"
    get_object_mem_sign = "hucc time for get from memory"
    get_object_plasma_sign = "hucc time for get object from plasma"
    get_object_remote_plasma_sign = "hucc get remote plasma plasma"
    exec_task_sign = "hucc time for exec task time"
    exec_task_callback_sign = "hucc time for exec task callback to lanaguage time"
    exec_task_args_sign = "hucc time for exec task args"
    put_object_mem_sign = "hucc time for put from plasma"
    push_task_sign = "hucc push normal task"

    sign = [get_object_mem_sign, get_object_plasma_sign, exec_task_sign, exec_task_callback_sign, exec_task_args_sign, put_object_mem_sign, get_object_remote_plasma_sign]
    datapath = "/tmp/ray/session_latest/logs"

    muti_time = []
    for sig in sign:
        interval_time = []
        for f in os.listdir(datapath):
            if core_worker_file_sign in f:
                with open(datapath + "/" + f, "r") as fd:
                    for line in fd.readlines():
                        interval = process_line_sign(sig, line)
                        if interval:
                            interval_time.append(interval)

        merge_time = merge(interval_time)
        sum_time = calculate_time(merge_time)
        print(sig, ": ", sum_time)

        if sig == get_object_remote_plasma_sign:
            muti_time.extend(interval_time)
        if sig == get_object_plasma_sign:
            muti_time.extend(interval_time)
    merge_time = merge(muti_time)
    mul_sum_time = calculate_time(merge_time)
    print("mul_time", ": ", mul_sum_time)

    push_task_start = {}
    push_task_end = {}
    for f in os.listdir(datapath):
        if core_driver_file_sign in f:
            with open(datapath + "/" + f, "r") as fd:
                for line in fd.readlines():
                    if push_task_sign in line:
                        if 'start' in line:
                            sign_sentence = line.split("\n")[0].split(" ")
                            push_task_start[sign_sentence[-7]] = float(sign_sentence[-1])/1000000
                        if 'end' in line:
                            sign_sentence = line.split("\n")[0].split(" ")
                            push_task_end[sign_sentence[-7]] = float(sign_sentence[-1])/1000000
    muti_time = []
    for k in push_task_start:
        push_task_interval = []
        v = push_task_start[k]
        if v > complish_read_time:
            if push_task_end.get(k):
                push_task_interval.append(v)
                push_task_interval.append(push_task_end[k])
                if push_task_interval:
                    muti_time.append(push_task_interval)
    merge_time = merge(muti_time)
    mul_sum_time = calculate_time(merge_time)
    print("push task time:", mul_sum_time)