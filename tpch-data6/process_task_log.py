import os

start_phase_time = 1667726922.6232402
end_phase_time = 1867707200.625101

# start_phase_time = 1667728564.9229202
# end_phase_time = 1867707200.625101

def process_line_sign(sign, data):
    data_tmp = []
    if sign in line:
        get_object_mem_time = line.split("\n")[0].split(" ")
        start_time, end_time =  float(get_object_mem_time[-1]) / 1000000, float(get_object_mem_time[-2][:-1]) / 1000000
        if start_time > start_phase_time and end_time < end_phase_time:
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
    raylet_file_sign = "raylet.out"
    get_object_mem_sign = "hucc time for get from memory"
    get_object_plasma_sign = "hucc time for get object from plasma"
    # get_object_remote_plasma_sign = "hucc get remote plasma plasma"
    # exec_task_sign = "hucc time for exec task time"
    exec_task_callback_sign = "hucc time for exec task callback to lanaguage time"
    exec_task_args_sign = "hucc time for exec task args"
    # put_object_mem_sign = "hucc time for put from plasma"
    push_task_sign = "hucc push normal task"

    push_rpc_task_sign = "hucc task rpc push normal task"
    push_rpc_handle_task_sign = "hucc task rpc handle push normal task"
    push_rpc_callback_task_sign = "hucc task callback rpc push normal task"
    push_rpc_handle_callback_task_sign = "hucc task callback rpc handle push normal task"


    datapath = "logs"

    push_rpc_task_start = {}
    push_rpc_task_end = {}
    push_rpc_callback_start = {}
    push_rpc_callback_end = {}
    for f in os.listdir(datapath):
        if core_driver_file_sign in f:
            with open(datapath + "/" + f, "r") as fd:
                for line in fd.readlines():
                    if push_rpc_task_sign in line:
                        sign_sentence = line.split("\n")[0].split(" ")
                        push_rpc_task_start[sign_sentence[-2]] = float(sign_sentence[-1])/1000000
                    if push_rpc_handle_task_sign in line:
                        sign_sentence = line.split("\n")[0].split(" ")
                        push_rpc_task_end[sign_sentence[-2]] = float(sign_sentence[-1])/1000000
                    if push_rpc_callback_task_sign in line:
                        sign_sentence = line.split("\n")[0].split(" ")
                        push_rpc_callback_start[sign_sentence[-2]] = float(sign_sentence[-1])/1000000
                    if push_rpc_handle_callback_task_sign in line:
                        sign_sentence = line.split("\n")[0].split(" ")
                        push_rpc_callback_end[sign_sentence[-2]] = float(sign_sentence[-1])/1000000

    merge_pups_time = []
    muti_time = []
    for k in push_rpc_task_start:
        push_task_interval = []
        v = push_rpc_task_start[k]
        if v > start_phase_time:
            if push_rpc_task_end.get(k):
                v1 = push_rpc_task_end[k]
                if v1 < end_phase_time:
                    # print(v, v1)
                    push_task_interval.append(v)
                    push_task_interval.append(v1)
                    if push_task_interval:
                        muti_time.append(push_task_interval)
    merge_time = merge(muti_time)
    mul_sum_time = calculate_time(merge_time)
    print("push task time:", mul_sum_time)
    # merge_pups_time.extend(merge_time)

    muti_time = []
    for k in push_rpc_callback_start:
        push_task_interval = []
        v = push_rpc_callback_start[k]
        if v > start_phase_time:
            if push_rpc_callback_end.get(k):
                v1 = push_rpc_callback_end[k]
                if v1 < end_phase_time:
                    # print(v, v1)
                    push_task_interval.append(v)
                    push_task_interval.append(v1)
                    if push_task_interval:
                        muti_time.append(push_task_interval)

    merge_time = merge(muti_time)
    mul_sum_time = calculate_time(merge_time)
    print("push task callback time:", mul_sum_time)
    # merge_pups_time.extend(merge_time)
    # mul_sum_time = calculate_time(merge_pups_time)
    # print("push task total callback time:", mul_sum_time)