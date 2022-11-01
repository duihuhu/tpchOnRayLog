import os

start_phase_time = 1667292687.202169
end_phase_time =  1667292689.4735315


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

    exec_task_callback_sign = "hucc time for exec task callback to lanaguage time"
    exec_task_args_sign = "hucc time for exec task args"
    push_task_sign = "hucc push normal task"

    sign = [get_object_mem_sign, get_object_plasma_sign, exec_task_callback_sign, exec_task_args_sign]
    datapath = "master"

    send_pull_object_sign = "hucc remote get object send pull request from"
    receive_handle_pull_object_sign = "hucc remote get object receive handle pull request from"
    receive_pull_reply_object_sign = "hucc remote get object receive pull reply"

    send_push_object_sign = "hucc remote get object send push request object id"
    receive_handle_push_object_sign = "hucc remote get object receive handle push request object id"
    receive_push_reply_object_sign = "hucc remote get object receive handle push reply object id"

    send_pull_object = {}
    receive_handle_pull_object = {}
    receive_pull_reply_object = {}
    send_push_object = {}
    receive_handle_push_object  = {}
    receive_push_reply_object = {}

    for f in os.listdir(datapath):
        if raylet_file_sign in f:
            with open(datapath + "/" + f, "r") as fd:
                for line in fd.readlines():
                    if send_pull_object_sign in line:
                        sign_sentence = line.split("\n")[0].split(" ")
                        send_pull_object[sign_sentence[-2]] = float(sign_sentence[-1]) / 1000000
                    elif receive_handle_pull_object_sign in line:
                        sign_sentence = line.split("\n")[0].split(" ")
                        receive_handle_pull_object[sign_sentence[-2]] = float(sign_sentence[-1]) / 1000000
                    elif receive_pull_reply_object_sign in line:
                        sign_sentence = line.split("\n")[0].split(" ")
                        receive_pull_reply_object[sign_sentence[-2]] = float(sign_sentence[-1]) / 1000000
                    elif send_push_object_sign in line:
                        sign_sentence = line.split("\n")[0].split(" ")
                        send_push_object[sign_sentence[-2]] = float(sign_sentence[-1]) / 1000000
                    elif receive_handle_push_object_sign in line:
                        sign_sentence = line.split("\n")[0].split(" ")
                        receive_handle_push_object[sign_sentence[-2]] = float(sign_sentence[-1]) / 1000000
                    elif receive_push_reply_object_sign in line:
                        sign_sentence = line.split("\n")[0].split(" ")
                        receive_push_reply_object[sign_sentence[-2]] = float(sign_sentence[-1]) / 1000000

    # print(len(send_pull_object))
    # print(len(receive_pull_reply_object))
    # print(len(receive_handle_push_object))

    muti_pull_reply_time = []
    muti_pull_handle_push_time = []
    for k in send_pull_object:
        pull_object_interval = []
        pull_push_object_interval = []
        v = send_pull_object[k]
        if v > start_phase_time:
            if receive_pull_reply_object.get(k):
                v1 = receive_pull_reply_object[k]
                if v1 < end_phase_time:
                    pull_object_interval.append(v)
                    pull_object_interval.append(receive_pull_reply_object[k])
                    if pull_object_interval:
                        muti_pull_reply_time.append(pull_object_interval)

            if receive_handle_push_object.get(k):
                v1 = receive_handle_push_object[k]
                if v1 < end_phase_time:
                    pull_push_object_interval.append(v)
                    pull_push_object_interval.append(receive_handle_push_object[k])
                    if pull_push_object_interval:
                        muti_pull_handle_push_time.append(pull_push_object_interval)

    pull_push_interval = []
    pull_push_interval.extend(muti_pull_reply_time)
    merge_time = merge(muti_pull_reply_time)
    sum_time = calculate_time(merge_time)
    print("muti_pull_reply_time", ": ", sum_time)

    pull_push_interval.extend(muti_pull_handle_push_time)
    merge_time = merge(muti_pull_handle_push_time)
    sum_time = calculate_time(merge_time)
    print("muti_pull_handle_push_time", ": ", sum_time)

    merge_time = merge(pull_push_interval)
    sum_time = calculate_time(merge_time)
    print("pull_push_interval", ": ", sum_time)
    # print(len(pull_push_interval))
    mem_plasma_time = []
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
        if sig == get_object_mem_sign:
            mem_plasma_time.extend(merge_time)
        if sig == get_object_plasma_sign:
            mem_plasma_time.extend(merge_time)
            pull_push_interval.extend(merge_time)

    merge_time = merge(pull_push_interval)
    mul_sum_time = calculate_time(merge_time)
    print("pull/push/local plasma", ": ", mul_sum_time)

    merge_time = merge(mem_plasma_time)
    mul_sum_time = calculate_time(merge_time)
    print("mem_local_plasma", ": ", mul_sum_time)

    pull_push_interval.extend(mem_plasma_time)
    merge_time = merge(pull_push_interval)
    mul_sum_time = calculate_time(merge_time)
    print("mem_local_pull/push_plasma", ": ", mul_sum_time)

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
        if v > start_phase_time:
            if push_task_end.get(k):
                v1 = push_task_end[k]
                if v1 < end_phase_time:
                    push_task_interval.append(v)
                    push_task_interval.append(push_task_end[k])
                    if push_task_interval:
                        muti_time.append(push_task_interval)
    merge_time = merge(muti_time)
    mul_sum_time = calculate_time(merge_time)
    print("push task time:", mul_sum_time)



    # muti_time = []
    # for f in os.listdir(datapath):
    #     if raylet_file_sign in f:
    #         with open(datapath + "/" + f, "r") as fd:
    #             for line in fd.readlines():
    #                 interval = []
    #                 if send_object_sign in line:
    #                     start_time = float(line.split("\n")[0].split(" ")[-3])
    #                     end_time = float(line.split("\n")[0].split(" ")[-1])
    #                     interval.append(start_time)
    #                     interval.append(end_time)
    #                 if interval:
    #                     muti_time.append(interval)
    # merge_time = merge(muti_time)
    # mul_sum_time = calculate_time(merge_time)
    # print("send object:", mul_sum_time)
