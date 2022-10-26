
read_time_sign = "hucc Reading time"
sql_exec_time_sign = "Execution time"
core_worker_sign = "hucc core_worker get objects"

def process_log(filename):
    complish_read_time = -1
    sql_execute_time = -1
    with open(filename, 'r') as fd:
        for line in fd.readlines():
            if read_time_sign in line:
                complish_read_time = float(line.split("\n")[0].split(" ")[-1])
            if sql_exec_time_sign in line:
                sql_execute_time = float(line.split("\n")[0].split(" ")[-1])
    interval_time = []
    with open(filename, 'r') as fd:
        for line in fd.readlines():
            if core_worker_sign in line:
                interval_tmp = []
                get_object_time = line.split("\n")[0].split(" ")
                start_time, end_time = float(get_object_time[-2][:-1]), float(get_object_time[-1])
                if start_time > complish_read_time:
                    interval_tmp.append(start_time)
                    interval_tmp.append(end_time)
                    interval_time.append(interval_tmp)
    interval = merge(interval_time)
    sum_get_object_time = 0
    for inter in interval:
        sum_get_object_time = sum_get_object_time + inter[1] - inter[0]
    print(sql_execute_time, sum_get_object_time)

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


if __name__ == "__main__":
    filename = "log3.txt"
    process_log(filename)