# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import matplotlib.pyplot as plt
import numpy as np
import  warnings
warnings.filterwarnings('ignore')
def get_lats(filename):
    lats = []
    with open(filename, 'r') as fd:
        for line in fd.readlines():
            sqlname, lat = line.split(":")
            lats.append(float(lat[:-1]))
    return lats

def delete_max_data_n(data, n):
    count = 0
    while count < n:
        index = data.index(max(data))
        del data[index]
        count = count + 1
    return data

def delete_min_data_n(data, n):
    count = 0
    while count < n:
        index = data.index(min(data))
        del data[index]
        count = count + 1
    return data


def plot_sql_one_box_one_plot(single_lats, cluster_lats, sqlname, xlen):
    box = {"linestyle":'--',"linewidth":3,"color":'blue'}
    mean = {"marker":'o','markerfacecolor':'pink','markersize':12}

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 9))
    # make a little extra space between the subplots
    # fig.subplots_adjust(left=0.5, bottom=1, right=1.8, top=1.6, wspace=0.2 , hspace=1.2)
    fig.suptitle(sqlname, size=20)

    ax1.plot(range(1, xlen+1), single_lats, label='single')
    ax1.plot(range(1, xlen+1), cluster_lats, label='cluster')
    ax1.set_xlabel('iteration times', size=20,)
    ax1.set_ylabel('lats', size=20)
    ax1.tick_params(axis="x", labelsize=20)
    ax1.tick_params(axis="y", labelsize=20)
    ax1.legend()
    ax1.grid()

    lats = []
    lats.append(single_lats)
    lats.append(cluster_lats)
    ax2.boxplot(lats, meanline=True, showmeans=True, labels=["single", "cluster"], boxprops=box, meanprops=mean)
    ax2.tick_params(axis="x", labelsize=20)
    ax2.tick_params(axis="y", labelsize=20)
    ax2.grid(True, linestyle="-.")
    ax2.legend()
    plt.tight_layout()
    path = "figure3/" + sqlname + '.png'
    plt.savefig(path)
    plt.show()

def plot_box(data):
    labels = ["single", "cluster"]
    i = 1
    for dt in data:
        plt.boxplot(dt, labels=labels, positions=(i,i+0.2))

        i = i + 0.4
    x_position = [1,1.4,1.8]
    x_position_fmt = ["q01", "q02" ,"q03"]
    plt.xticks([i  for i in x_position], x_position_fmt)
    plt.show()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    c_path = "tpch-data3/cluster/"
    s_path = "tpch-data3/single/"
    data = []
    for i in range(1,23):
        if i==19:
            continue
        t_data = []
        sqlname = "q" + str(i)
        filename = sqlname + "_runtime.txt"
        single_lats = get_lats(s_path+filename)
        cluster_lats = get_lats(c_path+filename)
        single_lats = delete_max_data_n(single_lats, 2)
        single_lats = delete_min_data_n(single_lats, 2)
        cluster_lats = delete_max_data_n(cluster_lats, 2)
        cluster_lats = delete_min_data_n(cluster_lats, 2)
        print("%.3f" % np.mean(single_lats), "%.3f" %  np.mean(cluster_lats), "%.3f" % np.median(single_lats), "%.3f" % np.median(cluster_lats))
        # print("%.3f" %((np.mean(cluster_lats) - np.mean(single_lats))/np.mean(single_lats)))
        # length = min(len(cluster_lats), len(single_lats))
        # plot_sql_one_box_one_plot(single_lats[:length], cluster_lats[:length], sqlname, length)
        t_data.append(single_lats)
        t_data.append(cluster_lats)
        data.append(t_data)
