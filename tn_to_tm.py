


if __name__ == "__main__":
    n = 8
    s_str = "print("
    for i in range(1, n):
        t_str = "\"%.3f\" % " + "(" + str("t") + str(i+1) + "-" + str("t") + str(i) +")"
        s_str = s_str + t_str + ","

    s_str = s_str + ")"
    print(s_str)