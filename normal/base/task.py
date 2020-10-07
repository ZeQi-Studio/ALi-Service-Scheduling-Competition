import numpy as np

task_path = r'C:\Users\10372\Desktop\服务调度大赛\work_order.csv'


class task_list:
    """
    task_list的第0列：任务编号
    task_list的第1列：任务到达的时间
    task_list的第3列：任务的种类
    task_list的第4列:任务的最大相应时间
    """

    def __init__(self, path):
        self.__path__ = path
        self.task_list = []
        self.__init_task__()

    def __init_task__(self):
        with open(self.__path__, encoding='utf-8') as f:
            self.task_list = np.loadtxt(f, delimiter=",").astype(int)
            # print(self.task_list[0][1])


if __name__ == '__main__':
    task_list(task_path)
