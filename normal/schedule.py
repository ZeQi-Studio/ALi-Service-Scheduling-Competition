from expert import expert
from task import task_list
from queue import Queue

expert_path = r'C:\Users\10372\Desktop\服务调度大赛\process_time_matrix.csv'
task_path = r'C:\Users\10372\Desktop\服务调度大赛\work_order.csv'

"""
queue format :[task_id,expert_id,start_time,processing_time]
"""

class processing_task:
    def __init__(self):
        self.__time__ = 0
        self.__index__ = 0
        self.__task_list__ = task_list(task_path)
        self.__expert_list__ = expert(expert_path)
        self.running_queue = Queue(maxsize=0)
        self.waiting_queue = Queue(maxsize=0)

    def assignment(self):
        task = self.__task_list__.task_list[self.__index__]
        task_start_time = task[1]
        if task_start_time == self.__time__:
            task_problem_id = task[2]
            expert_processing_list = self.__expert_list__.sorted_expert_task_dict[task_problem_id]
            #寻找空闲的专家
            for i in range(len(expert_processing_list)):
                expert_id = expert_processing_list[i][0]
                #如果专家空闲，则把任务分配给该专家
                if self.__expert_list__.expert_processing_task_num[expert_id] > 0:
                    task_id = task[0]
                    start_time = self.__time__
                    processing_time = expert_processing_list[i][1]
                    self.running_queue.put([task_id,expert_id,start_time,processing_time])
                    self.__expert_list__.expert_processing_task_num[expert_processing_list[i][0]] -= 1
                    return 1
            #没有找到空闲专家，放入等待队列

            pass
        else:
            self.__time__ += 1
            return False
        pass

    def check_running(self):
        pass

    def check_waiting(self):
        pass

    def check_end(self):
        pass


    def processing(self):
        while (1):
            self.check_running()
            self.check_waiting()
            self.assignment()
            if self.check_end() == True:
                break

