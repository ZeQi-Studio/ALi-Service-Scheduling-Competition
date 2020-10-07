from base.expert import expert
from base.task import task_list

expert_path = '../../data/process_time_matrix.csv'
task_path = '../../data/work_order.csv'

"""
should not use queue,but use list
running_queue format : running_queue[expert_id]:[task_id,start_time,task_processing_time,task_ending_time]
waiting_queue format : waiting_queue[task_problem_id]:[task_id,start_time,task_ending_time]
assignment_result format:assignment_result:[task_id,start_time,expert_id]
"""


class processing_task:
    def __init__(self):
        self.__time__ = 0
        self.__task_index__ = 0
        self.__task_list__ = task_list(task_path)
        self.__expert_list__ = expert(expert_path)
        self.running_queue = {}
        self.waiting_queue = {}
        self.assignment_result = []
        self.task_num = len(self.__task_list__.task_list)

        self.__init_waiting_queue__()
        self.__init_running_queue__()
        self.processing()

    def __init_waiting_queue__(self):
        for i in range(1, self.__expert_list__.question_num + 1):
            self.waiting_queue[i] = []

    def __init_running_queue__(self):
        for i in range(1, self.__expert_list__.expert_num + 1):
            self.running_queue[i] = []

    def __instert_into_running_queue__(self, expert_id, new_running_data):
        if len(self.running_queue[expert_id]) == 0:
            self.running_queue[expert_id].append(new_running_data)
        else:
            # 笨方法
            new_ending_time = new_running_data[2]
            for i in range(len(self.running_queue[expert_id])):
                ending_time = self.running_queue[expert_id][i][1] + self.running_queue[expert_id][i][2] - self.__time__
                if ending_time > new_ending_time:
                    self.running_queue[expert_id].insert(i, new_running_data)
                    break
                elif i == len(self.running_queue[expert_id]) - 1:
                    self.running_queue[expert_id].append(new_running_data)

    # 根据任务的紧急程度排序插入
    def __instert_into_waiting_queue__(self, task_problem_id, new_waiting_data):
        if len(self.waiting_queue[task_problem_id]) == 0:
            self.waiting_queue[task_problem_id].append(new_waiting_data)
        else:
            # 笨方法
            new_ending_time = new_waiting_data[2]
            for i in range(len(self.waiting_queue[task_problem_id])):
                ending_time = self.waiting_queue[task_problem_id][i][1] + self.waiting_queue[task_problem_id][i][
                    2] - self.__time__
                if ending_time > new_ending_time:
                    self.waiting_queue[task_problem_id].insert(i, new_waiting_data)
                    break
                elif i == len(self.waiting_queue[task_problem_id]) - 1:
                    self.waiting_queue[task_problem_id].append(new_waiting_data)

    def assignment(self):
        # 在同一个时间可能有多个任务达到，故采用while循环
        while self.__task_list__.task_list[self.__task_index__][1] == self.__time__:
            arrive_task = self.__task_list__.task_list[self.__task_index__]
            arrive_task_id = arrive_task[0]
            arrive_task_start_time = arrive_task[1]
            arrive_task_problem_id = arrive_task[2]
            arrive_task_ending_time = arrive_task[3]
            self.__task_index__ += 1
            if self.__task_index__ >= self.task_num:
                break
            expert_processing_list = self.__expert_list__.sorted_expert_task_dict[arrive_task_problem_id]
            # 放入等待队列
            self.__instert_into_waiting_queue__(arrive_task_problem_id,
                                                [arrive_task_id, arrive_task_start_time, arrive_task_ending_time])
            # 寻找空闲的专家
            for i in range(1, self.__expert_list__.question_num + 1):
                continue_schedule = True
                while (1):
                    if self.waiting_queue[i]:
                        if continue_schedule == True:
                            expert_processing_list = self.__expert_list__.sorted_expert_task_dict[i]
                            task = self.waiting_queue[i][0]
                            task_id = task[0]
                            task_start_time = task[1]
                            task_ending_time = task[2]  # 响应时间
                            # 寻找空闲的专家
                            for k in range(len(expert_processing_list)):
                                expert_id = expert_processing_list[k][0]
                                # 如果专家空闲并且任务能在规定时间内完成，则把任务分配给该专家
                                if self.__expert_list__.expert_processing_task_num[expert_id] > 0:
                                    expert_process_time = expert_processing_list[k][1]
                                    result_time = task_start_time + task_ending_time - (
                                            self.__time__ + expert_process_time)
                                    self.waiting_queue[i].pop(0)
                                    if result_time >= 0:
                                        start_time = self.__time__
                                        self.assignment_result.append([task_id, start_time, expert_id])
                                        self.__instert_into_running_queue__(expert_id,
                                                                            [task_id, start_time, expert_process_time,
                                                                             task_ending_time])
                                        self.__expert_list__.expert_processing_task_num[expert_id] -= 1
                                        break
                                    else:  # 当前最好的专家也不能解处理当前问题
                                        break
                                # 没有专家可以处理问题
                                if k == len(expert_processing_list) - 1:
                                    continue_schedule = False

                        else:
                            break
                    else:
                        break

        # 时间不合适
        self.__time__ += 1

    """
    暂时不对任务的响应时间进行检查(不考虑截止时间)
    """

    def check_running(self):
        for i in range(1, self.__expert_list__.expert_num + 1):
            if self.running_queue[i]:
                end_time = self.running_queue[i][0][1] + self.running_queue[i][0][2] - self.__time__
                if end_time == 0:
                    self.running_queue[i].pop(0)
                    self.__expert_list__.expert_processing_task_num[i] += 1

    def check_waiting(self):
        pass

    def check_assignment_end(self):
        if self.__task_index__ >= self.task_num:
            return True
        else:
            return False

    def check_end(self):
        if self.__time__ == 1300:
            return True

    def processing(self):
        while (1):
            self.check_running()
            self.check_waiting()
            if self.check_assignment_end() == False:
                self.assignment()
            else:
                break
            if self.check_end() == True:
                break
        print(len(self.assignment_result))


processing_task()
