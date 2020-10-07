from base.expert import expert as expert_data
from base.task import task_list as task_data


# 重构专家数据结构
class Expert:
    def __init__(self,
                 expert_id,
                 skill_dict):
        self.id = expert_id
        self.skill = skill_dict  # Python dict {task_id: processing_time}
        self.processing_task_num = 0
        self.work_load = 0

    def __str__(self):
        return "<id:{},skill:{},processing_task_num:{},word_load:{}>".format(self.id,
                                                                             self.skill,
                                                                             self.processing_task_num,
                                                                             self.work_load)

    def __repr__(self):
        return self.__str__()


class Task:
    def __init__(self,
                 task_id,
                 task_type,
                 start_time,
                 time_limit):
        self.id = task_id
        self.task_type = task_type
        self.start_time = start_time
        self.time_limit = time_limit
        self.assigned_expert = None

    def __str__(self):
        return "<id:{},type:{},start:{},limit:{}>".format(self.id,
                                                          self.task_type,
                                                          self.start_time,
                                                          self.time_limit)


# 通过专家的ID找到对应的专家类，以查询专家的状态
def load_expert_list(processing_matrix_file):
    data = expert_data(processing_matrix_file)
    raw_matrix = data.expert_list

    expert_list = []
    for i in range(1, data.expert_num + 1):
        skill_dict = {}
        for j in range(1, data.question_num + 1):
            if raw_matrix[i][j] < 999999:
                skill_dict[j] = raw_matrix[i][j]
        expert_list.append(Expert(expert_id=i,
                                  skill_dict=skill_dict))

    return expert_list


def load_task_list(work_order_file):
    data = task_data(work_order_file)
    raw_task_list = data.task_list

    task_list = []
    for task in raw_task_list:
        task_list.append(Task(task_id=task[0],
                              task_type=task[2],
                              start_time=task[1],
                              time_limit=task[3]))
    return task_list


# 对于给定的任务，哪些专家可以完成这项任务
def load_task_expert_dict(processing_matrix_file):
    data = expert_data(processing_matrix_file).sorted_expert_task_dict


if __name__ == '__main__':
    a = load_expert_list("data/process_time_matrix.csv")
    b = load_task_list("data/work_order.csv")
    # print(a)
    # print(b[0])