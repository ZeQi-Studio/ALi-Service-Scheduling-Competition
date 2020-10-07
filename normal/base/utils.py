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
        self.processing_task = []

    def __str__(self):
        return "<id:{},skill:{},processing_task_num:{},word_load:{}>".format(self.id,
                                                                             self.skill,
                                                                             self.processing_task_num,
                                                                             self.work_load)

    def __repr__(self):
        return self.__str__()

    # 未完成对应的任务，但是需要将资源释放出来
    def doRelease(self):
        pass
        # TODO: release method

    def assign(self, task, timer):
        # check 专家负荷是否超载
        if self.processing_task_num >= 3:
            raise FutureWarning
        # TODO:任务type与专家技能不匹配是否仍要分配,现默认任务匹配，专家将解决这一任务
        if task.task_type not in self.skill:
            pass
        # 专家负荷任务数+1,更新专家工作列表
        self.processing_task_num += 1
        self.processing_task.append(task)

        # 更新当前所分配任务Task的信息
        task.assigned_expert = self
        task.begin_time = timer

    # 默认调用该方法时，该任务已经完成
    def finish(self, task):
        # 更新专家工作负荷量、工作列表、负荷任务数目
        self.work_load += self.skill[task.task_type]
        self.processing_task.remove(task)
        self.processing_task_num -= 1


class Task:
    def __init__(self,
                 task_id,
                 task_type,
                 arrive_time,
                 time_limit):
        self.id = task_id
        self.task_type = task_type
        self.arrive_time = arrive_time
        self.begin_time = None
        self.time_limit = time_limit
        self.assigned_expert = None
        self.assigned_expert: Expert

    def __str__(self):
        return "<id:{},type:{},start:{},limit:{}>".format(self.id,
                                                          self.task_type,
                                                          self.arrive_time,
                                                          self.time_limit)

    def __repr__(self):
        return self.__str__()


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
                              arrive_time=task[1],
                              time_limit=task[3]))
    return task_list


# 对于给定的任务，哪些专家可以完成这项任务
def load_task_expert_dict(processing_matrix_file):
    data = expert_data(processing_matrix_file).sorted_expert_task_dict
    return data


if __name__ == '__main__':
    a = load_expert_list("data/process_time_matrix.csv")
    b = load_task_list("data/work_order.csv")
    # test Expeet.assign and Expert.finish
    t = b[0]
    e = a[0]
    print("专家", e)
    print("任务", t)
    e.assign(t)
    print("分配任务后专家信息：{}".format(e))
    print("分配任务后任务信息：{} assigned_expert:{}".format(t, t.assigned_expert))
    e.finish(t)
    print("任务完成后专家信息：{}".format(e))
    # print("任务完成后任务信息：{} assigned_expert:{}".format(t, t.assigned_expert))
